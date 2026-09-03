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
use delightql_protocol::{ErrorKind, ServerTerm, Transport};

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

/// Classify one NORMALIZED statement. `Err(goal)` hands the goal back
/// unchanged — it is not the effect chain's business and the caller proceeds
/// on the ordinary compilation path. `allow_adhoc` is false when CLI
/// danger/option overrides are active: the plan compiler applies default
/// gates only, so overridden DML/DDL statements keep that path.
/// run!/run_namespace! have no other path and always classify.
///
/// The goal arrives already read. Classification is a question about the
/// STATEMENT, and a classifier that re-parsed the text could answer it
/// differently from the compilation that follows.
pub(super) fn classify_effect_entry(
    goal: crate::pipeline::normalize::Goal,
    allow_adhoc: bool,
) -> std::result::Result<EffectEntry, crate::pipeline::normalize::Goal> {
    // Danger annotations are query-local refinement policy and travel into the
    // typed plan. Option overrides and inline DDL blocks still require the
    // ordinary compiler's broader configuration surface.
    if !goal.declared.options.is_empty() {
        return Err(goal);
    }
    let crate::pipeline::normalize::Goal {
        query,
        declared,
        category,
        spelling,
    } = goal;
    match classify_query(query.clone()) {
        Some(EffectEntry::AdhocBody { query: body, .. }) if allow_adhoc => {
            Ok(EffectEntry::AdhocBody {
                query: body,
                danger_specs: declared.dangers,
                ddl_blocks: declared.ddl_blocks,
            })
        }
        Some(other) if !matches!(other, EffectEntry::AdhocBody { .. }) => Ok(other),
        _ => Err(crate::pipeline::normalize::Goal {
            query,
            declared,
            category,
            spelling,
        }),
    }
}

#[stacksafe::stacksafe] // the Pipe payload is a StackSafe box
fn classify_query(query: Query) -> Option<EffectEntry> {
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
        return Some(EffectEntry::AdhocBody {
            query: Box::new(query),
            danger_specs: Vec::new(),
            ddl_blocks: Vec::new(),
        });
    }
    // Pure bindings do not themselves demand effects, but they do not erase
    // a directive demanded by the statement body either. Tail classification
    // below wraps the complete query, including those bindings, when the body
    // is a descriptor-declared ad-hoc terminal.
    let expr = &query.body;
    // Descend through PURE postfix operators (drills, narrows,
    // projections — e.g. the `!>` normalization or an explicit
    // `.returned(*)` release over a DML receipt): the classification is
    // by the expression's directive TAIL, not its outermost operator.
    // The AdhocBody wraps the FULL original query either way.
    // The receipt a direct invocation was written with: the access standing
    // in the effect position, which for a bare call is the read's own.
    let head_access = expr
        .head_access()
        .cloned()
        .unwrap_or(crate::pipeline::asts::core::Access::Unasked);
    let mut probe = expr.steps();
    loop {
        match probe.split_last().map(|(step, rest)| (step.form(), rest)) {
            // The structural forms — ordering, reposition, meta, the
            // witnesses, drill, narrowing — and the pure pipe operators are
            // the postfix steps this descent reads through: the
            // classification is by the expression's directive TAIL, not its
            // outermost step. Named by their exact variants — the pipe
            // stage, and the structural step that is one BY TYPE — never by
            // a run-membership protocol. An access step past the head's own
            // read — `… as u(a, b)` patterning the completed receipt — is a
            // consumer of that receipt exactly as a pipe is, and the descent
            // reads through it; the receipt access itself is the head's and
            // never stands among these steps.
            Some((
                crate::pipeline::asts::core::Continuation::Pipe { .. }
                | crate::pipeline::asts::core::Continuation::Structural(_)
                | crate::pipeline::asts::core::Continuation::Access { .. },
                prefix,
            )) => probe = prefix,
            _ => break,
        }
    }
    match probe.last() {
        // Direct invocations `run_namespace!(ns)(*)` / `run!("file")(*)`, with or
        // without the `(*)` receipt access (the two-paren spelling builds
        // the same `FunctorCall` shape). Non-glob receipt access is not
        // classified here — it falls through to the executor, whose
        // run!/run_namespace! entities refuse with their whole-statement
        // policy until receipt access lands more generally.
        None => {
            let crate::pipeline::asts::core::GroundForm::Reference(Relation::FunctorCall {
                call,
                ..
            }) = expr.head().form()
            else {
                return None;
            };

            // Glob/bare access = the payload-transparent dump (the
            // execution family's exception). A positional NAME list is the
            // exact-arity receipt binding; any other
            // spec falls through to the executor's refusal.
            let reference = Some(&call.call().callee)?;
            if adhoc_statement_call(call.call()) {
                return Some(EffectEntry::AdhocBody {
                    query: Box::new(query),
                    danger_specs: Vec::new(),
                    ddl_blocks: Vec::new(),
                });
            }
            let access = &head_access;
            let arguments = call
                .call()
                .arguments
                .value_domains()
                .cloned()
                .collect::<Vec<_>>();
            let run_access = if access.is_whole() {
                Some(None)
            } else {
                access.binders().map(|names| {
                    Some(
                        names
                            .into_iter()
                            .map(|binder| binder.name.to_string())
                            .collect(),
                    )
                })
            };
            let access = run_access?;
            match crate::pipeline::asts::effects::kind_for_reference(reference) {
                Some(crate::pipeline::asts::effects::DirectiveKind::RunNamespace) => {
                    single_argument(&arguments).map(|namespace| EffectEntry::RunNamespace {
                        namespace,
                        access: access.clone(),
                    })
                }
                Some(crate::pipeline::asts::effects::DirectiveKind::Run) => {
                    single_argument(&arguments).map(|path| EffectEntry::RunFile {
                        path,
                        access: access.clone(),
                    })
                }
                // `cli::repl.set_prompt!("✅")(*)` arrives here. See the
                // matching arm above for why it is a run.
                None if user_directive(reference) => Some(EffectEntry::AdhocBody {
                    query: Box::new(query),
                    danger_specs: Vec::new(),
                    ddl_blocks: Vec::new(),
                }),
                _ => None,
            }
        }
        _ => None,
    }
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
    ServerTerm::Error {
        kind: ErrorKind::Syntax,
        identity: e.error_uri().into_bytes(),
        message: format!("{}", e).into_bytes(),
    }
}

fn created_object_registration_error(message: String) -> ServerTerm {
    ServerTerm::Error {
        kind: ErrorKind::Connection,
        identity: b"delightql-error://runtime/session_health/external_effect".to_vec(),
        message: message.into_bytes(),
    }
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
            return error_term(&DelightQLError::validation_error_categorized(
                "effect/run/receipt_access",
                msg,
                "run receipt access",
            ));
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
                            let primary = DelightQLError::database_error_categorized(
                                "session_health/registration_unsupported",
                                format!("created-object registration unsupported: {reason}"),
                                "created-object registration invariant breach",
                            );
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
                        Ok(delightql_protocol::CloseResponse::Error { message, .. }) => {
                            failure.push_str(&format!(
                                "; unsent handle close failed: {}",
                                String::from_utf8_lossy(&message)
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
            DelightQLError::database_error(
                format!("Failed to acquire bootstrap lock: {}", e),
                "Bootstrap lock",
            )
        })?;
        let mut stmt = guard
            .prepare("SELECT COALESCE(kind, 'unknown') FROM namespace WHERE fq_name = ?1")
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to query namespace catalog: {}", e),
                    "Bootstrap query",
                )
            })?;
        let mut rows = stmt.query([namespace]).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query namespace catalog: {}", e),
                "Bootstrap query",
            )
        })?;
        match rows.next() {
            Ok(Some(row)) => Ok(Some(row.get(0).unwrap_or_else(|_| "unknown".to_string()))),
            Ok(None) => Ok(None),
            Err(e) => Err(DelightQLError::database_error(
                format!("Failed to read namespace catalog: {}", e),
                "Bootstrap query",
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
                Ok(EffectEntry::RunNamespace { namespace, .. }) => assert_eq!(namespace, "fx"),
                _ => panic!("expected RunNamespace for {:?}", dql),
            }
        }
    }

    #[test]
    fn whole_statement_run_classifies_with_path() {
        match classify_effect_entry(read_goal("run!(\"ddl/script.dql\")(*)"), true) {
            Ok(EffectEntry::RunFile { path, .. }) => assert_eq!(path, "ddl/script.dql"),
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
                "abort" => "orders(*) |> abort!(\"runtime/assertion\", \"test\")(*)".to_string(),
                "assert" => "has_rows(T(*))(*) : T(*)\n\
                             orders(*) |> assert!(has_rows(*), \"test\")(*)"
                    .to_string(),
                _ => format!("orders(*) |> {name}!(target(*))(*)"),
            };
            assert!(
                matches!(
                    classify_effect_entry(read_goal(&dql), true),
                    Ok(EffectEntry::AdhocBody { .. })
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
            Ok(EffectEntry::AdhocBody { .. })
        ));
    }

    /// The policy's two near misses, which a name list got right only by
    /// accident: DDL realized as an ENTITY has a callable to invoke, and a
    /// pipe terminal that writes no database is not a statement.
    #[test]
    fn writing_the_database_and_needing_the_pipe_are_both_required() {
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
        assert!(!returning.is_adhoc_statement_terminal());
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
                classify_effect_entry(read_goal(dql), true).is_err(),
                "expected today's path for {:?}",
                dql
            );
        }
    }

    #[test]
    fn query_local_danger_annotations_ride_the_typed_path() {
        let dql = "orders(*) |> insert!(t(*))(*) (~~danger://cardinality/cartesian ~~)";
        match classify_effect_entry(read_goal(dql), true) {
            Ok(EffectEntry::AdhocBody { danger_specs, .. }) => {
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
                classify_effect_entry(read_goal(dql), true).is_ok(),
                "expected classification for {:?}",
                dql
            );
        }
        // An ordinary statement is not the effect chain's business.
        assert!(classify_effect_entry(read_goal("users(*), a != b"), true).is_err());
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
