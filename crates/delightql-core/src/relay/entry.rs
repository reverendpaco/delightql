// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The Epic-3.3 ENTRY POINTS: where the execution directives (`run!`,
//! `run_namespace!`) and query-position DML/DDL directives leave today's
//! single-statement pipeline and take the effect chain — the transformer
//! (plan §3.1, `pipeline::effect_transformer`) compiles, the pump
//! (plan §3.2, `relay::pump::handle_plan`) plays.
//!
//! `handle_query` consults `classify_effect_entry` before the ordinary
//! compile path. Three shapes reroute, everything else stays byte-for-byte
//! on today's path (the classifier answers `None`):
//!
//! 1. `run_namespace!(ns)` / `run_namespace!(ns)(*)` as the WHOLE statement
//!    — F3: demand the consulted namespace's `main!`; refuse "has no main!
//!    to demand" when absent (effects ball main--22).
//! 2. `run!("file.dql")` / `run!("file.dql")(*)` as the whole statement —
//!    F2: consult-then-demand. This retires run!'s old free-statement
//!    query-grammar semantics (REPORT-2.1: run.rs:102 could never accept
//!    `:-` definitions).
//! 3. A statement whose top-level expression pipes into a DML terminal
//!    (`insert!`/`update!`/`delete!`) or a DDL creation directive
//!    (`temp_table!`/`table!`/`temp_view!`) — the statement compiles as a
//!    one-clause effect body so it returns its RECEIPT per EFFECT-ALGEBRA
//!    §3 (effects ball dml_receipt--01..06 / ddl_receipt--11..15) instead
//!    of the legacy `affected_rows` relation.
//!
//! The classifier is deliberately conservative: it declines statements
//! carrying assertions, emit streams, error hooks (pre-screened by the
//! caller), danger/option annotations, or any parse/build trouble — those
//! keep today's path and today's messages.

use delightql_protocol::{ErrorKind, ServerTerm, Transport};

use super::RelayParty;
use crate::error::DelightQLError;
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::DomainExpression;
use crate::pipeline::ast_unresolved::{
    Query, Relation, RelationalExpression, UnaryRelationalOperator,
};
use crate::pipeline::compiled_query::{CompiledPlan, PlanEntry};
use crate::pipeline::{builder_v2, effect_transformer, parser};

/// A statement the effect chain owns.
#[derive(Debug)]
pub(super) enum EffectEntry {
    /// `run_namespace!(ns)` — demand an already-consulted namespace's main!.
    RunNamespace {
        namespace: String,
        /// Non-glob receipt access: the exact-arity positional binding
        /// list (F5 reified, Phase 6 slice 6). None = glob/bare — the
        /// execution family's payload-transparent dump.
        access: Option<Vec<String>>,
    },
    /// `run!("file.dql")` — consult the file, then demand its main!.
    RunFile {
        path: String,
        /// See RunNamespace::access.
        access: Option<Vec<String>>,
    },
    /// A top-level directive-demanding statement: compile as an ad-hoc
    /// effect body; the run's value is the directive's receipt.
    AdhocBody {
        query: Box<Query>,
        /// Phase 10 slice b: statement annotations ride the typed program.
        assertions: Vec<crate::pipeline::asts::core::queries::AssertionSpec>,
        emits: Vec<crate::pipeline::asts::core::queries::EmitSpec>,
    },
}

/// Classify a single-statement query text. `None` = not the effect chain's
/// business; the caller proceeds on today's path. `allow_adhoc` is false
/// when CLI danger/option overrides are active — the plan compiler applies
/// default gates only, so overridden DML/DDL statements keep today's path;
/// run!/run_namespace! have no legacy path and always classify.
pub(super) fn classify_effect_entry(dql: &str, allow_adhoc: bool) -> Option<EffectEntry> {
    // Cheap pre-filter: every target shape contains a directive call —
    // `!` then `(`, with grammar-legal whitespace allowed between (review
    // F6: a bare `contains("!(")` missed `run_namespace! (fx)` and the
    // fallback refusal then lied; pinned by the effects ball's
    // main--25_spaced_directive_classifies). The parse+build cost this
    // gate accepts is unchanged for `!=`-style texts: `!` not followed by
    // (whitespace and) `(` still declines without parsing.
    if !contains_directive_call(dql) {
        return None;
    }
    let tree = parser::parse(dql).ok()?;
    let (mut queries, _features, assertions, emits, dangers, options, ddl_blocks) =
        builder_v2::parse_queries(&tree, dql).ok()?;
    // Phase 10 slice b (semantic routing): assertions and emits ride the
    // typed program as head steps — an annotated statement chooses the
    // SAME execution generation as its unannotated twin. Danger/option
    // overrides and inline DDL blocks are compile-mode configuration and
    // conservatively keep today's path.
    if queries.len() != 1 || !dangers.is_empty() || !options.is_empty() || !ddl_blocks.is_empty()
    {
        return None;
    }
    let query = queries.pop().expect("length checked above");
    match classify_query(query) {
        Some(EffectEntry::AdhocBody { query, .. }) => {
            if allow_adhoc {
                Some(EffectEntry::AdhocBody {
                    query,
                    assertions,
                    emits,
                })
            } else {
                None
            }
        }
        // The run/entry forms take no statement annotations today; an
        // annotated run! keeps today's path rather than dropping them.
        other if assertions.is_empty() && emits.is_empty() => other,
        _ => None,
    }
}

/// `!` followed by `(`, tolerating whitespace between — the textual shadow
/// of the grammar's directive-call rule (which permits the space).
fn contains_directive_call(dql: &str) -> bool {
    let mut rest = dql;
    while let Some(pos) = rest.find('!') {
        rest = &rest[pos + 1..];
        if rest.trim_start().starts_with('(') {
            return true;
        }
    }
    false
}

#[stacksafe::stacksafe] // the Pipe payload is a StackSafe box
fn classify_query(query: Query) -> Option<EffectEntry> {
    let Query::Relational(ref expr) = query else {
        // CTE-carrying and REPL-command shapes stay on today's path in
        // v0.1 (effect-CTE labels only occur inside consulted rules).
        return None;
    };
    // Descend through PURE postfix operators (drills, narrows,
    // projections — e.g. the `!>` normalization or an explicit
    // `.returned(*)` release over a DML receipt): the classification is
    // by the expression's directive TAIL, not its outermost operator.
    // The AdhocBody wraps the FULL original query either way.
    let mut probe: &RelationalExpression = expr;
    loop {
        match probe {
            RelationalExpression::Pipe(pipe)
                if !matches!(
                    &pipe.operator,
                    UnaryRelationalOperator::DirectiveTerminal { .. }
                        | UnaryRelationalOperator::DirectivePipeInvocation { .. }
                ) =>
            {
                probe = &pipe.source;
            }
            _ => break,
        }
    }
    match probe {
        RelationalExpression::Pipe(pipe) => {
            match &pipe.operator {
                // Relation-target DDL (Phase 3 canonical invocation) and
                // DML (Phase 6 slice 5 — the designator form):
                // source |> table!(my::ns.dump_table(*))(*) → receipt
                // lowering, same as the bare-name terminal form.
                UnaryRelationalOperator::DirectivePipeInvocation { name, .. }
                    if matches!(
                        name.as_str(),
                        "temp_table!" | "table!" | "temp_view!" | "insert!" | "update!"
                            | "delete!"
                    ) =>
                {
                    Some(EffectEntry::AdhocBody {
                        query: Box::new(query),
                        assertions: Vec::new(),
                        emits: Vec::new(),
                    })
                }
                UnaryRelationalOperator::DirectiveTerminal { name, .. } => {
                    match name.as_str() {
                        // DDL creation directives → receipt lowering.
                        // (`imprint!`/`imprint_replace!` keep their own
                        // existing execution path.)
                        "temp_table!" | "table!" | "temp_view!" => {
                            Some(EffectEntry::AdhocBody {
                                query: Box::new(query),
                                assertions: Vec::new(),
                                emits: Vec::new(),
                            })
                        }
                        // Two-paren `run_namespace!(ns)(*)`: the builder
                        // spells the argument as a one-row anonymous source.
                        "run_namespace!" => single_anonymous_argument(&pipe.source)
                            .map(|namespace| EffectEntry::RunNamespace {
                                namespace,
                                access: None,
                            }),
                        "run!" => single_anonymous_argument(&pipe.source)
                            .map(|path| EffectEntry::RunFile { path, access: None }),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        // Direct invocations `run_namespace!(ns)` / `run!("file")`, with or
        // without the `(*)` receipt access (Phase 3 canonical invocation:
        // the two-paren spelling now builds this same PseudoPredicate shape).
        // Non-glob receipt access is not classified here — it falls through
        // to the executor, whose run!/run_namespace! entities refuse with
        // their whole-statement policy until receipt access lands with the
        // Phase 4 receipt rebuild.
        RelationalExpression::Relation(Relation::PseudoPredicate {
            name,
            arguments,
            access,
            ..
        }) => {
            use crate::pipeline::asts::core::DomainSpec;
            // Glob/bare access = the payload-transparent dump (F5's
            // execution-family exception). A positional NAME list is the
            // exact-arity receipt binding (Phase 6 slice 6); any other
            // spec falls through to the executor's refusal.
            let run_access = match access {
                DomainSpec::Glob | DomainSpec::Bare => Some(None),
                DomainSpec::Positional(exprs) => {
                    let glob = exprs.len() == 1
                        && matches!(
                            &exprs[0],
                            crate::pipeline::asts::core::DomainExpression::Projection(
                                crate::pipeline::asts::core::expressions::domain::ProjectionExpr::Glob { .. }
                            )
                        );
                    if glob {
                        Some(None)
                    } else {
                        exprs
                            .iter()
                            .map(|e| match e {
                                crate::pipeline::asts::core::DomainExpression::Lvar {
                                    name, ..
                                } => Some(name.to_string()),
                                _ => None,
                            })
                            .collect::<Option<Vec<String>>>()
                            .map(Some)
                    }
                }
                _ => None,
            };
            let access = run_access?;
            match name.as_str() {
                "run_namespace!" => single_argument(arguments).map(|namespace| {
                    EffectEntry::RunNamespace {
                        namespace,
                        access: access.clone(),
                    }
                }),
                "run!" => single_argument(arguments).map(|path| EffectEntry::RunFile {
                    path,
                    access: access.clone(),
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The single argument of a run form: a bare/`::`-qualified name (an Lvar
/// with the `::` text intact, REPORT-2.2) or a string literal.
fn single_argument(arguments: &[DomainExpression]) -> Option<String> {
    let [value] = arguments else { return None };
    argument_value(value)
}

fn single_anonymous_argument(source: &RelationalExpression) -> Option<String> {
    let RelationalExpression::Relation(Relation::Anonymous { rows, .. }) = source else {
        return None;
    };
    let [row] = rows.as_slice() else { return None };
    let [value] = row.values.as_slice() else {
        return None;
    };
    argument_value(value)
}

fn argument_value(value: &DomainExpression) -> Option<String> {
    match value {
        DomainExpression::Lvar {
            name,
            qualifier: None,
            ..
        } => Some(name.to_string()),
        DomainExpression::Literal {
            value: LiteralValue::String(s),
            ..
        } => Some(s.clone()),
        _ => None,
    }
}

/// The namespace `run!("path/to/script.dql")` consults into: the file stem,
/// sanitized to identifier characters. EFFECT-ALGEBRA F2 names no
/// namespace; the stem is what a human would type, and it leaves the
/// script addressable afterwards (F3: "a consulted script is thereby
/// runnable without re-consulting" via `run_namespace!`).
fn namespace_from_path(path: &str) -> String {
    let stem = std::path::Path::new(path)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let sanitized: String = stem
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
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
                // F2: consult-then-demand. Liminal directives execute at
                // load; rules register; then main! is demanded exactly as
                // run_namespace! would.
                let namespace = match self.consult_for_run(&path) {
                    Ok(ns) => ns,
                    Err(e) => return error_term(&e),
                };
                let term = self.demand_namespace_main(&namespace);
                match access {
                    None => term,
                    Some(names) => {
                        self.bind_run_receipt(term, "run!", "path", &path, &names)
                    }
                }
            }
            EffectEntry::AdhocBody {
                query,
                assertions,
                emits,
            } => {
                match effect_transformer::compile_query_plan_annotated(
                    self.system,
                    &query,
                    None,
                    &assertions,
                    &emits,
                ) {
                    Ok(plan) => self.play_plan(&plan),
                    Err(e) => error_term(&e),
                }
            }
        }
    }

    /// Bind an exact-arity positional access list against the run's
    /// REIFIED receipt (F5, Phase 6 slice 6): `(success, operation,
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
        let rows: Vec<Vec<String>> = if self.last_run_exited {
            Vec::new()
        } else {
            vec![vec![
                "1".to_string(),
                operation.to_string(),
                echo_value.to_string(),
                payload,
            ]]
        };
        let columns: Vec<String> = names.to_vec();
        self.eager_header(&columns, &rows)
    }

    fn demand_namespace_main(&mut self, namespace: &str) -> ServerTerm {
        match effect_transformer::compile_namespace_main(self.system, namespace) {
            Ok(plan) => self.play_plan(&plan),
            Err(e) => error_term(&e),
        }
    }

    /// Fresh scratch per run, then pump. A second run on the same session
    /// must not collide with the first's leftover scratch (`__r_*`,
    /// `__exit` are per-connection temp state and the plan re-CREATEs
    /// them; a stale `__exit` row would even latch the exit peek and skip
    /// the whole run). Pinned by the CLI integration test
    /// `run_twice_on_one_session_gets_fresh_scratch`.
    fn play_plan(&mut self, plan: &CompiledPlan) -> ServerTerm {
        // D4: materialize the plan's observational projection
        // (sys::execution.effect_plan/…) — clear-then-insert is the
        // next-run-clears lifecycle. Best-effort: bookkeeping never
        // outranks the run (Q-D5's discipline, applied to the plan side).
        if let Some(typed) = &plan.typed {
            let _ = self.system.materialize_effect_plan(typed);
        }
        self.drop_plan_scratch(plan);
        let response = self.handle_plan(plan);
        if !matches!(response, ServerTerm::Error { .. }) {
            // Catalog registration for the run's created objects, so
            // post-run statements resolve them bare (materialize-pipe §1;
            // ddl_receipt--12/--13/--14, util--36). Best-effort per object:
            // an object skipped past the exit flag simply does not exist
            // and registers nothing.
            for obj in &plan.created_objects {
                let _ = self.system.register_run_created_object(
                    &obj.name,
                    obj.is_view,
                    obj.connection_id.unwrap_or(2),
                );
            }
        }
        response
    }

    /// Drop the plan's own scratch shells (the `CREATE TEMP TABLE temp.__…`
    /// statements emitted before the transaction bracket, invariant §5.6)
    /// if an earlier run on this session left them behind — a prior run
    /// that ABORTED (shells are pre-bracket, so they survive the rollback)
    /// or took `exit!` (which skips the plan's own trailing cleanup). Every
    /// DROP is `temp.`-qualified: it structurally cannot bind into the
    /// user's `main` schema (review F1, the SEV-1 this method used to be —
    /// an unqualified `DROP __r_insert` on a fresh session resolved to
    /// main and destroyed the user's table; pinned by the effects ball's
    /// scratch--51_user_table_survives_adhoc_dml). Kept ALONGSIDE the
    /// planner's adjacent/trailing drops because the pump's exit peek runs
    /// before every plan entry: a stale `temp.__exit` ROW would latch the
    /// peek and silently skip the entire run — including any in-plan DROP
    /// that would have cleared it — so the clearing must happen before the
    /// plan starts playing. Best-effort: a missing table is the normal
    /// first-run case. Pinned by the CLI integration test
    /// `run_twice_on_one_session_gets_fresh_scratch`.
    ///
    /// Each DROP routes on ITS SHELL ENTRY's `connection_id` — which is the
    /// plan's ONE settled connection (E-T1: `compile_with_settled_connection`
    /// stamps every entry uniformly for non-hub plans, pinned by
    /// `fatboy_plan_entries_all_carry_the_plan_connection` in
    /// pipeline/effect_transformer/tests.rs), so the clearing happens on the
    /// engine the scratch lives on, never the hub by accident.
    fn drop_plan_scratch(&mut self, plan: &CompiledPlan) {
        for entry in &plan.entries {
            match entry {
                PlanEntry::BeginTransaction { .. } => break, // shells precede the bracket
                PlanEntry::Statement(st) => {
                    if let Some(rest) = st.sql.strip_prefix("CREATE TEMP TABLE temp.") {
                        if rest.starts_with("__") {
                            if let Some(name) =
                                rest.split(|c: char| c == ' ' || c == '(').next()
                            {
                                let _ = self.execute_sql_routed(
                                    &format!("DROP TABLE IF EXISTS temp.{}", name),
                                    st.connection_id,
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// run!'s consult half: consult the file into its stem-derived
    /// namespace, or RE-consult when an earlier run! (or consult!) already
    /// loaded that namespace — each run! re-reads the file (a script
    /// runner's contract), while run_namespace! deliberately does not (F3).
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
        for dql in ["run_namespace!(fx)(*)", "run_namespace!(fx)", "run_namespace!(\"fx\")(*)"] {
            match classify_effect_entry(dql, true) {
                Some(EffectEntry::RunNamespace { namespace, .. }) => assert_eq!(namespace, "fx"),
                _ => panic!("expected RunNamespace for {:?}", dql),
            }
        }
    }

    #[test]
    fn whole_statement_run_classifies_with_path() {
        match classify_effect_entry("run!(\"ddl/script.dql\")(*)", true) {
            Some(EffectEntry::RunFile { path, .. }) => assert_eq!(path, "ddl/script.dql"),
            _ => panic!("expected RunFile"),
        }
    }

    #[test]
    fn dml_and_ddl_terminals_classify_as_adhoc_bodies() {
        for dql in [
            "orders(*), region = \"EU\" |> insert!(orders_eu(*))(*)",
            "orders!!(*), status = \"old\" |> delete!(orders(*))(*)",
            "orders(*) |> temp_table!(staged)",
            "orders(*) |> table!(archive)",
            "orders(*) |> temp_view!(fresh)",
        ] {
            assert!(
                matches!(classify_effect_entry(dql, true), Some(EffectEntry::AdhocBody { .. })),
                "expected AdhocBody for {:?}",
                dql
            );
        }
    }

    #[test]
    fn plain_queries_and_session_directives_stay_on_todays_path() {
        for dql in [
            "users(*)",
            "consult!(\"x.dql\", \"fx\")",
            "mount!(\"db.sqlite\", \"main\")",
            "users(*), region = \"EU\"",
            // imprint! keeps its own existing execution path
            "users(*) |> imprint!(t)",
        ] {
            assert!(
                classify_effect_entry(dql, true).is_none(),
                "expected None for {:?}",
                dql
            );
        }
    }

    #[test]
    fn annotated_statements_ride_the_typed_program() {
        // Phase 10 slice b (semantic routing): an assertion-carrying DML
        // classifies — the annotation rides the typed program as a head
        // step, so wrapped and unwrapped demands choose the SAME
        // execution generation (the acceptance clause). The old
        // conservative bailout is gone.
        let dql = "orders(*) |> insert!(t(*))(*) (~~assert ~> count:(*) as c, c = 1 |> exists(*) ~~)";
        match classify_effect_entry(dql, true) {
            Some(EffectEntry::AdhocBody { assertions, .. }) => {
                assert_eq!(assertions.len(), 1, "the assertion spec is threaded");
            }
            other => panic!("expected AdhocBody with the assertion, got {other:?}"),
        }
        // Danger/option overrides are compile-mode configuration and
        // conservatively keep today's path.
        let dql = "orders(*) |> insert!(t(*))(*) (~~danger:allow_full_scan ~~)";
        assert!(classify_effect_entry(dql, true).is_none());
    }

    #[test]
    fn spaced_directive_calls_classify() {
        // Grammar-legal whitespace between `!` and `(` must not fall past
        // the pre-filter into the lying position refusal (review F6;
        // effects ball main--25 pins this end to end).
        for dql in ["run_namespace! (fx)", "run_namespace!  (fx)(*)", "run! (\"a.dql\")"] {
            assert!(
                classify_effect_entry(dql, true).is_some(),
                "expected classification for {:?}",
                dql
            );
        }
        // `!` not followed by `(` still declines without a parse.
        assert!(!contains_directive_call("users(*), a != b"));
        assert!(contains_directive_call("run_namespace!\n(fx)"));
    }

    #[test]
    fn namespace_from_path_uses_sanitized_stem() {
        assert_eq!(namespace_from_path("ddl/torture.dql"), "torture");
        assert_eq!(namespace_from_path("a/b/my-script.dql"), "my_script");
        assert_eq!(namespace_from_path(""), "script");
    }
}
