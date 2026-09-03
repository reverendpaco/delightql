// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `consult!()` pseudo-predicate implementation
//!
//! Syntax: `consult!(file_path, namespace_name)`
//!
//! Example: `consult!("lib/functions.dql", "lib::math")`
//!
//! ## Behavior
//!
//! 1. Reads a DQL file containing definitions (functions and views)
//! 2. Parses the file using the DDL parser
//! 3. Stores definitions in the system's consult store under the given namespace
//! 4. Returns a single-row result table indicating success

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// Resolve namespace prefix conventions:
/// - `.::foo` → `{consulting_ns}::foo`  (relative to consulting DDL's namespace)
/// - `::foo`  → `foo`                   (absolute — escape to root)
/// - `foo`    → `foo`                   (plain — unchanged)
pub(crate) fn resolve_ns_prefix(name: &str, consulting_ns: &str) -> Result<String> {
    if name.starts_with(".::") {
        let suffix = &name[3..];
        if suffix.is_empty() {
            return Err(DelightQLError::database_error(
                ".:: prefix requires a name after it",
                "Empty relative namespace",
            ));
        }
        Ok(format!("{}::{}", consulting_ns, suffix))
    } else if name.starts_with("::") {
        let suffix = &name[2..];
        if suffix.is_empty() {
            return Err(DelightQLError::database_error(
                ":: prefix requires a name after it",
                "Empty absolute namespace",
            ));
        }
        Ok(suffix.to_string())
    } else {
        Ok(name.to_string())
    }
}

/// Build the receipt row for one executed liminal directive. `name` is the
/// directive name WITHOUT the `!`
/// (as the narrowed extraction delivers it); `args` are the arguments as
/// written in the file — receipts ECHO parameters, they never measure.
/// Echo column names follow the ruled table exactly: a namespace argument
/// is `namespace` (role-prefixed when a directive takes several), a file
/// argument is `path`; optional echoes are present with NULL (`reconsult!`'s
/// same-file `path`, `enlist!`'s plain-form `into`); `expose!`'s variadic
/// echoes take the glob-join convention (`namespace`, `namespace_2`, …).
/// Pinned by `liminal_receipt_columns_follow_the_ruled_table` and the
/// effects-ball liminal--45 baseline.
pub(crate) fn liminal_receipt_for(name: &str, args: &[String]) -> crate::system::LiminalReceipt {
    let arg = |i: usize| args.get(i).cloned();
    let echoes: Vec<(String, Option<String>)> = match name {
        // The interior-echo directive: consult!'s DIRECT receipt carries
        // `input ⟦path, namespace⟧`, but the liminal LEDGER row stays FLAT
        // pending the ledger's interior adoption — path, namespace.
        "consult" => vec![
            ("path".to_string(), arg(0)),
            ("namespace".to_string(), arg(1)),
        ],
        // doc!'s direct receipt is the interior `input` echo; its ledger
        // row stays flat (target, doc) like consult's.
        "doc" => vec![("target".to_string(), arg(0)), ("doc".to_string(), arg(1))],
        // expose!'s variadic echoes take the glob-join convention
        // (`namespace`, `namespace_2`, …) — genuinely runtime-shaped.
        "expose" => args
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let col = if i == 0 {
                    "namespace".to_string()
                } else {
                    format!("namespace_{}", i + 1)
                };
                (col, Some(a.clone()))
            })
            .collect(),
        // EVERY other session directive derives its ledger echoes from the
        // DESCRIPTOR's declared `receipt_echoes` — a new directive supplies
        // its ledger representation inductively, never a second per-name
        // vocabulary. Optional echoes absent from the call are present
        // with NULL. (mount_tree!'s created sub-namespace enumeration still
        // rides only the SURFACE receipt — threading post-execution results
        // into this pure builder stays deferred.)
        other => match crate::pipeline::asts::effects::descriptor(other) {
            Some(desc) if !desc.receipt_echoes.is_empty() => desc
                .receipt_echoes
                .iter()
                .enumerate()
                .map(|(i, e)| (e.name.to_string(), arg(i)))
                .collect(),
            // No descriptor or no declared echoes: echo the raw arguments
            // positionally so nothing is silently dropped.
            _ => args
                .iter()
                .enumerate()
                .map(|(i, a)| (format!("arg_{}", i + 1), Some(a.clone())))
                .collect(),
        },
    };
    crate::system::LiminalReceipt {
        operation: format!("{}!", name),
        echoes,
    }
}

/// consult!() pseudo-predicate entity
pub struct ConsultPredicate;

impl BinEntity for ConsultPredicate {
    fn name(&self) -> &str {
        "consult!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "file_path".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "namespace".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            // The receipt heading is the DESCRIPTOR's declaration
            // Core + `input` echo + `returned` payload.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("consult")),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for ConsultPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // Validate argument count
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "consult!() expects 2 arguments (file_path, namespace), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        // Extract file_path from first argument
        let file_path = extract_string_literal(&arguments[0], "file_path")?;

        // Extract namespace from second argument
        let namespace = extract_string_literal(&arguments[1], "namespace")?;

        if namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "consult!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        let _count = execute_consult(system, &file_path, &namespace, None)?;

        // (success, operation,
        // input, returned) — input echoes the lifted argument row,
        // returned carries the consulted namespaces.
        Ok(EntityResult::Relation(super::interior_receipt_result(
            "consult!",
            &["path", "namespace"],
            &[vec![Some(file_path.clone()), Some(namespace.clone())]],
            &["namespace"],
            &[vec![Some(namespace.clone())]],
            alias,
        )))
    }
}

/// THE ONE LIMINAL BINDER: execute an embedded session
/// directive through its REGISTERED ENTITY — the same descriptor,
/// binder, arity teachings, and execution the top level uses. The only
/// liminal-specific policy is DECLARED, not hand-spelled: parameters of
/// kind `Namespace` take `.::`/`::` prefix resolution relative to the
/// consulting namespace. This deletes the per-name dispatch arms (the
/// "second directive vocabulary"): any registered session entity works
/// embedded automatically — a directive can no longer be accepted by
/// the registry and forgotten by a separate liminal match.
pub(crate) fn execute_liminal_via_entity(
    system: &mut crate::system::DelightQLSystem,
    name: &str,
    args: &[String],
    consulting_ns: &str,
) -> Result<()> {
    let bound: Vec<DomainExpression> = bind_liminal_args(name, args, consulting_ns)?
        .into_iter()
        .map(|value| {
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(value)))
        })
        .collect();
    let entity = system
        .bin_registry()
        .lookup_entity(&format!("{name}!"))
        .ok_or_else(|| {
            DelightQLError::database_error(
                format!(
                    "liminal directive '{name}!' has a descriptor but no registered \
                     entity — a registration accident, not a policy"
                ),
                "Unregistered directive",
            )
        })?;
    let executable = entity.as_effect_executable().ok_or_else(|| {
        DelightQLError::database_error(
            format!("liminal directive '{name}!' is not executable"),
            "Not executable",
        )
    })?;
    // The liminal ledger records this statement separately
    // (liminal_receipt_for); the direct receipt has no liminal reader.
    let _receipt = executable.execute(&bound, None, system)?;
    Ok(())
}

/// THE ONE LIMINAL ARGUMENT BINDER: the directive's declared descriptor
/// supplies arity and its teachings, and parameters of kind `Namespace`
/// take `.::`/`::` prefix resolution relative to the consulting namespace.
/// Every embedded directive — the lexical-edge ones the walk executes for
/// their answer, and the rest it executes through their entity — binds
/// its arguments here, so no directive reads its arguments two ways.
fn bind_liminal_args(name: &str, args: &[String], consulting_ns: &str) -> Result<Vec<String>> {
    use crate::pipeline::asts::effects::{descriptor, DirectiveParamKind};
    let desc = descriptor(name).ok_or_else(|| {
        DelightQLError::database_error(
            format!("no descriptor for liminal directive '{name}!'"),
            "Unknown directive",
        )
    })?;
    let required = desc.params.iter().filter(|p| !p.optional).count();
    if args.len() < required || args.len() > desc.params.len() {
        return Err(DelightQLError::validation_error_categorized(
            "directive/binding/arity",
            format!(
                "{name}! expects {} argument(s) ({}), got {}",
                if required == desc.params.len() {
                    required.to_string()
                } else {
                    format!("{required}..{}", desc.params.len())
                },
                desc.params
                    .iter()
                    .map(|p| p.name)
                    .collect::<Vec<_>>()
                    .join(", "),
                args.len()
            ),
            "directive arity",
        ));
    }
    let mut bound: Vec<String> = Vec::with_capacity(args.len());
    for (param, arg) in desc.params.iter().zip(args) {
        bound.push(if param.kind == DirectiveParamKind::Namespace {
            resolve_ns_prefix(arg, consulting_ns)?
        } else {
            arg.clone()
        });
    }
    Ok(bound)
}

/// Caller-visible session state captured at the boundary of one liminal
/// program, so the runner can restore the caller afterwards. It is the
/// runner's alone: a load's own declared edges come from its authored
/// text, never from how the session moved while the load ran.
struct SavedLiminalState {
    enlisted: Vec<(i32, i32)>,
    aliases: Vec<(String, i32)>,
}

/// The ONE program-level lifecycle for consult, concat, and reconsult.
///
/// It owns the outer catalog savepoint, caller-state restoration, and
/// typed external-effect journal. Nested loads inherit the outer savepoint while
/// still receiving their own lexical enlist/alias save-and-restore boundary.
pub(crate) fn run_liminal_program<T>(
    system: &mut crate::system::DelightQLSystem,
    kind: crate::system::LiminalProgramKind,
    body: impl FnOnce(&mut crate::system::DelightQLSystem) -> Result<T>,
) -> Result<T> {
    let saved = SavedLiminalState {
        enlisted: system.save_enlisted_state()?,
        aliases: system.save_alias_state()?,
    };
    let ns_mark = system.max_namespace_id()?;
    let outermost = system.begin_liminal_program(ns_mark, kind)?;

    let result = body(system);

    let restore_enlisted = system.restore_enlisted_state(&saved.enlisted);
    let restore_aliases = system.restore_alias_state(&saved.aliases);
    let restore_result = match (restore_enlisted, restore_aliases) {
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    };
    let commit = result.is_ok() && restore_result.is_ok();

    if outermost {
        if !commit {
            // Catalog state needs no inverse walk: the program savepoint
            // restores it exactly, including mutations of pre-existing
            // namespaces. Only effects outside that connection require the
            // typed journal — drained HERE, at the one outermost close
            // — a nested drain would empty the OUTER program's journal too,
            // which is only safe while every nested failure provably aborts
            // the enclosing program; draining at the boundary needs no such
            // invariant. This includes a body
            // that succeeded but whose lexical state could not be restored:
            // that is still an aborted program, never a partial commit.
            system.rollback_liminal_external_effects();
        }
        let closed = system.end_liminal_program(commit);
        if let Err(close_error) = closed {
            // A failed commit RELEASE leaves the program
            // context (and its journal) alive. Compensate first, then ask the
            // same boundary to roll the catalog back. Only a successful
            // rollback clears the context; uncertainty quarantines the
            // session so no later query can observe a split world.
            if commit {
                system.rollback_liminal_external_effects();
                if let Err(rollback_error) = system.end_liminal_program(false) {
                    system.quarantine_session(
                        "liminal program close",
                        format!(
                            "commit close failed ({close_error}); rollback close failed ({rollback_error})"
                        ),
                    );
                }
            } else {
                system.quarantine_session("liminal program rollback", close_error.to_string());
            }

            if result.is_ok() {
                restore_result?;
                return Err(close_error);
            }
            // The body's error remains the primary failure, but a failed
            // rollback is still surfaced through the health latch above.
        } else if result.is_ok() {
            restore_result?;
        }
    } else if result.is_ok() {
        restore_result?;
    }

    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiminalDirectiveMode {
    /// A newly loaded source expects nested consult! targets to be new.
    Fresh,
    /// A reloaded source reconsults an existing nested child and consults a
    /// child that did not exist in the previous version.
    Replay,
}

/// One ledger row under construction.
///
/// A directive's receipt and a DEFINE row are settled the moment their form
/// is reached; a WITNESS is not, because its verdict may depend on the
/// definitions this very load registers. Its POSITION is taken here all the
/// same — the vector is the order — and only its verdict waits.
pub(crate) enum PreparedRow {
    Settled(crate::system::LiminalRow),
    PendingWitness(Witness),
}

/// Execute one file's liminal space, form by form, in AUTHORED ORDER, and
/// answer with THE LOAD it constructs: the ledger rows, the definition
/// clauses (taken out of the same walk that placed their DEFINE rows, so
/// ledger and registration cannot disagree), the deferred `doc!`s, and
/// every lexical edge as the act that performed it answered — bound for
/// `namespace`, which the load owns.
///
/// This is the single interpreter shared by consult/concat and reconsult;
/// the mode varies only the lifecycle of a nested `consult!` child. It walks
/// the file's own ordered sequence, so the ledger it prepares is one row per
/// top-level form in file-appearance order without anything reconstructing
/// that order afterwards.
pub(crate) fn execute_liminal_forms(
    system: &mut crate::system::DelightQLSystem,
    forms: Vec<ConsultedForm>,
    namespace: &str,
    file_path: &str,
    mode: LiminalDirectiveMode,
) -> Result<crate::system::PreparedLoad> {
    let mut prepared = crate::system::PreparedLoad::from_file(namespace, file_path, mode);
    // THE CLAUSES OF ONE SUBJECT CONTRIBUTE ONE ROW: the ledger records what
    // the load defined, never how many clauses spelled it, and the row
    // stands at the first clause's position.
    //
    // The key is the SUBJECT'S OWN identity, the same one registration
    // groups by — an unstropped name folds, a stropped one does not. Keying
    // on the catalog spelling would split `Counter` and `counter` into two
    // rows for one entity.
    let mut defined: Vec<crate::pipeline::asts::ddl::DefSubject> = Vec::new();

    for form in forms {
        let directive = match form {
            ConsultedForm::Definition(clause) => {
                if !defined.contains(&clause.front.subject) {
                    defined.push(clause.front.subject.clone());
                    prepared.settle(PreparedRow::Settled(crate::system::LiminalRow::Define {
                        entity: clause.front.subject.catalog_name(),
                    }));
                }
                prepared.define(clause);
                continue;
            }
            ConsultedForm::Witness(witness) => {
                prepared.settle(PreparedRow::PendingWitness(witness));
                continue;
            }
            ConsultedForm::Directive(directive) => directive,
        };
        let directive = &directive;
        prepared.settle(PreparedRow::Settled(crate::system::LiminalRow::Directive(
            liminal_receipt_for(&directive.name, &directive.args),
        )));
        {
            match directive.name.as_str() {
                // THE LEXICAL-EDGE DIRECTIVES ARE THE LOAD'S OWN ACTS: each
                // performs the session effect the entity would and records
                // the edge it performed — kind, shorthand, and selected
                // target, whole — in this load, in one step. The arguments
                // are bound by the same descriptor binder as every other
                // directive.
                "enlist" => {
                    let bound = bind_liminal_args("enlist", &directive.args, namespace)?;
                    prepared.enlist(system, &bound[0])?;
                }
                "alias" => {
                    let bound = bind_liminal_args("alias", &directive.args, namespace)?;
                    prepared.alias(system, &bound[1], &bound[0])?;
                }
                "consult" => {
                    if directive.args.len() != 2 {
                        return Err(DelightQLError::database_error(
                            format!(
                                "consult!() in DDL expects 2 arguments, got {}",
                                directive.args.len()
                            ),
                            "Invalid directive",
                        ));
                    }
                    let resolved_ns = resolve_ns_prefix(&directive.args[1], namespace)?;
                    if mode == LiminalDirectiveMode::Replay
                        && system.namespace_exists(&resolved_ns)?
                    {
                        system.reconsult_namespace(&resolved_ns, Some(&directive.args[0]))?;
                    } else {
                        execute_consult(system, &directive.args[0], &resolved_ns, Some(namespace))?;
                    }
                }
                "expose" => {
                    if directive.args.is_empty() {
                        return Err(DelightQLError::database_error(
                            "expose!() requires at least one namespace argument",
                            "Invalid directive",
                        ));
                    }
                    // An exposure selects its child NOW, as the child stands
                    // after the directives before it (a nested consult! has
                    // created it); the identity is what publication exposes.
                    for arg in &directive.args {
                        let child = resolve_ns_prefix(arg, namespace)?;
                        prepared.expose(system, &child)?;
                    }
                }
                "doc" => {
                    if directive.args.len() != 2 {
                        return Err(DelightQLError::database_error(
                            format!(
                            "doc!() in a liminal space expects 2 arguments (target, doc), got {}",
                            directive.args.len()
                        ),
                            "Invalid directive",
                        ));
                    }
                    prepared.doc(directive.args[0].clone(), directive.args[1].clone());
                }
                other => execute_liminal_via_entity(system, other, &directive.args, namespace)?,
            }
        }
    }

    Ok(prepared)
}

/// Prove the load's relational goals and settle the ledger.
///
/// A WITNESS RUNS WHERE ITS FILE'S DEFINITIONS ARE: after registration and
/// inside the consultation, so a goal may use what the submission it stands
/// in defines. It is READ-ONLY by construction — a `?-` relational goal is a
/// `relex`, which the grammar admits no directive into — and the compiled
/// kind is checked before anything runs, so a road that ever produced a
/// mutation here would refuse rather than write.
///
/// Order is the vector's. Nothing is searched for, matched by position
/// afterwards, or zipped back together.
pub(crate) fn prove_witnesses(
    system: &mut crate::system::DelightQLSystem,
    namespace: &str,
    rows: Vec<PreparedRow>,
) -> Result<Vec<crate::system::LiminalRow>> {
    let mut settled = Vec::with_capacity(rows.len());
    for row in rows {
        settled.push(match row {
            PreparedRow::Settled(row) => row,
            PreparedRow::PendingWitness(witness) => {
                let Witness { goal, canonical } = witness;
                let met = prove_goal(system, namespace, goal, &canonical)?;
                crate::system::LiminalRow::Goal {
                    met,
                    goal: canonical,
                }
            }
        });
    }
    Ok(settled)
}

/// Prove one relational goal: YES when the body holds of any row.
///
/// THE GOAL IS COMPILED WHOLE. Its declarations are its own — the same
/// entrance the prompt uses (`Pipeline::from_goal`) spends its danger and
/// option acknowledgments and registers its subordinate blocks. An ordinary
/// `assert!` effect may be demanded by the goal through the same typed plan;
/// it is not a separately compiled consultation sidecar.
fn prove_goal(
    system: &mut crate::system::DelightQLSystem,
    namespace: &str,
    goal: crate::pipeline::normalize::Goal,
    canonical: &str,
) -> Result<bool> {
    let compiled = crate::pipeline::Pipeline::new_consulted_goal(
        goal,
        system,
        namespace,
        crate::relation::Planning::open(crate::names::Registry::new(&[])),
    )
    .compile()
    .map_err(|e| witness_failed(canonical, e))?;

    // A LOAD MAY NOT WRITE USER DATA. The grammar already bars a directive
    // from a relational goal; this is the second fence, on what the goal
    // COMPILED to, so no future lowering can make a witness mutate.
    if compiled.kind != crate::pipeline::compiled_query::SqlKind::Query {
        return Err(DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::CONSULT_WITNESS_READ_ONLY,
            format!(
                "the consulted goal '?- {canonical}' compiled to a statement that writes: \
                 a consultation may READ user data only, through a top-level goal that \
                 proves and records a YES/NO witness"
            ),
            "a consulted witness may not write",
        ));
    }

    // The goal executes on the connection resolution routed it to, exactly
    // as the same body would at the prompt.
    let connection = match compiled.connection_id {
        Some(id) => system.get_connection(id)?,
        None => std::sync::Arc::clone(&system.connection),
    };
    let conn = connection.lock().map_err(|e| {
        DelightQLError::connection_poison_error(
            "Failed to acquire the connection lock for a consulted goal",
            format!("Connection was poisoned: {e}"),
        )
    })?;

    let (_, rows) = conn
        .query_all_rows(&compiled.primary_sql, &[])
        .map_err(|e| witness_failed(canonical, e))?;
    Ok(!rows.is_empty())
}

/// Whether a check's one cell says yes.
///
/// The relay's law, over the carrier a direct execution answers with: an
/// ABSENT cell is not a yes, and a check that answered NULL did not hold.
/// A witness that could not be compiled or executed ABORTS the load. The
/// liminal space's only stopper is abort, and a goal whose verdict is
/// unknown is not a NO.
fn witness_failed(spelling: &str, cause: impl std::fmt::Display) -> DelightQLError {
    DelightQLError::database_error(
        format!("the consulted goal '?- {spelling}' could not be proved: {cause}"),
        "witness failure",
    )
}

/// Execute a consult operation: read file, process embedded directives,
/// parse as DDL, and store definitions.
///
/// The consultation lifecycle: `consult!` creates ONE namespace from ONE
/// source. An existing destination refuses with the lifecycle teaching;
/// the caller chooses reload or deletion — there is no append and no
/// silent merge (one consulted source owns one namespace).
pub(crate) fn execute_consult(
    system: &mut crate::system::DelightQLSystem,
    file_path: &str,
    namespace: &str,
    _consulting_ns: Option<&str>,
) -> Result<usize> {
    // System name guard (catechism Deviation #3): a USER-TYPED consult target
    // may not land on a reserved system name. Applied to the already-resolved
    // namespace, so surface `consult!`, embedded `consult!` directives, and
    // `consult_tree!`'s per-file namespaces all pass through here.
    crate::system::validate_user_namespace_target(namespace)?;

    // THE LIFECYCLE REFUSAL: ordinary consult! creates ONE
    // namespace from ONE source. An existing destination refuses with the
    // lifecycle teaching; the caller chooses reload or deletion — never a
    // silent merge, and never an append: no later source joins an existing
    // consulted namespace.
    if system.namespace_exists(namespace)? {
        return Err(DelightQLError::validation_error_categorized(
            "directive/consult/exists",
            format!(
                "consult! creates namespace '{namespace}' from one source, and it \
                 already exists. Reload the same source with reconsult!(\"{namespace}\") \
                 or remove it first with unconsult!(\"{namespace}\") — one consulted \
                 source owns one namespace, and a second consult is never a merge"
            ),
            "consult lifecycle",
        ));
    }

    // Resolve relative path against session CWD (for test isolation).
    let resolved_path = crate::session_cwd::resolve_path(file_path);
    let file_path = resolved_path.display().to_string();
    let file_path = file_path.as_str();

    // Read the file
    let source = std::fs::read_to_string(file_path).map_err(|e| {
        DelightQLError::database_error(
            format!("consult!() failed to read file '{}': {}", file_path, e),
            "File read error",
        )
    })?;

    // ONE PARSE PER CONSULTED SUBMISSION. Parse errors get the consult!()
    // wrapper (and its error class); categorized validation errors — the
    // effect-algebra refusals, liminal eligibility, R-rule badges — pass
    // through UNWRAPPED, because a semantic refusal must not be re-badged as
    // a parse error.
    let consulted = Consulted::read(&source).map_err(|e| wrap_consult_parse_error(e, file_path))?;
    run_liminal_program(
        system,
        crate::system::LiminalProgramKind::Consult,
        |system| consult_body(system, consulted, file_path, namespace),
    )
}

/// The abortable middle of `execute_consult`: executes the file's liminal
/// directives, stores its definitions, and records the file's own declared
/// lexical graph. Every early `Err` return here is caught by
/// `execute_consult`, which restores the caller's saved state.
fn consult_body(
    system: &mut crate::system::DelightQLSystem,
    consulted: Consulted,
    file_path: &str,
    namespace: &str,
) -> Result<usize> {
    let Consulted {
        forms,
        ddl_blocks: inline_ddl_blocks,
    } = consulted;
    let load = execute_liminal_forms(
        system,
        forms,
        namespace,
        file_path,
        LiminalDirectiveMode::Fresh,
    )?;

    // ONE ORCHESTRATION BOUNDARY: the load is SPENT by publication — its
    // definitions, its doc!s, and its declared edges land together in one
    // transaction, for the destination and from the source the load owns.
    // A failure in any of them rolls the whole consultation back: no
    // namespace, no concat additions, no ledger. The result propagates
    // IMMEDIATELY: inline blocks must not run after a failed registration.
    let published = system.publish(load)?;
    let definitions_loaded = published.definitions_loaded();

    // Nested consultations share the outer program savepoint. There is no
    // inverse walk here: any block failure bubbles to the one runner, which
    // restores the complete catalog and the typed external journal.
    for block in &inline_ddl_blocks {
        let child_ns = match &block.namespace {
            Some(suffix) => format!("{}::{}", namespace, suffix),
            None => namespace.to_string(),
        };
        crate::pipeline::inline_ddl::register_inline_ddl_block(&block.body, &child_ns, system)
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Inline DDL block failed in consult of '{}': {}",
                        file_path, e
                    ),
                    "inline DDL error",
                )
            })?;
    }

    // THE WITNESSES RUN LAST, on the load the file just made: its
    // definitions are registered and its blocks are in place, so a goal
    // reads exactly what the consultation law makes available. The ledger
    // is written once, whole, in file-appearance order — and inside the
    // program savepoint, so a failure anywhere above or below rolls
    // definitions, effects and rows away together.
    let rows = prove_witnesses(system, namespace, published.into_ledger())?;
    system.record_liminal_ledger(namespace, &rows)?;

    Ok(definitions_loaded)
}

/// Wrap a parse failure from a consulted file in the consult!() context
/// (a `database_error`, so its class is `error://runtime` like every other
/// consult refusal), while letting categorized validation errors — the
/// effect-algebra refusals such as liminal eligibility and R-rule badges —
/// pass through UNWRAPPED, keeping their badges legible. Used by both
/// the extraction parse (complete-form segmentation) and the
/// cleaned-source parse so the two stages fail identically.
fn wrap_consult_parse_error(e: DelightQLError, file_path: &str) -> DelightQLError {
    if matches!(
        e,
        DelightQLError::ValidationError {
            subcategory: Some(_),
            ..
        }
    ) {
        return e;
    }
    // A TEACHING ABOUT THE DEFINITION'S OWN SHAPE keeps its badge: re-wrapping
    // it would bury the identity the teaching exists to publish. Every other
    // parse failure of a consulted file is a consult failure and is wrapped as
    // one — including a teaching about an expression inside it, which says
    // nothing about the file being a definition file.
    if let DelightQLError::ParseError {
        subcategory: Some(badge),
        ..
    } = &e
    {
        if crate::uri_registry::subcat::PARSE_DEFINITION_SHAPED.contains(badge) {
            return e;
        }
    }
    DelightQLError::database_error(
        format!("consult!() failed to parse '{}': {}", file_path, e),
        "Parse error",
    )
}

/// A recognized liminal statement. The typed shape lives with the effect
/// AST family: the extraction layer IS the liminal
/// loader, so its record IS the liminal-directive node.
pub(crate) use crate::pipeline::asts::effects::LiminalDirective as EmbeddedDirective;

/// A relational goal in a consulted file: a READ-ONLY YES/NO WITNESS.
///
/// The law admits exactly one way for a load to read user data, and this is
/// it. The witness proves inside the consultation and records what it found;
/// it may not write.
#[derive(Debug)]
pub(crate) struct Witness {
    /// The goal WHOLE, as normalization built it — its body and the
    /// declarations its own text owns. A sidecar belongs to the form that
    /// wrote it, so the witness carries them to its own compilation rather
    /// than being reduced to a bare query on the way here.
    pub goal: crate::pipeline::normalize::Goal,
    /// The body's CANONICAL spelling — the identity the ledger names it by.
    /// Two goals that differ only in layout are one goal to a ledger scan,
    /// which is the whole reason the law asks for a canonical spelling.
    pub canonical: String,
}

/// One TOP-LEVEL FORM of a consulted file.
///
/// The three the law admits: a definition (or fact), a session directive,
/// and a relational goal. Which one a `?-` form is comes from the PRODUCTION
/// the grammar admitted it under, carried here by normalization — nothing
/// asks the built tree a second time.
#[derive(Debug)]
pub(crate) enum ConsultedForm {
    Definition(crate::pipeline::asts::ddl::ClauseDecl),
    Directive(EmbeddedDirective),
    Witness(Witness),
}

/// One consulted submission, read ONCE.
///
/// The canonical entrance and the one normalization: definitions, the goals
/// the file states, and the subordinate blocks it declares all come out of a
/// single parse. Nothing regenerates DelightQL source in order to read it
/// again.
#[derive(Debug)]
pub(crate) struct Consulted {
    /// The file's top-level forms, in AUTHORED ORDER — ONE sequence,
    /// because the ledger is one row per form in file-appearance order and
    /// separately collected vectors cannot say which form came first.
    pub forms: Vec<ConsultedForm>,
    /// Subordinate blocks: a `(~~ddl … ~~)` at file scope belongs to its file
    /// and is processed inside the enclosing consultation's transaction.
    pub ddl_blocks: Vec<crate::pipeline::asts::core::InlineDdlSpec>,
}

impl Consulted {
    /// One clause per authored definition, in authored order. This is
    /// `DefinitionGroup::assemble`'s input, so registration parses nothing.
    pub fn into_definitions(self) -> Vec<crate::pipeline::asts::ddl::ClauseDecl> {
        self.forms
            .into_iter()
            .filter_map(|form| match form {
                ConsultedForm::Definition(clause) => Some(clause),
                ConsultedForm::Directive(_) | ConsultedForm::Witness(_) => None,
            })
            .collect()
    }

    /// How many definition clauses the file states.
    #[cfg(test)]
    pub fn definitions_len(&self) -> usize {
        self.forms
            .iter()
            .filter(|form| matches!(form, ConsultedForm::Definition(_)))
            .count()
    }

    /// The liminal directives the file states, in authored order.
    pub fn directives(&self) -> impl Iterator<Item = &EmbeddedDirective> {
        self.forms.iter().filter_map(|form| match form {
            ConsultedForm::Directive(directive) => Some(directive),
            ConsultedForm::Definition(_) | ConsultedForm::Witness(_) => None,
        })
    }

    /// The relational goals, in authored order.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn witnesses(&self) -> impl Iterator<Item = &Witness> {
        self.forms.iter().filter_map(|form| match form {
            ConsultedForm::Witness(witness) => Some(witness),
            ConsultedForm::Definition(_) | ConsultedForm::Directive(_) => None,
        })
    }
}

impl Consulted {
    /// Read a consulted submission through the canonical entrance.
    ///
    /// A DEFINITION FILE IS THE CANONICAL FORM, so a file of naked queries has
    /// no derivation here and refuses at the grammar rather than by a guard
    /// that counts what a second parse produced.
    pub(crate) fn read(source: &str) -> Result<Consulted> {
        use crate::pipeline::normalize::{GoalCategory, TopLevelForm};

        let tree = crate::pipeline::parse::definition_file(source)?;
        let normalized = crate::pipeline::normalize::definition_file(
            &tree,
            std::rc::Rc::new(crate::names::Registry::new(&[])),
        )?;
        let crate::pipeline::normalize::Normalized { forms, declared } = normalized;
        // AUTHORED ORDER SURVIVES THE READ. Every form keeps its position in
        // one sequence, because the ledger this load writes is one row per
        // form in file-appearance order.
        let mut consulted = Vec::with_capacity(forms.len());
        for form in forms {
            consulted.push(match form {
                TopLevelForm::Definition(clause) => ConsultedForm::Definition(clause),
                TopLevelForm::Goal(goal) => {
                    admit_liminal_declarations(&goal)?;
                    match goal.category {
                        GoalCategory::Effectual => {
                            ConsultedForm::Directive(liminal_directive(&goal.query)?)
                        }
                        GoalCategory::Relational => {
                            let canonical =
                                crate::term_spec::canonicalize_query(&goal.spelling, |detail| {
                                    unspellable_goal(&goal.spelling, detail)
                                })?;
                            ConsultedForm::Witness(Witness { goal, canonical })
                        }
                    }
                }
            });
        }
        Ok(Consulted {
            forms: consulted,
            ddl_blocks: declared.ddl_blocks,
        })
    }

    /// The same, for the loaders that do NOT execute directives — autoload
    /// and `sys::meta`, whose sources are genuinely stored text. A directive
    /// there is a LOUD error rather than a silent misparse. `context` names
    /// the caller. An inline `(~~ddl ~~)` block never reaches here: its body
    /// is typed definition content normalized with its enclosing submission.
    pub(crate) fn read_without_directives(source: &str, context: &str) -> Result<Consulted> {
        let consulted = Consulted::read(source).map_err(|e| match e {
            DelightQLError::ParseError { .. } => DelightQLError::database_error(
                format!("{context}: failed to parse DDL: {e}"),
                "Parse error",
            ),
            other => other,
        })?;
        let mut names: Vec<String> = consulted
            .directives()
            .map(|d| format!("{}!", d.name))
            .collect();
        // A relational goal is the load's one road to user data, and these
        // loaders have no load to attach one to.
        names.extend(consulted.witnesses().map(|w| format!("?- {}", w.canonical)));
        if !names.is_empty() {
            let names = names.join(", ");
            return Err(DelightQLError::database_error(
                format!(
                    "embedded liminal statements ({names}) are not supported in {context} — \
                     only consult!()/reconsult!() files execute them today"
                ),
                "Unsupported directive",
            ));
        }
        Ok(consulted)
    }
}

/// A goal whose body has no canonical spelling. The ledger names a goal by
/// one, so a body the canonicalizer cannot render has no ledger identity —
/// keeping the raw bytes would be a second spelling authority.
fn unspellable_goal(authored: &str, detail: String) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::CONSULT_GOAL_UNSPELLABLE,
        format!(
            "the consulted goal '?- {authored}' has no canonical spelling, so the load's \
             ledger cannot name it: {detail}"
        ),
        "a consulted goal is named by its canonical spelling",
    )
}

/// WHAT A LIMINAL FORM MAY DECLARE.
///
/// A sidecar belongs to the form that wrote it, and at load there is exactly
/// one road that can spend one: a relational goal COMPILES AND EXECUTES, so
/// its assertions, gate acknowledgments and subordinate blocks travel with
/// it into its own compilation. A session directive is not a relation — it
/// compiles nothing and the load does not shape its receipt — so a
/// declaration on one has no evaluator. An expected-error hook has no
/// meaning in either position: a load ABORTS on failure, so a form that
/// declares it expects to fail is declaring that the load must not finish.
///
/// Both refuse. Accepting a declaration and dropping it would let a false
/// assertion read as a met witness.
fn admit_liminal_declarations(goal: &crate::pipeline::normalize::Goal) -> Result<()> {
    use crate::pipeline::normalize::GoalCategory;

    let refuse = |what: &str, teaching: &str| {
        Err(DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::CONSULT_LIMINAL_DECLARATION,
            format!(
                "the liminal statement '?- {}' declares {what}, and a load has no road to \
                 spend it: {teaching}",
                goal.spelling
            ),
            "a liminal declaration with no evaluator",
        ))
    };

    if goal.declared.expected_error.is_some() {
        return refuse(
            "an expected error",
            "an error hook says the form must fail, and a failing form ABORTS the load — \
             state the expectation where the load is demanded instead",
        );
    }
    if goal.category == GoalCategory::Relational {
        // The witness's own compilation spends the rest.
        return Ok(());
    }
    if !goal.declared.dangers.is_empty() || !goal.declared.options.is_empty() {
        return refuse(
            "a danger or option acknowledgment",
            "a session directive compiles no query, so no gate is weighed for it to \
             acknowledge",
        );
    }
    if !goal.declared.ddl_blocks.is_empty() {
        return refuse(
            "a subordinate DDL block",
            "a block belongs to its FILE and is processed in the consultation's own \
             namespace — write it at file scope, not on a directive",
        );
    }
    Ok(())
}

/// The liminal directive ONE EFFECT goal is.
///
/// Which world a goal belongs to was settled by the production the grammar
/// admitted it under and carried here, so this reads only what an effect
/// goal must be: THE RULED LIMINAL FORM IS `name!(args)(*)` — the chain's
/// head is an effect-marked functor call, its scalar arguments are the
/// directive's, and a let block, a continuation, or an alias means the
/// author wrote a relation rather than a session directive.
fn liminal_directive(query: &Query) -> Result<EmbeddedDirective> {
    use crate::pipeline::asts::core::expressions::access::Access;
    use crate::pipeline::asts::core::{GroundForm, Relation, SealedCall};

    let not_eligible = |detail: String| {
        DelightQLError::validation_error_categorized(
            crate::pipeline::asts::effects::LIMINAL_NOT_ELIGIBLE_BADGE,
            detail,
            "not liminal-eligible",
        )
    };

    if !query.is_bare() {
        return Err(not_eligible(
            "a liminal statement is a session directive and takes no local \
             bindings — a preamble belongs to the relation it feeds"
                .to_string(),
        ));
    }
    let chain = &query.body;
    let GroundForm::Reference(Relation::FunctorCall { call, alias, .. }) = chain.head().form()
    else {
        return Err(not_eligible(
            "a liminal statement is one session directive — write \
             '<name>!(…)(*)'"
                .to_string(),
        ));
    };
    let name = call.call().callee.name_text();
    let Some(bare) = name.strip_suffix('!') else {
        return Err(not_eligible(format!(
            "'{name}' carries no directive mark, so it names a relation and not \
             a session directive"
        )));
    };

    // Liminal eligibility: only session directives.
    if !crate::pipeline::asts::effects::is_liminal_eligible(bare) {
        return Err(not_eligible(
            crate::pipeline::asts::effects::liminal_not_eligible_message(bare),
        ));
    }
    // The mark is the fence: a pure call under a session name is not a
    // directive, whatever it spells.
    let SealedCall::Effect(effect) = call else {
        return Err(not_eligible(format!(
            "a liminal '{bare}!' statement is a session directive, not a relation"
        )));
    };
    // THE RULED LIMINAL FORM IS `name!(args)(*)`. FN.24 gives the lone group
    // to receipt access, and only the WHOLE receipt is licensed here: every
    // other access reshapes the receipt as a relation, and running the effect
    // while discarding the shaping performs something the author did not
    // write. Read off the typed access, not off a second look at the source.
    let _ = effect;
    match chain.head_access().unwrap_or(&Access::Unasked) {
        Access::All => {}
        Access::Unasked => {
            return Err(not_eligible(format!(
                "a liminal '{bare}!' statement asks for its whole receipt — \
                 write '{bare}!(…)(*)'"
            )))
        }
        Access::Slots(_) | Access::Dequalify(_) | Access::DequalifyAll => {
            return Err(not_eligible(format!(
                "a liminal '{bare}!' statement takes the whole receipt, not a \
                 relational shaping of it — write '{bare}!(…)(*)', or state the \
                 directive as a query if you mean to read its receipt"
            )))
        }
    }
    // A liminal statement is exactly `name!(args)(*)`. A continuation and an
    // alias are relational vocabulary; neither has liminal meaning.
    if chain.has_steps() {
        return Err(not_eligible(format!(
            "a liminal '{bare}!' statement does not take a relational \
             continuation — it is a session directive, not a relation"
        )));
    }
    if alias.is_some() {
        return Err(not_eligible(format!(
            "a liminal '{bare}!' statement does not take an alias — \
             it is a session directive, not a relation"
        )));
    }
    // Session directives take strings and namespace paths, never relations.
    if call.call().relations().next().is_some()
        || call
            .call()
            .arguments
            .scalar_members()
            .iter()
            .any(|member| member.scalar_domain().is_none())
        || call
            .call()
            .arguments
            .ho_members()
            .any(|argument| argument.scalar_domain().is_none())
    {
        return Err(not_eligible(format!(
            "a liminal '{bare}!' statement takes string or namespace-path \
             arguments, not relations"
        )));
    }

    let mut args = Vec::new();
    for domain in call.call().arguments.value_domains() {
        let DomainExpression::Application(FunctionApplication::Ground(value)) = domain else {
            return Err(not_eligible(format!(
                "a liminal '{bare}!' statement takes ground arguments"
            )));
        };
        let text = match value {
            LiteralValue::String(text) => text.clone(),
            LiteralValue::Symbol(text) => text.clone(),
            other => other.to_string(),
        };
        if !text.is_empty() {
            args.push(text);
        }
    }

    Ok(EmbeddedDirective {
        name: bare.to_string(),
        args,
    })
}

/// Extract a string literal value from a DomainExpression
pub(super) fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => {
            Ok(s.clone())
        }
        _ => Err(DelightQLError::database_error(
            format!("consult!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}

#[cfg(test)]
mod tier1_tests {
    use super::*;

    #[test]
    fn shared_front_end_parses_clean_source_and_normalizes_whitespace() {
        // A trailing newline must not matter, and both definitions must
        // survive — the shape that started this thread.
        let src = "sm:(v) :- _:(v @ \"a\" -> \"X\"; _ -> \"Y\")\n\
                   myview(*) :- _(z @ 1) |> (z)\n";
        let consulted =
            Consulted::read_without_directives(src, "test").expect("clean DDL source should parse");
        assert_eq!(consulted.definitions_len(), 2);
    }

    #[test]
    fn shared_front_end_refuses_embedded_directives_loudly() {
        // A directive in a non-executing context is a LOUD error, not a
        // silent misparse.
        let src = "?- consult!(\"other.dql\", \"lib::x\")(*)\nmyview(*) :- _(z @ 1) |> (z)";
        let err = Consulted::read_without_directives(src, "autoload module 'sys::demo'")
            .expect_err("embedded directive must be refused");
        let msg = err.to_string();
        assert!(msg.contains("embedded liminal statements"), "{msg}");
        assert!(
            msg.contains("sys::demo"),
            "context should name the caller: {msg}"
        );
    }
}

#[cfg(test)]
mod liminal_abort_tests {
    //! A consult that aborts mid-file must
    //! restore the caller's enlist/alias state on ALL exit paths, not just
    //! the success path.

    use super::*;
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> crate::system::DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        crate::system::DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    /// A liminal doc! (session directive, R9-exempt)
    /// must load: it is deferred until the file's entities exist, and an
    /// unqualified target resolves against this file's namespace (with the
    /// `!` suffix fallback for effect rules). RED before 2.2: the doc! line
    /// aborted the consult with "pseudo-predicate doc!() is not supported in
    /// DDL files" — the TORTURE-TEST.dql liminal space could never load.
    #[test]
    fn liminal_doc_documents_this_files_effect_rule() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("docd.dql");
        std::fs::write(
            &path,
            "?- doc!(\"main\", \"documented at load\")(*)\n\n\
             main!(*) :- _(msg @ \"x\") |> insert!(audit_log(*))(*)\n",
        )
        .expect("write docd.dql");

        let mut system = fresh_system();
        execute_consult(&mut system, path.to_str().unwrap(), "docns", None)
            .expect("liminal doc! over this file's own effect rule must load");
    }

    /// A consulted file whose liminal space enlists (and aliases) and THEN
    /// aborts (missing file on the next line) must leave the session's
    /// enlist/alias state exactly as it was before the consult. RED before
    /// the all-paths restore: the early `?` return skipped the restore and
    /// the partial mutations leaked into the global session.
    #[test]
    fn liminal_abort_restores_enlist_and_alias_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lib_path = dir.path().join("lib.dql");
        std::fs::write(&lib_path, "helper(*) :- _(z @ 1) |> (z)\n").expect("write lib.dql");
        let missing_path = dir.path().join("missing.dql");
        let bad_path = dir.path().join("bad.dql");
        std::fs::write(
            &bad_path,
            format!(
                "?- consult!(\"{lib}\", \"leaklib\")(*)\n\
                 ?- enlist!(\"leaklib\")(*)\n\
                 ?- alias!(\"leaklib\", \"leak_alias\")(*)\n\
                 ?- consult!(\"{missing}\", \"gone\")(*)\n\
                 outer_view(*) :- _(z @ 1) |> (z)\n",
                lib = lib_path.display(),
                missing = missing_path.display(),
            ),
        )
        .expect("write bad.dql");

        let mut system = fresh_system();
        let enlisted_before = system.save_enlisted_state().expect("snapshot enlists");
        let aliases_before = system.save_alias_state().expect("snapshot aliases");

        let err = execute_consult(&mut system, bad_path.to_str().unwrap(), "outer", None)
            .expect_err("consult must abort on the missing file");
        assert!(
            err.to_string().contains("failed to read file"),
            "abort cause should be the missing file: {err}"
        );

        let enlisted_after = system.save_enlisted_state().expect("snapshot enlists");
        let aliases_after = system.save_alias_state().expect("snapshot aliases");
        assert_eq!(
            enlisted_after, enlisted_before,
            "aborted consult must not leak enlistments into the session"
        );
        assert_eq!(
            aliases_after, aliases_before,
            "aborted consult must not leak aliases into the session"
        );
    }
}

#[cfg(test)]
mod name_collision_tests {
    //! foo/foo! name collision.
    //! Both directions are pinned red-first by effects-ball
    //! rules--47/rules--48 (views vs effect rules). This module pins the
    //! scope: colon-functions (`foo:(x)`) register plain-named
    //! into the SAME entity table/namespace as relations, so they share the
    //! functor namespace and the collision rule covers them too — in both
    //! directions.

    use super::*;
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> crate::system::DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        crate::system::DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    fn consult_source(source: &str) -> crate::error::Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("collide.dql");
        std::fs::write(&path, source).expect("write collide.dql");
        let mut system = fresh_system();
        execute_consult(&mut system, path.to_str().unwrap(), "fnns", None).map(|_| ())
    }

    #[test]
    fn colon_function_collides_with_effect_rule_both_directions() {
        // Function first, effect rule second.
        let err = consult_source(
            "foo:(x) :- x + 1\n\n\
             foo!(*) :- _(msg @ \"x\") |> insert!(audit_log(*))(*)\n",
        )
        .expect_err("effect rule 'foo!' must refuse where function 'foo' exists");
        assert!(
            err.to_string()
                .contains("may not hold both 'foo' and 'foo!'"),
            "{err}"
        );
        assert!(
            err.to_string()
                .contains("already holds an entity named 'foo'"),
            "direction must name the pre-existing entity: {err}"
        );

        // Effect rule first, function second.
        let err = consult_source(
            "foo!(*) :- _(msg @ \"x\") |> insert!(audit_log(*))(*)\n\n\
             foo:(x) :- x + 1\n",
        )
        .expect_err("function 'foo' must refuse where effect rule 'foo!' exists");
        assert!(
            err.to_string()
                .contains("already holds an effect rule 'foo!'"),
            "direction must name the pre-existing effect rule: {err}"
        );
    }
}

#[cfg(test)]
mod directive_goal_tests {
    //! A LIMINAL DIRECTIVE IS AN EFFECT GOAL. The boundary these pin is the
    //! typed one: what the grammar admits as a top-level goal, what the goal's
    //! chain head has to be for it to be a directive, and which shapes carry a
    //! session directive's name without being one.

    use super::*;

    const VIEW: &str = "myview(*) :- _(z @ 1) |> (z)\n";

    /// A trailing `//` comment must not un-recognize a directive — comments
    /// are grammar extras.
    #[test]
    fn a_directive_goal_with_a_trailing_comment_is_recognized() {
        let src = format!("?- consult!(\"lib.dql\", \"lib\")(*)   // load the library\n{VIEW}");
        let consulted = Consulted::read(&src).expect("the file must read");
        assert_eq!(consulted.directives().count(), 1);
        assert_eq!(
            consulted.directives().nth(0).expect("a directive").name,
            "consult"
        );
        assert_eq!(
            consulted.directives().nth(0).expect("a directive").args,
            vec!["lib.dql", "lib"]
        );
        assert_eq!(consulted.definitions_len(), 1);
    }

    /// `//` inside a quoted string argument (a URL/path) is NOT a comment.
    #[test]
    fn a_double_slash_inside_a_string_argument_is_not_a_comment() {
        let src =
            format!("?- mount!(\"http://host//share/db.sqlite\", \"remote\")(*) // attach\n{VIEW}");
        let consulted = Consulted::read(&src).expect("the file must read");
        assert_eq!(consulted.directives().count(), 1);
        assert_eq!(
            consulted.directives().nth(0).expect("a directive").name,
            "mount"
        );
        assert_eq!(
            consulted.directives().nth(0).expect("a directive").args,
            vec!["http://host//share/db.sqlite", "remote"]
        );
    }

    /// A goal with a non-session name refuses with the liminal-eligibility
    /// teaching — unknown names and known non-session names alike.
    #[test]
    fn a_non_session_directive_goal_is_not_liminal_eligible() {
        let src = format!("?- frobnicate!(\"x\")(*)\n{VIEW}");
        let err = Consulted::read(&src).expect_err("a non-session directive goal must refuse");
        assert!(
            err.to_string()
                .contains("only session directives are liminal-eligible"),
            "{err}"
        );
    }

    /// ONLY THE WHOLE RECEIPT IS A LIMINAL DIRECTIVE.
    ///
    /// FN.24 gives the lone group to receipt access, and `?- name!(args)(*)`
    /// is the ruled form. A caller pattern and the two dequalifying accesses
    /// are relational shaping and therefore outside the liminal form. The
    /// liminal classifier owns that distinction before any directive runs.
    #[test]
    fn only_the_whole_receipt_is_a_liminal_directive() {
        // The positive control: the ruled form is still a directive.
        let src = format!("?- enlist!(\"std::string\")(*)\n{VIEW}");
        let consulted = Consulted::read(&src).expect("the ruled form is a directive");
        assert_eq!(consulted.directives().count(), 1);
        assert_eq!(
            consulted.directives().nth(0).expect("a directive").name,
            "enlist"
        );

        for shaped in ["(x)", "(*.(x))", "(.*)"] {
            let src = format!("?- enlist!(\"std::string\"){shaped}\n{VIEW}");
            let err = match Consulted::read(&src) {
                Err(err) => err,
                Ok(read) => panic!(
                    "a shaped receipt must refuse; {shaped} yielded {} directive(s)",
                    read.directives().count()
                ),
            };
            assert_eq!(
                err.error_uri(),
                "delightql-error://semantic/directive/liminal/not_eligible",
                "{shaped}: {err}"
            );
            assert!(err.to_string().contains("whole receipt"), "{shaped}: {err}");
        }
    }

    /// An effect-rule CLAUSE is a DEFINITION, never a directive. The head's
    /// bytes open like a directive's and the neck is what tells them apart.
    #[test]
    fn an_effect_rule_clause_is_a_definition() {
        let src = "touch!(*) :- _(msg @ \"touched\") |> insert!(audit_log(*))(*)\n";
        let consulted = Consulted::read(src).expect("the clause must read");
        assert!(consulted.directives().next().is_none());
        assert_eq!(consulted.definitions_len(), 1);
    }

    /// A rule head on one line with its body on the next is ONE definition:
    /// the body's demand is not a goal, so it can never be reclassified.
    #[test]
    fn a_multi_line_rule_body_is_not_a_directive() {
        let src = "union_them!(*) :- _(msg @ \"ran\") |> insert!(audit_log(*))(*)\n\
                   main!(*) :-\n    union_them!(*)\n";
        let consulted = Consulted::read(src).expect("the multi-line rule must read");
        assert!(consulted.directives().next().is_none());
        assert_eq!(consulted.definitions_len(), 2);
    }

    /// Comments and blank lines between head and body do not terminate a rule.
    #[test]
    fn comments_between_head_and_body_do_not_terminate_the_rule() {
        let src = "union_them!(*) :- _(msg @ \"ran\") |> insert!(audit_log(*))(*)\n\
                   main!(*) :-\n\n// the demand follows\n\n    union_them!(*)\n";
        let consulted = Consulted::read(src).expect("comments inside a rule must read");
        assert!(consulted.directives().next().is_none());
        assert_eq!(consulted.definitions_len(), 2);
    }

    /// An incomplete head is a parse refusal, never an installed empty rule.
    #[test]
    fn an_incomplete_head_is_refused() {
        let err = Consulted::read("main!(*) :-\n").expect_err("an incomplete head must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("parse error") || msg.contains("Parse error") || msg.contains("Syntax"),
            "{msg}"
        );
    }

    /// A directive goal may span physical lines: the FORM is the boundary.
    #[test]
    fn a_directive_goal_may_span_lines() {
        let src = format!("?- consult!(\"lib.dql\",\n         \"lib\")(*)\n{VIEW}");
        let consulted = Consulted::read(&src).expect("the multi-line goal must read");
        assert_eq!(consulted.directives().count(), 1);
        assert_eq!(
            consulted.directives().nth(0).expect("a directive").args,
            vec!["lib.dql", "lib"]
        );
    }

    /// A relational goal READS AS A WITNESS, and an effect goal as a
    /// directive, from the PRODUCTION each was admitted under — the two
    /// families come apart at the read, before anything is executed.
    #[test]
    fn a_relational_goal_reads_as_a_witness() {
        let src = format!("?- users(*)\n?- enlist!(\"std::string\")(*)\n{VIEW}");
        let consulted = Consulted::read(&src).expect("both goal families read");
        let witnesses: Vec<&str> = consulted
            .witnesses()
            .map(|witness| witness.canonical.as_str())
            .collect();
        assert_eq!(witnesses, vec!["users(*)"]);
        assert_eq!(consulted.directives().count(), 1);
        assert_eq!(
            consulted.directives().nth(0).expect("a directive").name,
            "enlist"
        );
    }

    /// AUTHORED ORDER IS THE READ'S. A file's forms come out as ONE
    /// sequence, so the ledger's file-appearance order needs no second look
    /// at the source.
    #[test]
    fn the_read_keeps_the_file_s_form_order() {
        let src = "?- enlist!(\"std::string\")(*)\n\
                   a(*) :- _(x @ 1)\n\
                   ?- a(*)\n\
                   b(*) :- _(y @ 2)\n";
        let consulted = Consulted::read(src).expect("the file reads");
        let shape: Vec<&str> = consulted
            .forms
            .iter()
            .map(|form| match form {
                ConsultedForm::Directive(_) => "directive",
                ConsultedForm::Definition(_) => "definition",
                ConsultedForm::Witness(_) => "witness",
            })
            .collect();
        assert_eq!(
            shape,
            vec!["directive", "definition", "witness", "definition"]
        );
    }

    /// A session name under a RELATIONAL vocabulary is not a directive: an
    /// alias and a continuation both say the author wrote a relation.
    #[test]
    fn a_directive_takes_no_alias_and_no_continuation() {
        let aliased = Consulted::read("?- enlist!(\"std::string\")(*) as e\n")
            .expect_err("a directive takes no alias");
        assert!(
            aliased.to_string().contains("does not take an alias"),
            "{aliased}"
        );

        let continued = Consulted::read("?- enlist!(\"std::string\")(*) |> (x)\n")
            .expect_err("a directive takes no continuation");
        assert!(
            continued.to_string().contains("does not take a relational"),
            "{continued}"
        );
    }
}

#[cfg(test)]
mod liminal_ledger_tests {
    //! THE LIMINAL RELATION's persistence pins.
    //! The presentation half (the catalog's `liminal` drill, corresponding
    //! union, insertion order) is pinned end-to-end by the effects-ball
    //! liminal--43/45 baselines; this module pins the ledger's LIFECYCLE:
    //! receipt schema per the ruled table, file-order collection (doc!'s
    //! deferral keeps its file position), abort leaving no ledger, reconsult
    //! replacing whole, unconsult killing it, and the empty liminal of a
    //! namespace created by other means.

    use super::*;
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> crate::system::DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        crate::system::DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    fn write_file(dir: &tempfile::TempDir, name: &str, source: &str) -> String {
        let path = dir.path().join(name);
        std::fs::write(&path, source).expect("write dql file");
        path.to_str().expect("utf8 path").to_string()
    }

    /// The receipt schema follows the ruled table: `operation` carries the
    /// name as written with `!`; echoes are named per the table, present-with-
    /// NULL for the optional forms, glob-join suffixed for expose!'s variadics.
    #[test]
    fn liminal_receipt_columns_follow_the_ruled_table() {
        let s = |v: &str| v.to_string();
        let cases: Vec<(&str, Vec<String>, Vec<(&str, Option<&str>)>)> = vec![
            (
                "consult",
                vec![s("a.dql"), s("ns")],
                vec![("path", Some("a.dql")), ("namespace", Some("ns"))],
            ),
            (
                "mount",
                vec![s("db.sqlite"), s("ns")],
                vec![("path", Some("db.sqlite")), ("namespace", Some("ns"))],
            ),
            // mount_new! takes the mount! row.
            (
                "mount_new",
                vec![s("fresh.db"), s("ns")],
                vec![("path", Some("fresh.db")), ("namespace", Some("ns"))],
            ),
            (
                "reconsult",
                vec![s("ns")],
                vec![("namespace", Some("ns")), ("path", None)],
            ),
            (
                "reconsult",
                vec![s("ns"), s("b.dql")],
                vec![("namespace", Some("ns")), ("path", Some("b.dql"))],
            ),
            ("unconsult", vec![s("ns")], vec![("namespace", Some("ns"))]),
            ("unmount", vec![s("ns")], vec![("namespace", Some("ns"))]),
            ("refresh", vec![s("ns")], vec![("namespace", Some("ns"))]),
            ("delist", vec![s("ns")], vec![("namespace", Some("ns"))]),
            (
                "ground",
                vec![s("d"), s("l"), s("g")],
                vec![
                    ("data_namespace", Some("d")),
                    ("lib_namespace", Some("l")),
                    ("namespace", Some("g")),
                ],
            ),
            (
                "enlist",
                vec![s("ns")],
                vec![("namespace", Some("ns")), ("into", None)],
            ),
            (
                "alias",
                vec![s("ns"), s("st")],
                vec![("namespace", Some("ns")), ("shorthand", Some("st"))],
            ),
            (
                "expose",
                vec![s("a"), s("b"), s("c")],
                vec![
                    ("namespace", Some("a")),
                    ("namespace_2", Some("b")),
                    ("namespace_3", Some("c")),
                ],
            ),
            (
                "doc",
                vec![s("main"), s("the doc")],
                vec![("target", Some("main")), ("doc", Some("the doc"))],
            ),
        ];
        for (name, args, expected) in cases {
            let r = liminal_receipt_for(name, &args);
            assert_eq!(
                r.operation,
                format!("{name}!"),
                "operation echoes the name as written"
            );
            let got: Vec<(&str, Option<&str>)> = r
                .echoes
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_deref()))
                .collect();
            assert_eq!(
                got, expected,
                "{name}!'s echo columns must follow the §8 table"
            );
        }
    }

    /// doc! executes AFTER registration (deferred) but its receipt keeps its
    /// FILE position in the ledger, not its execution time.
    #[test]
    fn liminal_ledger_doc_keeps_file_position() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_file(
            &dir,
            "docpos.dql",
            "?- enlist!(\"main\")(*)\n\
             ?- doc!(\"r\", \"documented at load\")(*)\n\
             ?- alias!(\"main\", \"m0\")(*)\n\n\
             r(*) :- _(x @ 1)\n",
        );
        let mut system = fresh_system();
        execute_consult(&mut system, &path, "docpos", None).expect("consult must load");
        let ops = system
            .liminal_ledger_operations("docpos")
            .expect("ledger read")
            .expect("namespace must exist");
        assert_eq!(
            ops,
            vec!["enlist!", "doc!", "alias!", "DEFINE"],
            "doc!'s receipt sits at its file position, between enlist! and alias!, \
             and the file's definition contributes its DEFINE row after them"
        );
    }

    /// The corresponding-union declared-addition columns arrive in
    /// first-appearance order across the file's mixed row schemas (enlist!
    /// contributes namespace+into, alias! adds shorthand, THE DEFINE ROW adds
    /// entity) — the schema the catalog drill presents (end-to-end:
    /// effects/liminal--45).
    #[test]
    fn liminal_echo_union_is_first_appearance_ordered() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_file(
            &dir,
            "union.dql",
            "?- enlist!(\"main\")(*)\n?- alias!(\"main\", \"m1\")(*)\n\nr(*) :- _(x @ 1)\n",
        );
        let mut system = fresh_system();
        execute_consult(&mut system, &path, "unionns", None).expect("consult must load");
        let (fq, union) = system
            .liminal_echo_columns("unionns")
            .expect("ledger read")
            .expect("namespace must exist");
        assert_eq!(fq, "unionns");
        assert_eq!(
            union,
            vec!["namespace", "into", "shorthand", "entity"],
            "union corresponding, first appearance wins the position"
        );
    }

    /// An aborted load leaves no namespace and no ledger — the ledger's
    /// existence is the success signal. Abort road: the file's second
    /// directive fails, so load publication (and the receipt write inside its
    /// transaction) is never reached; no orphan rows remain.
    #[test]
    fn liminal_ledger_abort_leaves_no_ledger() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("nope.dql");
        let path = write_file(
            &dir,
            "abort.dql",
            &format!(
                "?- enlist!(\"main\")(*)\n?- consult!(\"{}\", \"sub\")(*)\n\nr(*) :- _(x @ 1)\n",
                missing.display()
            ),
        );
        let mut system = fresh_system();
        let before = system.liminal_receipt_row_count();
        execute_consult(&mut system, &path, "abortns", None)
            .expect_err("consult must abort on the missing nested file");
        assert!(
            system
                .liminal_ledger_operations("abortns")
                .expect("ledger read")
                .is_none(),
            "aborted load must leave no namespace"
        );
        assert_eq!(
            system.liminal_receipt_row_count(),
            before,
            "aborted load must leave no ledger rows behind"
        );
    }

    /// A registration refusal AFTER the directives ran must also roll the
    /// ledger away with the namespace — the receipt write sits INSIDE the
    /// consult transaction (in-transaction half of the abort pin).
    #[test]
    fn liminal_ledger_registration_refusal_rolls_ledger_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        // R1 violation: a pure-named rule whose body ends in a directive —
        // refused by validate_effect_algebra_discipline during publication,
        // after the liminal directives executed and receipts were staged.
        let path = write_file(
            &dir,
            "refuse.dql",
            "?- enlist!(\"main\")(*)\n\n\
             bad(*) :- _(msg @ \"x\") |> insert!(audit_log(*))(*)\n",
        );
        let mut system = fresh_system();
        let before = system.liminal_receipt_row_count();
        execute_consult(&mut system, &path, "refusens", None)
            .expect_err("R1 refusal must abort the load");
        assert!(
            system
                .liminal_ledger_operations("refusens")
                .expect("ledger read")
                .is_none(),
            "refused load must leave no namespace"
        );
        assert_eq!(
            system.liminal_receipt_row_count(),
            before,
            "the consult transaction must roll the staged receipts away"
        );
    }

    /// Reconsulting a namespace replaces its ledger WHOLE: the record
    /// describes THE load, not the history of loads.
    #[test]
    fn liminal_ledger_reconsult_replaces_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path_a = write_file(
            &dir,
            "a.dql",
            "?- enlist!(\"main\")(*)\n?- alias!(\"main\", \"m2\")(*)\n\nr(*) :- _(x @ 1)\n",
        );
        let path_b = write_file(
            &dir,
            "b.dql",
            "?- enlist!(\"main\")(*)\n\nr(*) :- _(x @ 2)\n",
        );
        let mut system = fresh_system();
        execute_consult(&mut system, &path_a, "rens", None).expect("first consult");
        assert_eq!(
            system
                .liminal_ledger_operations("rens")
                .expect("read")
                .expect("ns"),
            vec!["enlist!", "alias!", "DEFINE"]
        );
        system
            .reconsult_namespace("rens", Some(&path_b))
            .expect("reconsult with the new file");
        assert_eq!(
            system
                .liminal_ledger_operations("rens")
                .expect("read")
                .expect("ns"),
            vec!["enlist!", "DEFINE"],
            "reconsult replaces the ledger whole — no residue of the first load"
        );
        let (_, union) = system
            .liminal_echo_columns("rens")
            .expect("read")
            .expect("ns");
        assert_eq!(
            union,
            vec!["namespace", "into", "entity"],
            "the union schema shrinks with the replacement: alias!'s shorthand is gone"
        );
    }

    /// The ledger dies with its namespace (unconsult) — catalog state,
    /// session-scoped, no orphan rows.
    #[test]
    fn liminal_ledger_dies_with_namespace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_file(
            &dir,
            "die.dql",
            "?- enlist!(\"main\")(*)\n\nr(*) :- _(x @ 1)\n",
        );
        let mut system = fresh_system();
        let before = system.liminal_receipt_row_count();
        execute_consult(&mut system, &path, "diens", None).expect("consult");
        // One row for the enlist! and one THE DEFINE ROW for `r`.
        assert_eq!(system.liminal_receipt_row_count(), before + 2);
        system.unconsult_namespace("diens").expect("unconsult");
        assert!(
            system
                .liminal_ledger_operations("diens")
                .expect("read")
                .is_none(),
            "namespace gone"
        );
        assert_eq!(
            system.liminal_receipt_row_count(),
            before,
            "the ledger died with the namespace — no orphan rows"
        );
    }

    /// A namespace created by other means has an EMPTY liminal: `main`
    /// (a data namespace) exists but was never consulted — the drill's
    /// schema source reports the bare receipt prefix (no echo columns) over
    /// zero receipt rows.
    #[test]
    fn liminal_ledger_empty_for_non_consulted() {
        let system = fresh_system();
        let (fq, union) = system
            .liminal_echo_columns("main")
            .expect("read")
            .expect("main exists");
        assert_eq!(fq, "main");
        assert!(union.is_empty(), "no receipts, no echo columns");
        assert_eq!(
            system
                .liminal_ledger_operations("main")
                .expect("read")
                .expect("main exists"),
            Vec::<String>::new(),
            "empty ledger for a namespace created by other means"
        );
    }

    /// THE DEFINE ROW: one row per defined ENTITY, however many clauses
    /// spelled it, at the first clause's position — and interleaved with the
    /// directives in the file's own order.
    #[test]
    fn liminal_ledger_defines_once_per_entity_in_file_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_file(
            &dir,
            "defines.dql",
            "a(*) :- _(x @ 1)\n\
             ?- enlist!(\"main\")(*)\n\
             b(*) :- _(y @ 1)\n\
             b(*) :- _(y @ 2)\n\
             a(*) :- _(x @ 3)\n",
        );
        let mut system = fresh_system();
        execute_consult(&mut system, &path, "defns", None).expect("consult must load");
        assert_eq!(
            system
                .liminal_ledger_operations("defns")
                .expect("ledger read")
                .expect("namespace must exist"),
            vec!["DEFINE", "enlist!", "DEFINE"],
            "`a` is defined by clauses 1 and 5 and contributes ONE row, at the \
             first clause's position; `b`'s two clauses contribute one more"
        );
    }

    /// A WITNESS FAILURE ABORTS THE LOAD, whole. The liminal space's only
    /// stopper is abort, and a goal whose verdict is unknown is not a NO:
    /// definitions, effects and ledger rows roll back together.
    #[test]
    fn liminal_witness_failure_rolls_the_whole_load_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_file(
            &dir,
            "badgoal.dql",
            "?- enlist!(\"main\")(*)\n\
             r(*) :- _(x @ 1)\n\
             ?- nosuchrelation(*)\n",
        );
        let mut system = fresh_system();
        let before = system.liminal_receipt_row_count();
        let err = execute_consult(&mut system, &path, "badns", None)
            .expect_err("an unprovable goal aborts the load");
        assert!(err.to_string().contains("could not be proved"), "{err}");
        assert!(
            system
                .liminal_ledger_operations("badns")
                .expect("ledger read")
                .is_none(),
            "the aborted load left no namespace"
        );
        assert_eq!(
            system.liminal_receipt_row_count(),
            before,
            "the aborted load left no ledger rows"
        );
    }
}
