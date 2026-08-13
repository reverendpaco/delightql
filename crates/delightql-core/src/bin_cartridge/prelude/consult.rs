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
        // The interior-echo directives (consult!'s family): their DIRECT
        // receipts carry `input ⟦path, namespace⟧`, but the liminal LEDGER
        // row stays FLAT pending the ledger's interior adoption — path,
        // namespace, for both the fresh consult and the explicit concat.
        "consult" | "consult_concat_into_ns" => vec![
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

/// `consult_concat_into_ns!()` — the EXPLICIT opt-in to multi-source
/// namespaces: adds another source
/// file to an EXISTING consulted namespace. The verbosity is deliberate:
/// callers must choose merged source environments rather than receive
/// them from a repeated `consult!` (which refuses). The namespace's
/// enlist/alias environment is NAMESPACE-WIDE: both sources' liminal
/// enlists accumulate in the namespace-owned edge tables the scoped
/// lookups read, so each file's definitions see the other's imports.
pub struct ConsultConcatPredicate;

impl BinEntity for ConsultConcatPredicate {
    fn name(&self) -> &str {
        "consult_concat_into_ns!"
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
            // The receipt heading is the DESCRIPTOR's declaration:
            // consult!'s shape — core + `input` echo + `returned`
            // payload (the namespace the source joined).
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema(
                "consult_concat_into_ns",
            )),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for ConsultConcatPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "consult_concat_into_ns!() expects 2 arguments (file_path, namespace), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }
        let file_path = extract_string_literal(&arguments[0], "file_path")?;
        let namespace = extract_string_literal(&arguments[1], "namespace")?;

        // The destination-class invariant lives in the SHARED
        // execute_consult_mode, so every route (this entity, the embedded
        // liminal arm, the reconsult pass) receives it inductively.
        let _count =
            execute_consult_mode(system, &file_path, &namespace, None, ConsultMode::Concat)?;

        Ok(EntityResult::Relation(super::interior_receipt_result(
            "consult_concat_into_ns!",
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
    let mut bound: Vec<DomainExpression> = Vec::with_capacity(args.len());
    for (param, arg) in desc.params.iter().zip(args) {
        let value = if param.kind == DirectiveParamKind::Namespace {
            resolve_ns_prefix(arg, consulting_ns)?
        } else {
            arg.clone()
        };
        bound.push(DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(value),)));
    }
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

/// Execute a consult operation: read file, process embedded directives,
/// parse as DDL, and store definitions.
///
/// `consulting_ns` is the namespace of the DDL that triggered this consult.
/// When present, `.::` and `::` prefixes in embedded directives are resolved
/// relative to `namespace` (the target namespace for this file).
/// How a consultation enters an existing namespace (the
/// consultation lifecycle): ordinary `consult!` REFUSES an existing
/// destination — a second consult is never implicit concatenation or
/// replacement; `consult_concat_into_ns!` is the explicit opt-in.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsultMode {
    Fresh,
    Concat,
}

pub(crate) fn execute_consult(
    system: &mut crate::system::DelightQLSystem,
    file_path: &str,
    namespace: &str,
    consulting_ns: Option<&str>,
) -> Result<usize> {
    execute_consult_mode(
        system,
        file_path,
        namespace,
        consulting_ns,
        ConsultMode::Fresh,
    )
}

/// Caller-visible session state captured at the boundary of one liminal
/// program. The shared runner passes it to the load body so namespace-local
/// deltas can be computed before the runner restores the caller.
pub(crate) struct SavedLiminalState {
    pub enlisted: Vec<(i32, i32)>,
    pub aliases: Vec<(String, i32)>,
}

/// The ONE program-level lifecycle for consult, concat, and reconsult.
///
/// It owns the outer catalog savepoint, caller-state restoration, and
/// typed external-effect journal. Nested loads inherit the outer savepoint while
/// still receiving their own lexical enlist/alias save-and-restore boundary.
pub(crate) fn run_liminal_program<T>(
    system: &mut crate::system::DelightQLSystem,
    kind: crate::system::LiminalProgramKind,
    body: impl FnOnce(&mut crate::system::DelightQLSystem, &SavedLiminalState) -> Result<T>,
) -> Result<T> {
    let saved = SavedLiminalState {
        enlisted: system.save_enlisted_state()?,
        aliases: system.save_alias_state()?,
    };
    let ns_mark = system.max_namespace_id()?;
    let outermost = system.begin_liminal_program(ns_mark, kind)?;

    let result = body(system, &saved);

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

pub(crate) struct PreparedLiminalDirectives {
    pub receipts: Vec<crate::system::LiminalReceipt>,
    pub deferred_exposes: Vec<Vec<String>>,
    pub deferred_docs: Vec<(String, String)>,
}

/// Execute one file's liminal directive sequence. This is the single
/// interpreter shared by consult/concat and reconsult; the mode varies only
/// the lifecycle of a nested `consult!` child.
pub(crate) fn execute_liminal_directives(
    system: &mut crate::system::DelightQLSystem,
    directives: &[EmbeddedDirective],
    namespace: &str,
    mode: LiminalDirectiveMode,
) -> Result<PreparedLiminalDirectives> {
    let mut prepared = PreparedLiminalDirectives {
        receipts: Vec::with_capacity(directives.len()),
        deferred_exposes: Vec::new(),
        deferred_docs: Vec::new(),
    };

    for directive in directives {
        prepared
            .receipts
            .push(liminal_receipt_for(&directive.name, &directive.args));
        match directive.name.as_str() {
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
                if mode == LiminalDirectiveMode::Replay && system.namespace_exists(&resolved_ns)? {
                    system.reconsult_namespace(&resolved_ns, Some(&directive.args[0]))?;
                } else {
                    execute_consult(system, &directive.args[0], &resolved_ns, Some(namespace))?;
                }
            }
            "consult_concat_into_ns" => {
                if directive.args.len() != 2 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "consult_concat_into_ns!() in DDL expects 2 arguments, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[1], namespace)?;
                execute_consult_mode(
                    system,
                    &directive.args[0],
                    &resolved_ns,
                    Some(namespace),
                    ConsultMode::Concat,
                )?;
            }
            "expose" => {
                if directive.args.is_empty() {
                    return Err(DelightQLError::database_error(
                        "expose!() requires at least one namespace argument",
                        "Invalid directive",
                    ));
                }
                prepared.deferred_exposes.push(
                    directive
                        .args
                        .iter()
                        .map(|arg| resolve_ns_prefix(arg, namespace))
                        .collect::<Result<Vec<_>>>()?,
                );
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
                prepared
                    .deferred_docs
                    .push((directive.args[0].clone(), directive.args[1].clone()));
            }
            other => execute_liminal_via_entity(system, other, &directive.args, namespace)?,
        }
    }

    Ok(prepared)
}

pub(crate) fn execute_consult_mode(
    system: &mut crate::system::DelightQLSystem,
    file_path: &str,
    namespace: &str,
    _consulting_ns: Option<&str>,
    mode: ConsultMode,
) -> Result<usize> {
    // System name guard (catechism Deviation #3): a USER-TYPED consult target
    // may not land on a reserved system name. Applied to the already-resolved
    // namespace, so surface `consult!`, embedded `consult!` directives, and
    // `consult_tree!`'s per-file namespaces all pass through here.
    crate::system::validate_user_namespace_target(namespace)?;

    // THE CONCAT CLASS INVARIANT, in the SHARED implementation so every
    // route receives it inductively — a check on the entity alone leaves the
    // embedded arm to bypass it: concat extends an EXISTING file-consulted
    // library namespace only.
    if mode == ConsultMode::Concat {
        match system.namespace_kind_and_provenance(namespace)? {
            None => {
                return Err(DelightQLError::validation_error_categorized(
                    "directive/consult_concat/missing",
                    format!(
                        "consult_concat_into_ns! ADDS a source to an existing consulted \
                         namespace, and '{namespace}' does not exist — consult the first \
                         source ordinarily: consult!(\"<file>\", \"{namespace}\")"
                    ),
                    "concat target missing",
                ));
            }
            Some((kind, provenance)) => {
                if !(kind == "lib" && provenance.as_deref() == Some("file")) {
                    return Err(DelightQLError::validation_error_categorized(
                        "directive/consult_concat/not_consulted",
                        format!(
                            "consult_concat_into_ns! extends a CONSULTED namespace; \
                             '{namespace}' is a {kind} namespace — it holds \
                             {}, not consulted sources",
                            match kind.as_str() {
                                "data" => "your database's tables",
                                "system" => "engine machinery",
                                "scratch" => "in-session scratch definitions",
                                _ => "something else",
                            }
                        ),
                        "concat target class",
                    ));
                }
            }
        }
    }

    // A consulted file may concatenate sources into a namespace it is
    // constructing, but may not imperatively extend a caller-owned namespace.
    // The catalog savepoint makes this reversible; the refusal remains the
    // file/session policy. Prompt-level concat keeps its ordinary purpose.
    if mode == ConsultMode::Concat {
        system.refuse_preexisting_namespace_mutation_in_program(
            namespace,
            "adding a source to",
            "directive/consult_concat/uncompensable",
        )?;
    }

    // THE LIFECYCLE REFUSAL: ordinary consult! creates ONE
    // namespace from ONE source. An existing destination refuses with the
    // lifecycle teaching; the caller chooses reload, deletion, or explicit
    // concatenation — never receives a silent merge.
    if mode == ConsultMode::Fresh && system.namespace_exists(namespace)? {
        return Err(DelightQLError::validation_error_categorized(
            "directive/consult/exists",
            format!(
                "consult! creates namespace '{namespace}' from one source, and it \
                 already exists. Reload the same source with reconsult!(\"{namespace}\"); \
                 remove it first with unconsult!(\"{namespace}\"); or ADD another \
                 source explicitly with consult_concat_into_ns!(\"<file>\", \"{namespace}\") \
                 — a second ordinary consult is never an implicit merge"
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
        |system, saved| {
            consult_body(
                system,
                consulted,
                file_path,
                namespace,
                &saved.enlisted,
                &saved.aliases,
                mode,
            )
        },
    )
}

/// The abortable middle of `execute_consult`: executes the file's liminal
/// directives, stores its definitions, and (on success) records + restores
/// enlist/alias state. Every early `Err` return here is caught by
/// `execute_consult`, which restores the caller's saved state.
fn consult_body(
    system: &mut crate::system::DelightQLSystem,
    consulted: Consulted,
    file_path: &str,
    namespace: &str,
    saved_enlisted: &[(i32, i32)],
    saved_aliases: &[(String, i32)],
    mode: ConsultMode,
) -> Result<usize> {
    let Consulted {
        definitions,
        directives,
        ddl_blocks: inline_ddl_blocks,
    } = consulted;
    let prepared =
        execute_liminal_directives(system, &directives, namespace, LiminalDirectiveMode::Fresh)?;
    let liminal_receipts = prepared.receipts;
    let deferred_exposes = prepared.deferred_exposes;
    let deferred_docs = prepared.deferred_docs;

    // ONE ORCHESTRATION BOUNDARY: the
    // enlist/alias deltas are computed BEFORE registration (the liminal
    // directives already ran), and everything that must land atomically
    // with registration — deferred exposes, deferred doc!s, the
    // namespace-local edge recording, per-source provenance — is applied
    // INSIDE consult_file's catalog transaction. A failure in any of them
    // rolls the whole consultation back: no namespace, no concat
    // additions, no ledger.
    let current_enlisted = system.save_enlisted_state()?;
    let current_aliases = system.save_alias_state()?;
    let new_enlists: Vec<(i32, i32)> = current_enlisted
        .iter()
        .filter(|row| !saved_enlisted.contains(row))
        .cloned()
        .collect();
    let new_aliases: Vec<(String, i32)> = current_aliases
        .iter()
        .filter(|row| !saved_aliases.contains(row))
        .cloned()
        .collect();

    // Concat + inline blocks remains a semantic refusal: concatenating a
    // source must not silently invent child-module merge semantics. Atomicity
    // itself is no longer the limitation; the outer program savepoint spans
    // the nested block consultations.
    if !inline_ddl_blocks.is_empty() && mode == ConsultMode::Concat {
        return Err(DelightQLError::validation_error_categorized(
            "directive/consult_concat/inline_ddl",
            format!(
                "consult_concat_into_ns! source '{file_path}' carries inline \
                 (~~ddl~~) blocks — concat cannot yet orchestrate them \
                 atomically. Consult them in the FIRST source, or as their \
                 own consult!"
            ),
            "concat inline ddl",
        ));
    }

    let post = crate::system::ConsultPost {
        deferred_exposes,
        deferred_docs,
        new_enlists: &new_enlists,
        new_aliases: &new_aliases,
        record_source: true,
    };
    // The registration result propagates IMMEDIATELY: inline blocks and
    // exposes must not run after a failed registration.
    let definitions_loaded = system
        .consult_file(
            file_path,
            namespace,
            definitions,
            &liminal_receipts,
            Some(&post),
        )
        .map(|cr| cr.definitions_loaded)?;

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

/// One consulted submission, read ONCE.
///
/// The canonical entrance and the one normalization: definitions, the goals
/// the file states, and the subordinate blocks it declares all come out of a
/// single parse. Nothing regenerates DelightQL source in order to read it
/// again.
#[derive(Debug)]
pub(crate) struct Consulted {
    /// One clause per authored definition, in authored order. This is
    /// `DefinitionGroup::assemble`'s input, so registration parses nothing.
    pub definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    /// The liminal directives the file states, in authored order.
    pub directives: Vec<EmbeddedDirective>,
    /// Subordinate blocks: a `(~~ddl … ~~)` at file scope belongs to its file
    /// and is processed inside the enclosing consultation's transaction.
    pub ddl_blocks: Vec<crate::pipeline::asts::core::InlineDdlSpec>,
}

impl Consulted {
    /// Read a consulted submission through the canonical entrance.
    ///
    /// A DEFINITION FILE IS THE CANONICAL FORM, so a file of naked queries has
    /// no derivation here and refuses at the grammar rather than by a guard
    /// that counts what a second parse produced.
    pub(crate) fn read(source: &str) -> Result<Consulted> {
        let tree = crate::pipeline::parse::definition_file(source)?;
        let normalized = crate::pipeline::normalize::definition_file(
            &tree,
            std::rc::Rc::new(crate::names::Registry::new(&[])),
        )?;
        let crate::pipeline::normalize::Normalized {
            queries,
            definitions,
            declared,
        } = normalized;
        let mut directives = Vec::with_capacity(queries.len());
        for goal in &queries {
            directives.push(liminal_directive(&goal.query)?);
        }
        Ok(Consulted {
            definitions,
            directives,
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
        if !consulted.directives.is_empty() {
            let names = consulted
                .directives
                .iter()
                .map(|d| format!("{}!", d.name))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(DelightQLError::database_error(
                format!(
                    "embedded directives ({names}) are not supported in {context} — only \
                     consult!()/reconsult!() files execute them today"
                ),
                "Unsupported directive",
            ));
        }
        Ok(consulted)
    }
}

/// The liminal directive ONE top-level goal is.
///
/// A liminal directive IS an effect goal (FN.36) — `?- name!(args)(*)` — and
/// the reading is typed: the chain's head is an effect-marked functor call,
/// its scalar arguments are the directive's, and a continuation or an alias
/// means the author wrote a relation rather than a session directive.
///
/// A goal that is not a directive at all is a RELATIONAL WITNESS. The law
/// admits one; proving it during consultation is the gap named here, and it is
/// deliberately not a parse-time refusal.
fn liminal_directive(query: &Query) -> Result<EmbeddedDirective> {
    use crate::pipeline::asts::core::expressions::access::Access;
    use crate::pipeline::asts::core::{Grelex, Relation, SealedCall};

    if !query.is_bare() {
        return Err(relational_witness_gap());
    }
    let chain = &query.body;
    let Grelex::Reference(Relation::FunctorCall { call, alias, .. }) = &chain.head else {
        return Err(relational_witness_gap());
    };
    let name = call.call().callee.name_text();
    let Some(bare) = name.strip_suffix('!') else {
        return Err(relational_witness_gap());
    };

    let not_eligible = |detail: String| {
        DelightQLError::validation_error_categorized(
            crate::pipeline::asts::effects::LIMINAL_NOT_ELIGIBLE_BADGE,
            detail,
            "not liminal-eligible",
        )
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

/// A relational goal in a consulted file is a READ-ONLY WITNESS (FN.36): it
/// proves and yields a YES/NO receipt. The grammar admits it and normalization
/// builds it; what is missing is the proving, which needs an execution
/// boundary consultation does not have. Named here so the gap is one thing a
/// reader can find, not a generic refusal.
fn relational_witness_gap() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "consult/witness/unbuilt",
        "a relational goal in a consulted file is a read-only witness, and \
         proving one during consultation is not built yet — state the goal \
         after the consultation instead",
        "relational witness in a consulted file",
    )
}

/// Extract a string literal value from a DomainExpression
pub(super) fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => Ok(s.clone()),
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
        assert_eq!(consulted.definitions.len(), 2);
    }

    #[test]
    fn shared_front_end_refuses_embedded_directives_loudly() {
        // A directive in a non-executing context is a LOUD error, not a
        // silent misparse.
        let src = "?- consult!(\"other.dql\", \"lib::x\")(*)\nmyview(*) :- _(z @ 1) |> (z)";
        let err = Consulted::read_without_directives(src, "autoload module 'sys::demo'")
            .expect_err("embedded directive must be refused");
        let msg = err.to_string();
        assert!(msg.contains("embedded directives"), "{msg}");
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
        assert_eq!(consulted.directives.len(), 1);
        assert_eq!(consulted.directives[0].name, "consult");
        assert_eq!(consulted.directives[0].args, vec!["lib.dql", "lib"]);
        assert_eq!(consulted.definitions.len(), 1);
    }

    /// `//` inside a quoted string argument (a URL/path) is NOT a comment.
    #[test]
    fn a_double_slash_inside_a_string_argument_is_not_a_comment() {
        let src =
            format!("?- mount!(\"http://host//share/db.sqlite\", \"remote\")(*) // attach\n{VIEW}");
        let consulted = Consulted::read(&src).expect("the file must read");
        assert_eq!(consulted.directives.len(), 1);
        assert_eq!(consulted.directives[0].name, "mount");
        assert_eq!(
            consulted.directives[0].args,
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
    /// are RELATIONAL shaping of the receipt; the shaping has no liminal
    /// meaning, and executing the effect while discarding it performs
    /// something the author did not write. Read off the typed access — the
    /// mark, the alias, and the continuations are checked the same way, and a
    /// second syntactic road beside them is a second answer.
    #[test]
    fn only_the_whole_receipt_is_a_liminal_directive() {
        // The positive control: the ruled form is still a directive.
        let src = format!("?- enlist!(\"std::string\")(*)\n{VIEW}");
        let consulted = Consulted::read(&src).expect("the ruled form is a directive");
        assert_eq!(consulted.directives.len(), 1);
        assert_eq!(consulted.directives[0].name, "enlist");

        for shaped in ["(x)", "(*.(x))", "(.*)"] {
            let src = format!("?- enlist!(\"std::string\"){shaped}\n{VIEW}");
            let err = match Consulted::read(&src) {
                Err(err) => err,
                Ok(read) => panic!(
                    "a shaped receipt must refuse; {shaped} yielded {} directive(s)",
                    read.directives.len()
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
        assert!(consulted.directives.is_empty());
        assert_eq!(consulted.definitions.len(), 1);
    }

    /// A rule head on one line with its body on the next is ONE definition:
    /// the body's demand is not a goal, so it can never be reclassified.
    #[test]
    fn a_multi_line_rule_body_is_not_a_directive() {
        let src = "union_them!(*) :- _(msg @ \"ran\") |> insert!(audit_log(*))(*)\n\
                   main!(*) :-\n    union_them!(*)\n";
        let consulted = Consulted::read(src).expect("the multi-line rule must read");
        assert!(consulted.directives.is_empty());
        assert_eq!(consulted.definitions.len(), 2);
    }

    /// Comments and blank lines between head and body do not terminate a rule.
    #[test]
    fn comments_between_head_and_body_do_not_terminate_the_rule() {
        let src = "union_them!(*) :- _(msg @ \"ran\") |> insert!(audit_log(*))(*)\n\
                   main!(*) :-\n\n// the demand follows\n\n    union_them!(*)\n";
        let consulted = Consulted::read(src).expect("comments inside a rule must read");
        assert!(consulted.directives.is_empty());
        assert_eq!(consulted.definitions.len(), 2);
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
        assert_eq!(consulted.directives.len(), 1);
        assert_eq!(consulted.directives[0].args, vec!["lib.dql", "lib"]);
    }

    /// A relational goal is a READ-ONLY WITNESS, and proving one during
    /// consultation is a named gap rather than a parse refusal: the grammar
    /// admits it and normalization builds it.
    #[test]
    fn a_relational_goal_is_a_witness_and_not_a_parse_refusal() {
        let src = format!("?- users(*)\n{VIEW}");
        let tree = crate::pipeline::parse::definition_file(&src)
            .expect("a relational goal is admitted by the grammar");
        assert!(!tree.has_defects());
        let err = Consulted::read(&src).expect_err("proving a witness is not built yet");
        assert!(err.to_string().contains("read-only witness"), "{err}");
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
            vec!["enlist!", "doc!", "alias!"],
            "doc!'s receipt sits at its file position, between enlist! and alias!"
        );
    }

    /// The corresponding-union echo columns arrive in first-appearance order
    /// across the file's mixed directive schemas (enlist! contributes
    /// namespace+into, alias! adds shorthand) — the schema the catalog drill
    /// presents (end-to-end: effects/liminal--45).
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
            vec!["namespace", "into", "shorthand"],
            "union corresponding, first appearance wins the position"
        );
    }

    /// An aborted load leaves no namespace and no ledger — the ledger's
    /// existence is the success signal. Abort road: the file's second
    /// directive fails, so consult_file (and the receipt write inside its
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
        // refused by validate_effect_algebra_discipline inside consult_file,
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
            vec!["enlist!", "alias!"]
        );
        system
            .reconsult_namespace("rens", Some(&path_b))
            .expect("reconsult with the new file");
        assert_eq!(
            system
                .liminal_ledger_operations("rens")
                .expect("read")
                .expect("ns"),
            vec!["enlist!"],
            "reconsult replaces the ledger whole — no residue of the first load"
        );
        let (_, union) = system
            .liminal_echo_columns("rens")
            .expect("read")
            .expect("ns");
        assert_eq!(
            union,
            vec!["namespace", "into"],
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
        assert_eq!(system.liminal_receipt_row_count(), before + 1);
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
}
