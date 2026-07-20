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
use crate::pipeline::parser::{parse, parse_ddl_file};

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

/// Build the receipt row for one executed liminal directive (EFFECT-ALGEBRA
/// §8, THE LIMINAL RELATION). `name` is the directive name WITHOUT the `!`
/// (as the narrowed extraction delivers it); `args` are the arguments as
/// written in the file — receipts ECHO parameters, they never measure (§3).
/// Echo column names follow the ruled §8 table exactly: a namespace argument
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
        // row stays FLAT pending the ledger's interior adoption (§8 status
        // note) — path, namespace, for both the fresh consult and the
        // explicit concat (review otolxyzl::qmqwqlms P2: concat recorded
        // positional arg_N before this).
        "consult" | "consult_concat_into_ns" => vec![
            ("path".to_string(), arg(0)),
            ("namespace".to_string(), arg(1)),
        ],
        // doc!'s direct receipt is the interior `input` echo; its ledger
        // row stays flat (target, doc) like consult's.
        "doc" => vec![
            ("target".to_string(), arg(0)),
            ("doc".to_string(), arg(1)),
        ],
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
        // DESCRIPTOR's declared `receipt_echoes` (review otolxyzl::qmqwqlms
        // P2: the per-name match was a second handwritten vocabulary — a
        // new directive now supplies its ledger representation
        // inductively). Optional echoes absent from the call are present
        // with NULL, per the §8 table notes. (mount_tree!'s created
        // sub-namespace enumeration still rides only the SURFACE receipt —
        // threading post-execution results into this pure builder stays
        // deferred, REPORT-SCHEMA-MOUNT-BC.)
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
            // (Phase 6 slice 2): core + `input` echo + `returned` payload.
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

        // EFFECT-ALGEBRA §3: (success, operation,
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
/// namespaces (Phase 8, DIRECTIVE-CONVERGENCE-PLAN): adds another source
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
        // execute_consult_mode (review qmqwqlms round 2, P1) — every
        // route (this entity, the embedded liminal arm, the reconsult
        // pass) receives it inductively.
        let _count = execute_consult_mode(
            system,
            &file_path,
            &namespace,
            None,
            ConsultMode::Concat,
        )?;

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

/// THE ONE LIMINAL BINDER (Phase 9): execute an embedded session
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
        bound.push(DomainExpression::Literal {
            value: LiteralValue::String(value),
            alias: None,
        });
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
/// How a consultation enters an existing namespace (Phase 8, the
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
    execute_consult_mode(system, file_path, namespace, consulting_ns, ConsultMode::Fresh)
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
            // (review zmvnywzu, P2: a nested drain would empty the OUTER
            // program's journal too, which is only safe while every nested
            // failure provably aborts the enclosing program; draining at
            // the boundary needs no such invariant). This includes a body
            // that succeeded but whose lexical state could not be restored:
            // that is still an aborted program, never a partial commit.
            system.rollback_liminal_external_effects();
        }
        let closed = system.end_liminal_program(commit);
        if result.is_ok() {
            restore_result?;
            closed?;
        } else if let Err(close_error) = closed {
            // The abort's real error must not be masked, but a savepoint
            // that failed to close is a session-health event — say so.
            log::warn!(
                "liminal program abort: closing the catalog savepoint failed ({close_error}); \
                 the bootstrap catalog may be in an open transaction until session restart"
            );
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

    // THE CONCAT CLASS INVARIANT (review qmqwqlms round 2, P1: the
    // embedded arm bypassed the entity's check — the guard lives in the
    // SHARED implementation now, so every route receives it inductively):
    // concat extends an EXISTING file-consulted library namespace only.
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

    // THE LIFECYCLE REFUSAL (Phase 8): ordinary consult! creates ONE
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

    // Pre-process: extract and execute embedded directives (consult!, mount!, enlist!, etc.)
    // Parse errors here get the same consult!() wrapper (and error class) the
    // cleaned-source parse has always used; categorized validation errors
    // (eligibility refusals, R-rule badges) pass through unwrapped — badge
    // hygiene, same discipline as consult_body's parse_ddl_file wrapping.
    let (cleaned_source, directives) = extract_embedded_directives(&source).map_err(|e| match e {
        DelightQLError::ParseError { .. } => wrap_consult_parse_error(e, file_path),
        other => other,
    })?;
    run_liminal_program(
        system,
        crate::system::LiminalProgramKind::Consult,
        |system, saved| {
            consult_body(
                system,
                directives,
                &cleaned_source,
                file_path,
                namespace,
                &saved.enlisted,
                &saved.aliases,
                mode,
            )
        },
    )
}

/// The abortable middle of `execute_consult`: executes embedded directives,
/// parses/stores the DDL, and (on success) records + restores enlist/alias
/// state. Every early `Err` return here is caught by `execute_consult`,
/// which restores the caller's saved state.
fn consult_body(
    system: &mut crate::system::DelightQLSystem,
    directives: Vec<EmbeddedDirective>,
    cleaned_source: &str,
    file_path: &str,
    namespace: &str,
    saved_enlisted: &[(i32, i32)],
    saved_aliases: &[(String, i32)],
    mode: ConsultMode,
) -> Result<usize> {
    let prepared = execute_liminal_directives(
        system,
        &directives,
        namespace,
        LiminalDirectiveMode::Fresh,
    )?;
    let liminal_receipts = prepared.receipts;
    let deferred_exposes = prepared.deferred_exposes;
    let deferred_docs = prepared.deferred_docs;

    // Parse the cleaned source as DDL. Categorized validation errors (the
    // effect-algebra refusals: liminal eligibility, R-rule badges) pass
    // through UNWRAPPED — badge hygiene (REPORT-2.1 note 3): a builder
    // refusal must not be re-badged as a parse error.
    let mut ddl = parse_ddl_file(&cleaned_source)
        .map_err(|e| wrap_consult_parse_error(e, file_path))?;

    // Guard: reject files that are valid DQL queries misclassified as DDL.
    // The DDL grammar shares `:` with CTE syntax, so a DQL query file with
    // CTEs can produce spurious "definitions". Check: if the cleaned source
    // parses cleanly as DQL AND produces only a single query (not multiple
    // independent facts), it's a query file, not DDL.
    if !cleaned_source.trim().is_empty() {
        if let Ok(dql_tree) = parse(&cleaned_source) {
            let dql_cst = crate::pipeline::cst::CstTree::new(&dql_tree, &cleaned_source);
            let query_count = dql_cst
                .root()
                .children()
                .filter(|c| c.kind() == "query")
                .count();
            // A single DQL query with CTEs/CFEs is definitely not DDL.
            // Multiple top-level expressions could be facts (valid DDL).
            if query_count == 1 {
                return Err(DelightQLError::database_error(
                    format!(
                        "consult!() failed: '{}' is a DQL query file, not a DDL file. \
                         consult!() expects definitions (:-), tables (:=), or functions — \
                         not queries. Use run!() to execute query files.",
                        file_path
                    ),
                    "Not a DDL file",
                ));
            }
        }
    }

    // Guard: file must contain DDL definitions, not bare queries
    if ddl.definitions.is_empty() && ddl.inline_ddl_blocks.is_empty() {
        return Err(DelightQLError::database_error(
            format!(
                "consult!() failed: '{}' contains no DDL definitions. \
                 consult!() expects a file with rules (:-), tables (:=), \
                 or function definitions — not queries.",
                file_path
            ),
            "Not a DDL file",
        ));
    }
    if !ddl.query_statements.is_empty() {
        return Err(DelightQLError::database_error(
            format!(
                "consult!() failed: '{}' contains query statements (?-). \
                 consult!() expects a pure DDL file with only definitions.",
                file_path
            ),
            "Not a DDL file",
        ));
    }

    // Extract inline DDL blocks before consuming ddl in consult_file
    let inline_ddl_blocks = std::mem::take(&mut ddl.inline_ddl_blocks);

    // ONE ORCHESTRATION BOUNDARY (review otolxyzl::qmqwqlms P1): the
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
    // The registration result propagates IMMEDIATELY (the review's
    // inversion: inline blocks and exposes used to run even after a
    // failed registration).
    let definitions_loaded = system
        .consult_file(file_path, namespace, ddl, &liminal_receipts, Some(&post))
        .map(|cr| cr.definitions_loaded)?;

    // Nested consultations share the outer program savepoint. There is no
    // inverse walk here: any block failure bubbles to the one runner, which
    // restores the complete catalog and the typed external journal.
    for block in &inline_ddl_blocks {
        let child_ns = match &block.namespace {
            Some(suffix) => format!("{}::{}", namespace, suffix),
            None => namespace.to_string(),
        };
        crate::pipeline::sequential::process_inline_ddl_block(&block.body, &child_ns, system)
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

pub(crate) use crate::pipeline::asts::effects::RENAMED_DIRECTIVES as RENAMED_PSEUDO_PREDICATES;

/// Wrap a parse failure from a consulted file in the consult!() context
/// (a `database_error`, so its class is `error://runtime` like every other
/// consult refusal), while letting categorized validation errors — the
/// effect-algebra refusals such as liminal eligibility and R-rule badges —
/// pass through UNWRAPPED (badge hygiene, REPORT-2.1 note 3). Used by both
/// the extraction parse (Phase 1A complete-form segmentation) and the
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
    DelightQLError::database_error(
        format!("consult!() failed to parse '{}': {}", file_path, e),
        "Parse error",
    )
}

/// A recognized liminal statement. The typed shape lives with the effect
/// AST family (EFFECT-ALGEBRA §8): the extraction layer IS the liminal
/// loader, so its record IS the liminal-directive node.
pub(crate) use crate::pipeline::asts::effects::LiminalDirective as EmbeddedDirective;

/// Extract embedded pseudo-predicate directives from DDL source text.
/// Returns (cleaned_source, directives). Errors on unknown !-suffixed names.
///
/// COMPLETE-FORM BOUNDARY (DIRECTIVE-CONVERGENCE-PLAN Phase 1A): the
/// production DDL grammar segments the file into whole forms, replacing the
/// previous physical-line scan. A rule head ending in `:-` owns its body
/// across newlines, blank lines, and comments — a body line like
/// `union_them!(*)` inside a multi-line `main!` clause is part of that
/// rule's definition node and can never be reclassified as a liminal
/// directive (pinned by effects/main--28/29/30). An incomplete head
/// (`main!(*) :-` with no body) is a parse refusal from `parse_ddl`, never
/// an installed empty rule. Exactly the grammar's top-level
/// `liminal_directive` nodes are candidates; of those, session directives
/// are lifted and every other name refuses with the §8 eligibility message.
pub(crate) fn extract_embedded_directives(
    source: &str,
) -> Result<(String, Vec<EmbeddedDirective>)> {
    // Empty input has no forms to segment; preserve the old contract
    // without invoking the grammar (repeat1 cannot parse emptiness).
    if source.trim().is_empty() {
        return Ok((source.to_string(), Vec::new()));
    }

    let tree = crate::pipeline::parser::parse_ddl(source)?;
    let cst = crate::pipeline::cst::CstTree::new(&tree, source);

    let mut directives = Vec::new();
    let mut cleaned: Vec<u8> = source.as_bytes().to_vec();

    for child in cst.root().children() {
        if child.kind() != "liminal_directive" {
            continue;
        }
        let relation_statement = child.find_child("liminal_relation_statement");
        let call = child
            .find_child("pseudo_predicate_call")
            .or(relation_statement)
            .ok_or_else(|| {
                DelightQLError::parse_error(
                    "liminal_directive node without a recognized call child",
                )
            })?;
        let name = call.field_text("name").ok_or_else(|| {
            DelightQLError::parse_error("liminal directive without a name")
        })?;

        // Check for renamed pseudo-predicates and give helpful error
        if let Some((_, new_name)) = RENAMED_PSEUDO_PREDICATES
            .iter()
            .find(|(old, _)| *old == name)
        {
            return Err(DelightQLError::database_error(
                format!(
                    "{}!() has been renamed to {}!(). Please update your code.",
                    name, new_name
                ),
                "Renamed directive",
            ));
        }

        // Liminal eligibility (EFFECT-ALGEBRA §8): only session directives.
        if !crate::pipeline::asts::effects::is_liminal_eligible(&name) {
            return Err(DelightQLError::validation_error_categorized(
                crate::pipeline::asts::effects::LIMINAL_NOT_ELIGIBLE_BADGE,
                crate::pipeline::asts::effects::liminal_not_eligible_message(&name),
                "not liminal-eligible",
            ));
        }

        // A relation-argument statement (insert!(audit_log(*))-shaped) can
        // never execute liminally even under a session name: session
        // directives take string/path arguments, not relations.
        if call.kind() == "liminal_relation_statement" {
            return Err(DelightQLError::validation_error_categorized(
                crate::pipeline::asts::effects::LIMINAL_NOT_ELIGIBLE_BADGE,
                format!(
                    "a liminal '{}!' statement takes string or namespace-path \
                     arguments, not relations",
                    name
                ),
                "not liminal-eligible",
            ));
        }

        // A liminal statement is exactly `name!(args)`. The grammar also
        // permits aliases and interior continuations on the shared
        // pseudo_predicate_call shape; neither has liminal meaning.
        if call.find_child("table_alias").is_some() {
            return Err(DelightQLError::validation_error_categorized(
                crate::pipeline::asts::effects::LIMINAL_NOT_ELIGIBLE_BADGE,
                format!(
                    "a liminal '{}!' statement does not take an alias — \
                     it is a session directive, not a relation",
                    name
                ),
                "not liminal-eligible",
            ));
        }
        if call.field("continuation").is_some() {
            return Err(DelightQLError::validation_error_categorized(
                crate::pipeline::asts::effects::LIMINAL_NOT_ELIGIBLE_BADGE,
                format!(
                    "a liminal '{}!' statement does not take a relational \
                     continuation — it is a session directive, not a relation",
                    name
                ),
                "not liminal-eligible",
            ));
        }

        // Typed argument extraction from the CST (same node vocabulary as
        // the query builder): string literals are unquoted, bare
        // ::-qualified namespace paths keep their text.
        let mut args = Vec::new();
        if let Some(args_list) = call.field("arguments") {
            for arg in args_list.children() {
                match arg.kind() {
                    "domain_expression" | "namespace_path" => {
                        // Phase 9: liminal string arguments decode through
                        // the SAME literal decoder the query builder uses —
                        // triple-quoted (\"\"\"…\"\"\") and b64:"…" forms
                        // included; the old hand-unquote knew neither.
                        let text = arg.text().trim();
                        let value = crate::pipeline::builder_v2::expressions::literals::decode_string_literal_text(text)
                            .unwrap_or_else(|| text.to_string());
                        if !value.is_empty() {
                            args.push(value);
                        }
                    }
                    _ => {}
                }
            }
        }

        directives.push(EmbeddedDirective {
            name: name.to_string(),
            args,
        });

        // Blank the directive's byte range (spaces, newlines preserved) so
        // downstream parses of the cleaned source keep their line numbers.
        let range = child.raw_node().byte_range();
        for b in &mut cleaned[range] {
            if *b != b'\n' {
                *b = b' ';
            }
        }
    }

    let cleaned = String::from_utf8(cleaned)
        .expect("space-blanking byte ranges preserves UTF-8 validity");
    Ok((cleaned, directives))
}

/// Shared DDL-source front end for the loaders that do NOT execute embedded
/// directives — autoload (`ensure_stdlib_loaded`), `sys::meta`, and inline
/// `(~~ddl ~~)` blocks (DDL-LOADING-PATHS.md Tier 1). Routing all three
/// through this makes a `.dql` parse identically however it is loaded:
/// same whitespace handling as `consult!` (the trailing-newline diff goes
/// away), and embedded directives become a LOUD error instead of a silent
/// misparse. `context` names the caller for the error message.
///
/// Tier 2 will let these paths actually execute directives; until then,
/// refusing them here converges the behavior and closes the silent trap.
pub(crate) fn parse_ddl_source_no_directives(
    source: &str,
    context: &str,
) -> Result<crate::pipeline::parser::DDLFile> {
    let (cleaned_source, directives) = extract_embedded_directives(source).map_err(|e| match e {
        DelightQLError::ParseError { .. } => DelightQLError::database_error(
            format!("{context}: failed to parse DDL: {e}"),
            "Parse error",
        ),
        other => other,
    })?;
    if !directives.is_empty() {
        let names = directives
            .iter()
            .map(|d| format!("{}!", d.name))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(DelightQLError::database_error(
            format!(
                "embedded directives ({names}) are not supported in {context} — only \
                 consult!()/reconsult!() files execute them today \
                 (DDL-LOADING-PATHS.md Tier 2)"
            ),
            "Unsupported directive",
        ));
    }
    parse_ddl_file(&cleaned_source).map_err(|e| {
        DelightQLError::database_error(
            format!("{context}: failed to parse DDL: {e}"),
            "Parse error",
        )
    })
}

/// Extract a string literal value from a DomainExpression
pub(super) fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Literal {
            value: LiteralValue::String(s),
            ..
        } => Ok(s.clone()),
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
        // Trailing newline (the raw-vs-clean byte diff) must not matter, and
        // both definitions must survive — the shape that started this thread.
        let src = "sm:(v) :- _:(v @ \"a\" -> \"X\"; _ -> \"Y\")\n\
                   myview(*) :- _(z @ 1) |> (z)\n";
        let ddl = parse_ddl_source_no_directives(src, "test")
            .expect("clean DDL source should parse");
        assert_eq!(ddl.definitions.len(), 2);
    }

    #[test]
    fn shared_front_end_refuses_embedded_directives_loudly() {
        // A directive in a non-executing context is a LOUD error now, not a
        // silent misparse (DDL-LOADING-PATHS.md Tier 1). Tier 2 will execute.
        let src = "consult!(\"other.dql\", \"lib::x\")\nmyview(*) :- _(z @ 1) |> (z)";
        let err = parse_ddl_source_no_directives(src, "autoload module 'sys::demo'")
            .expect_err("embedded directive must be refused");
        let msg = err.to_string();
        assert!(msg.contains("embedded directives"), "{msg}");
        assert!(msg.contains("sys::demo"), "context should name the caller: {msg}");
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

    /// A liminal doc! (session directive, R9-exempt — EFFECT-ALGEBRA §8)
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
            "doc!(\"main\", \"documented at load\")\n\n\
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
                "consult!(\"{lib}\", \"leaklib\")\n\
                 enlist!(\"leaklib\")\n\
                 alias!(\"leaklib\", \"leak_alias\")\n\
                 consult!(\"{missing}\", \"gone\")\n\
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
            err.to_string().contains("already holds an entity named 'foo'"),
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
mod directive_line_tests {
    //! Extraction-boundary pins, re-expressed against the grammar-driven
    //! complete-form extraction (DIRECTIVE-CONVERGENCE-PLAN Phase 1A). The
    //! behaviors originally pinned against the line scanner still hold:
    //! trailing comments, `//` inside string arguments, non-session refusal,
    //! and clause bodies flowing to the parser — plus the new complete-form
    //! guarantees for multi-line rules and incomplete heads.

    use super::*;

    const VIEW: &str = "myview(*) :- _(z @ 1) |> (z)\n";

    /// A trailing `//` comment must not un-recognize a liminal directive
    /// — comments are grammar extras.
    #[test]
    fn directive_with_trailing_comment_is_recognized() {
        let src = format!("consult!(\"lib.dql\", \"lib\")   // load the library\n{VIEW}");
        let (cleaned, directives) =
            extract_embedded_directives(&src).expect("extraction must succeed");
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].name, "consult");
        assert_eq!(directives[0].args, vec!["lib.dql", "lib"]);
        assert!(!cleaned.contains("consult!"), "{cleaned}");
    }

    /// `//` inside a quoted string argument (a URL/path) is NOT a comment.
    #[test]
    fn double_slash_inside_string_argument_is_not_a_comment() {
        let src = format!(
            "mount!(\"http://host//share/db.sqlite\", \"remote\") // attach\n{VIEW}"
        );
        let (_, directives) =
            extract_embedded_directives(&src).expect("extraction must succeed");
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].name, "mount");
        assert_eq!(
            directives[0].args,
            vec!["http://host//share/db.sqlite", "remote"]
        );
    }

    /// A whole-statement directive with a non-session name must error with
    /// the §8 eligibility refusal — unknown names and known non-session
    /// names alike (pinned red-first by the effects ball:
    /// liminal--41_dml_not_eligible, liminal--42_run_not_eligible).
    #[test]
    fn malformed_directive_line_still_errors_clearly() {
        let src = format!("frobnicate!(\"x\")\n{VIEW}");
        let err = extract_embedded_directives(&src)
            .expect_err("non-session directive statement must error");
        assert!(
            err.to_string()
                .contains("only session directives are liminal-eligible"),
            "{err}"
        );
    }

    /// An effect-rule CLAUSE flows to the parser as a definition — never
    /// extraction, never an error (2.2 narrowing, preserved by Phase 1A).
    #[test]
    fn effect_rule_clause_line_flows_to_the_parser() {
        let src = "touch!(*) :- _(msg @ \"touched\") |> insert!(audit_log(*))(*)\n";
        let (cleaned, directives) =
            extract_embedded_directives(src).expect("clause must not error at extraction");
        assert!(directives.is_empty(), "effect-rule clause must flow to the parser");
        assert!(cleaned.contains("touch!"), "clause text must survive: {cleaned}");
    }

    /// End-to-end through the extraction: a commented directive line is
    /// extracted as a directive AND blanked from the cleaned source.
    #[test]
    fn extract_strips_commented_directive_line_from_cleaned_source() {
        let src = "consult!(\"lib.dql\", \"lib\") // load\nmyview(*) :- _(z @ 1) |> (z)\n";
        let (cleaned, directives) =
            extract_embedded_directives(src).expect("extraction must succeed");
        assert_eq!(directives.len(), 1, "directive must be extracted");
        assert_eq!(directives[0].name, "consult");
        assert!(
            !cleaned.contains("consult!"),
            "directive must be removed from cleaned source: {cleaned}"
        );
        assert!(
            cleaned.contains("myview"),
            "definitions must survive cleaning: {cleaned}"
        );
    }

    /// Phase 1A acceptance (effects/main--28): a rule head on one physical
    /// line with its body on the next belongs to ONE definition. The body
    /// demand must not be reclassified as a liminal directive.
    #[test]
    fn multi_line_rule_body_is_not_liminal() {
        let src = "union_them!(*) :- _(msg @ \"ran\") |> insert!(audit_log(*))(*)\n\
                   main!(*) :-\n    union_them!(*)\n";
        let (cleaned, directives) =
            extract_embedded_directives(src).expect("multi-line rule must extract cleanly");
        assert!(
            directives.is_empty(),
            "rule-body demand must not become a liminal directive: {directives:?}"
        );
        assert!(cleaned.contains("union_them!(*)"));
    }

    /// Phase 1A acceptance (effects/main--29): comments and blank lines
    /// between head and body do not terminate the rule.
    #[test]
    fn comments_between_head_and_body_do_not_terminate_the_rule() {
        let src = "union_them!(*) :- _(msg @ \"ran\") |> insert!(audit_log(*))(*)\n\
                   main!(*) :-\n\n// the demand follows\n\n    union_them!(*)\n";
        let (_, directives) =
            extract_embedded_directives(src).expect("comments inside a rule must extract cleanly");
        assert!(directives.is_empty(), "{directives:?}");
    }

    /// Phase 1A acceptance: an incomplete head is refused as a parse error,
    /// never installed as an empty rule and never a liminal reclassification.
    #[test]
    fn incomplete_head_is_refused() {
        let src = "main!(*) :-\n";
        let err = extract_embedded_directives(src)
            .expect_err("incomplete rule head must be refused");
        let msg = err.to_string();
        assert!(msg.contains("parse error") || msg.contains("Parse error"), "{msg}");
    }

    /// A directive statement may now span physical lines: the form, not the
    /// line, is the boundary.
    #[test]
    fn multi_line_directive_arguments_are_extracted() {
        let src = format!("consult!(\"lib.dql\",\n         \"lib\")\n{VIEW}");
        let (cleaned, directives) =
            extract_embedded_directives(&src).expect("multi-line directive must extract");
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].name, "consult");
        assert_eq!(directives[0].args, vec!["lib.dql", "lib"]);
        assert!(!cleaned.contains("consult!"), "{cleaned}");
    }

    /// Blanking preserves line structure so downstream parse errors keep
    /// their positions.
    #[test]
    fn cleaned_source_preserves_line_numbers() {
        let src = format!("enlist!(\"std::string\")\n{VIEW}");
        let (cleaned, directives) =
            extract_embedded_directives(&src).expect("extraction must succeed");
        assert_eq!(directives.len(), 1);
        assert_eq!(
            cleaned.lines().count(),
            src.lines().count(),
            "blanking must not change the line count"
        );
    }
}

#[cfg(test)]
mod liminal_ledger_tests {
    //! THE LIMINAL RELATION's persistence pins (EFFECT-ALGEBRA §8, Epic 5).
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

    /// The receipt schema follows the ruled §8 table: `operation` carries the
    /// name as written with `!`; echoes are named per the table, present-with-
    /// NULL for the optional forms, glob-join suffixed for expose!'s variadics.
    #[test]
    fn liminal_receipt_columns_follow_the_ruled_table() {
        let s = |v: &str| v.to_string();
        let cases: Vec<(&str, Vec<String>, Vec<(&str, Option<&str>)>)> = vec![
            ("consult", vec![s("a.dql"), s("ns")], vec![("path", Some("a.dql")), ("namespace", Some("ns"))]),
            ("mount", vec![s("db.sqlite"), s("ns")], vec![("path", Some("db.sqlite")), ("namespace", Some("ns"))]),
            // mount_new! takes the mount! row (EFFECT-ALGEBRA §6, §8 table).
            ("mount_new", vec![s("fresh.db"), s("ns")], vec![("path", Some("fresh.db")), ("namespace", Some("ns"))]),
            ("reconsult", vec![s("ns")], vec![("namespace", Some("ns")), ("path", None)]),
            ("reconsult", vec![s("ns"), s("b.dql")], vec![("namespace", Some("ns")), ("path", Some("b.dql"))]),
            ("unconsult", vec![s("ns")], vec![("namespace", Some("ns"))]),
            ("unmount", vec![s("ns")], vec![("namespace", Some("ns"))]),
            ("refresh", vec![s("ns")], vec![("namespace", Some("ns"))]),
            ("delist", vec![s("ns")], vec![("namespace", Some("ns"))]),
            ("ground", vec![s("d"), s("l"), s("g")], vec![("data_namespace", Some("d")), ("lib_namespace", Some("l")), ("namespace", Some("g"))]),
            ("enlist", vec![s("ns")], vec![("namespace", Some("ns")), ("into", None)]),
            ("alias", vec![s("ns"), s("st")], vec![("namespace", Some("ns")), ("shorthand", Some("st"))]),
            ("expose", vec![s("a"), s("b"), s("c")], vec![("namespace", Some("a")), ("namespace_2", Some("b")), ("namespace_3", Some("c"))]),
            ("doc", vec![s("main"), s("the doc")], vec![("target", Some("main")), ("doc", Some("the doc"))]),
        ];
        for (name, args, expected) in cases {
            let r = liminal_receipt_for(name, &args);
            assert_eq!(r.operation, format!("{name}!"), "operation echoes the name as written");
            let got: Vec<(&str, Option<&str>)> = r
                .echoes
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_deref()))
                .collect();
            assert_eq!(got, expected, "{name}!'s echo columns must follow the §8 table");
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
            "enlist!(\"main\")\n\
             doc!(\"r\", \"documented at load\")\n\
             alias!(\"main\", \"m0\")\n\n\
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
            "enlist!(\"main\")\nalias!(\"main\", \"m1\")\n\nr(*) :- _(x @ 1)\n",
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
    /// existence is the success signal (§8). Abort road: the file's second
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
                "enlist!(\"main\")\nconsult!(\"{}\", \"sub\")\n\nr(*) :- _(x @ 1)\n",
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
            "enlist!(\"main\")\n\n\
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
    /// describes THE load, not the history of loads (§8).
    #[test]
    fn liminal_ledger_reconsult_replaces_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path_a = write_file(
            &dir,
            "a.dql",
            "enlist!(\"main\")\nalias!(\"main\", \"m2\")\n\nr(*) :- _(x @ 1)\n",
        );
        let path_b = write_file(&dir, "b.dql", "enlist!(\"main\")\n\nr(*) :- _(x @ 2)\n");
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
            "enlist!(\"main\")\n\nr(*) :- _(x @ 1)\n",
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

    /// A namespace created by other means has an EMPTY liminal (§8): `main`
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
