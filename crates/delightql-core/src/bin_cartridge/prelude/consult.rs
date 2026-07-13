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
        // mount_new! echoes the SAME row as mount! (EFFECT-ALGEBRA §6, §8
        // table: `path, namespace`).
        "consult" | "mount" | "mount_new" => vec![
            ("path".to_string(), arg(0)),
            ("namespace".to_string(), arg(1)),
        ],
        // mount_tree! echoes its two written parameters; the JSON array of
        // CREATED sub-namespaces (R-S3) rides the SURFACE receipt (the
        // executor knows the enumeration result). Threading that
        // post-execution list into the pure ledger builder is deferred —
        // see REPORT-SCHEMA-MOUNT-BC.
        "mount_tree" => vec![
            ("path".to_string(), arg(0)),
            ("namespace".to_string(), arg(1)),
        ],
        "reconsult" => vec![
            ("namespace".to_string(), arg(0)),
            // NULL when re-reading the same file (§8 table note).
            ("path".to_string(), arg(1)),
        ],
        "unconsult" | "unmount" | "refresh" | "delist" => {
            vec![("namespace".to_string(), arg(0))]
        }
        "ground" => vec![
            ("data_namespace".to_string(), arg(0)),
            ("lib_namespace".to_string(), arg(1)),
            ("namespace".to_string(), arg(2)),
        ],
        "enlist" => vec![
            ("namespace".to_string(), arg(0)),
            // NULL for the plain form (§8 table note).
            ("into".to_string(), arg(1)),
        ],
        "alias" => vec![
            ("namespace".to_string(), arg(0)),
            ("shorthand".to_string(), arg(1)),
        ],
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
        "doc" => vec![
            ("target".to_string(), arg(0)),
            ("doc".to_string(), arg(1)),
        ],
        // consult_tree!'s echo columns land with its liminal arm
        // (EFFECT-ALGEBRA §12 item 2, explicitly deferred); any other name is
        // refused by the executor before a receipt could be recorded. Echo
        // the raw arguments positionally so nothing is silently dropped.
        _ => args
            .iter()
            .enumerate()
            .map(|(i, a)| (format!("arg_{}", i + 1), Some(a.clone())))
            .collect(),
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
            output_schema: OutputSchema::Relation(vec![("ns".to_string(), "String".to_string())]),
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

        Ok(EntityResult::Relation(super::directive_result(
            &namespace, alias,
        )))
    }
}

/// Execute a consult operation: read file, process embedded directives,
/// parse as DDL, and store definitions.
///
/// `consulting_ns` is the namespace of the DDL that triggered this consult.
/// When present, `.::` and `::` prefixes in embedded directives are resolved
/// relative to `namespace` (the target namespace for this file).
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
    let (cleaned_source, directives) = extract_embedded_directives(&source)?;
    let saved_enlisted = system.save_enlisted_state()?;
    let saved_aliases = system.save_alias_state()?;

    // ALL exit paths must restore the caller's enlist/alias state
    // (bugs/liminal-abort-state-leak): a directive failure mid-file used to
    // return early past the restore, leaking partial mutations into the
    // session while a fully successful consult scoped them. The success
    // path records new enlists/aliases as namespace-local and restores
    // inside `consult_body`; the failure path restores here (best-effort,
    // so restore trouble cannot mask the abort error). Pinned by
    // `liminal_abort_restores_enlist_and_alias_state`.
    let result = consult_body(
        system,
        directives,
        &cleaned_source,
        file_path,
        namespace,
        &saved_enlisted,
        &saved_aliases,
    );
    if result.is_err() {
        let _ = system.restore_enlisted_state(&saved_enlisted);
        let _ = system.restore_alias_state(&saved_aliases);
    }
    result
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
) -> Result<usize> {
    // Deferred expose directives: expose! must run after consult_file creates
    // this DDL's namespace, so we validate args now but execute later.
    let mut deferred_exposes: Vec<Vec<String>> = Vec::new();

    // Deferred doc! directives: a liminal doc! (session directive, R9-exempt
    // — EFFECT-ALGEBRA §8) documents an entity of THIS file, which exists
    // only after consult_file registers the rules. Validate arity now,
    // execute after registration (the expose! precedent).
    let mut deferred_docs: Vec<(String, String)> = Vec::new();

    // THE LIMINAL RELATION (EFFECT-ALGEBRA §8): collect one receipt per
    // liminal directive, in this single file-order pass — so a deferred
    // doc!'s receipt keeps its FILE position in the ledger, not its
    // execution time (pinned by `liminal_ledger_doc_keeps_file_position`).
    // Receipts are persisted by consult_file inside the consult transaction;
    // any abort on this road (a failing directive below, a parse refusal, a
    // registration refusal) leaves no namespace and no ledger (pinned by
    // `liminal_ledger_abort_leaves_no_ledger`).
    let mut liminal_receipts: Vec<crate::system::LiminalReceipt> = Vec::new();

    for directive in directives {
        liminal_receipts.push(liminal_receipt_for(&directive.name, &directive.args));
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
                execute_consult(system, &directive.args[0], &resolved_ns, Some(namespace))?;
            }
            "mount" => {
                if directive.args.len() != 2 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "mount!() in DDL expects 2 arguments, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[1], namespace)?;
                system.mount_database(&directive.args[0], &resolved_ns)?;
            }
            "mount_new" => {
                if directive.args.len() != 2 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "mount_new!() in DDL expects 2 arguments, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[1], namespace)?;
                system.mount_new_database(&directive.args[0], &resolved_ns)?;
            }
            "mount_tree" => {
                if directive.args.len() != 2 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "mount_tree!() in DDL expects 2 arguments, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[1], namespace)?;
                system.mount_database_tree(&directive.args[0], &resolved_ns)?;
            }
            "enlist" => {
                if directive.args.len() != 1 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "enlist!() in DDL expects 1 argument, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[0], namespace)?;
                system.enlist_namespace(&resolved_ns)?;
            }
            "delist" => {
                if directive.args.len() != 1 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "delist!() in DDL expects 1 argument, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[0], namespace)?;
                system.delist_namespace(&resolved_ns)?;
            }
            "alias" => {
                if directive.args.len() != 2 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "alias!() in DDL expects 2 arguments (namespace, shorthand), got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[0], namespace)?;
                system.register_namespace_alias(&directive.args[1], &resolved_ns)?;
            }
            "unmount" => {
                if directive.args.len() != 1 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "unmount!() in DDL expects 1 argument, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[0], namespace)?;
                system.unmount_database(&resolved_ns)?;
            }
            "unconsult" => {
                if directive.args.len() != 1 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "unconsult!() in DDL expects 1 argument, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[0], namespace)?;
                system.unconsult_namespace(&resolved_ns)?;
            }
            "ground" => {
                if directive.args.len() != 3 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "ground!() in DDL expects 3 arguments (data_ns, lib_ns, new_ns), got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let data_ns = resolve_ns_prefix(&directive.args[0], namespace)?;
                let lib_ns = resolve_ns_prefix(&directive.args[1], namespace)?;
                let new_ns = resolve_ns_prefix(&directive.args[2], namespace)?;
                system.ground_namespace(&data_ns, &lib_ns, &new_ns)?;
            }
            "expose" => {
                if directive.args.is_empty() {
                    return Err(DelightQLError::database_error(
                        "expose!() requires at least one namespace argument",
                        "Invalid directive",
                    ));
                }
                // Resolve args now but defer execution until after consult_file
                // creates this DDL's namespace.
                let resolved: Vec<String> = directive
                    .args
                    .iter()
                    .map(|arg| resolve_ns_prefix(arg, namespace))
                    .collect::<Result<Vec<_>>>()?;
                deferred_exposes.push(resolved);
            }
            "refresh" => {
                if directive.args.len() != 1 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "refresh!() in DDL expects 1 argument, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[0], namespace)?;
                system.refresh_namespace(&resolved_ns)?;
            }
            "reconsult" => {
                if directive.args.is_empty() || directive.args.len() > 2 {
                    return Err(DelightQLError::database_error(
                        format!(
                            "reconsult!() in DDL expects 1 or 2 arguments, got {}",
                            directive.args.len()
                        ),
                        "Invalid directive",
                    ));
                }
                let resolved_ns = resolve_ns_prefix(&directive.args[0], namespace)?;
                let new_file = if directive.args.len() == 2 {
                    Some(directive.args[1].as_str())
                } else {
                    None
                };
                system.reconsult_namespace(&resolved_ns, new_file)?;
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
                deferred_docs.push((directive.args[0].clone(), directive.args[1].clone()));
            }
            other => {
                return Err(DelightQLError::database_error(
                    format!(
                        "pseudo-predicate {}!() is not supported in DDL files",
                        other
                    ),
                    "Unsupported directive",
                ));
            }
        }
    }

    // Parse the cleaned source as DDL. Categorized validation errors (the
    // effect-algebra refusals: liminal eligibility, R-rule badges) pass
    // through UNWRAPPED — badge hygiene (REPORT-2.1 note 3): a builder
    // refusal must not be re-badged as a parse error.
    let mut ddl = parse_ddl_file(&cleaned_source).map_err(|e| {
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
    })?;

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

    // Store in system
    let result = system
        .consult_file(file_path, namespace, ddl, &liminal_receipts)
        .map(|cr| cr.definitions_loaded);

    // Process inline DDL blocks — each creates a child namespace
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

    // Execute deferred expose directives now that the namespace exists
    for resolved_args in deferred_exposes {
        for resolved_ns in &resolved_args {
            system.expose_namespace(namespace, resolved_ns)?;
        }
    }

    // Execute deferred doc! directives now that the file's entities exist.
    // A liminal doc! target resolves relative to the file's own namespace;
    // an unqualified rule name is tried verbatim, then with the `!` suffix
    // (effect rules store the `!` in the entity name), then as an
    // already-qualified path. Failure ABORTS the load (§8: session
    // directives succeed or abort). Pinned by
    // `liminal_doc_documents_this_files_effect_rule`.
    // (Guarded on the registration result: when consult_file itself refused
    // — e.g. an R-rule validation error — that error must surface, not a
    // doc!-target miss over entities that were never registered.)
    if result.is_ok() {
        for (target, doc) in deferred_docs {
            let candidates = [
                format!("{}.{}", namespace, target),
                format!("{}.{}!", namespace, target),
                target.clone(),
            ];
            let mut last_err = None;
            let mut done = false;
            for candidate in &candidates {
                match system.set_entity_doc(candidate, &doc) {
                    Ok(_) => {
                        done = true;
                        break;
                    }
                    Err(e) => last_err = Some(e),
                }
            }
            if !done {
                return Err(last_err.expect("candidates is non-empty"));
            }
        }
    }

    // Record which namespaces were newly enlisted and which aliases were created
    // by this DDL as namespace-local dependencies.
    // Then restore the caller's state so DDL-internal changes don't leak.
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

    if !new_enlists.is_empty() || !new_aliases.is_empty() {
        if !new_enlists.is_empty() {
            system.record_namespace_local_enlists(namespace, &new_enlists)?;
        }
        if !new_aliases.is_empty() {
            system.record_namespace_local_aliases(namespace, &new_aliases)?;
        }
        system.restore_enlisted_state(&saved_enlisted)?;
        system.restore_alias_state(&saved_aliases)?;
    }

    result
}

const RENAMED_PSEUDO_PREDICATES: &[(&str, &str)] = &[
    ("engage", "enlist"),
    ("part", "delist"),
    ("ground_into", "ground"),
];

/// A recognized liminal statement. The typed shape lives with the effect
/// AST family (EFFECT-ALGEBRA §8): the extraction layer IS the liminal
/// loader, so its record IS the liminal-directive node.
pub(crate) use crate::pipeline::asts::effects::LiminalDirective as EmbeddedDirective;

/// Extract embedded pseudo-predicate directives from DDL source text.
/// Returns (cleaned_source, directives). Errors on unknown !-suffixed names.
pub(crate) fn extract_embedded_directives(
    source: &str,
) -> Result<(String, Vec<EmbeddedDirective>)> {
    let mut cleaned_lines = Vec::new();
    let mut directives = Vec::new();

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") || trimmed.is_empty() {
            cleaned_lines.push(line);
            continue;
        }
        match parse_directive(trimmed)? {
            Some(directive) => directives.push(directive),
            None => cleaned_lines.push(line),
        }
    }

    Ok((cleaned_lines.join("\n"), directives))
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
    let (cleaned_source, directives) = extract_embedded_directives(source)?;
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

/// Strip a trailing `//` comment from a directive line, ignoring `//`
/// inside double-quoted string arguments (paths/URLs like "http://x//y"
/// must survive). Uses the same naive string model as the argument
/// extractor below (quotes toggle, no escape sequences). Pinned by
/// `directive_with_trailing_comment_is_recognized` and
/// `double_slash_inside_string_argument_is_not_a_comment`.
fn strip_trailing_line_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut in_string = false;
    for i in 0..bytes.len() {
        match bytes[i] {
            b'"' => in_string = !in_string,
            b'/' if !in_string && bytes.get(i + 1) == Some(&b'/') => {
                return line[..i].trim_end();
            }
            _ => {}
        }
    }
    line
}

/// Find the byte index of the `)` matching the `(` at `open_pos`, using the
/// extraction layer's naive string model (quotes toggle, no escapes).
/// Returns None if the parens never balance on this line.
fn find_matching_close_paren(line: &str, open_pos: usize) -> Option<usize> {
    let bytes = line.as_bytes();
    let mut depth = 0usize;
    let mut in_string = false;
    for (i, &b) in bytes.iter().enumerate().skip(open_pos) {
        match b {
            b'"' => in_string = !in_string,
            b'(' if !in_string => depth += 1,
            b')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Try to parse a `name!("arg1", "arg2", ...)` directive from a trimmed line.
///
/// NARROWED (IMPLEMENTATION-PLAN §2.2, ruled 2026-07-10): the textual layer
/// owns ONLY the liminal space — lines that are, in their entirety, a single
/// `name!(args)` statement. Effect-rule clauses (`name!(*) :- …`) and
/// expression-position directives (trailing access parens, pipes, …) flow to
/// the real parsers: anything after the matching close paren returns
/// Ok(None).
///
/// Of the whole-line statements, exactly the SESSION directives are lifted
/// (liminal-eligible, EFFECT-ALGEBRA §8); every other directive name — DML,
/// DDL, execution, utility, user effect rules, and unknown names alike —
/// refuses with the eligibility message (pinned red-first by the effects
/// ball: liminal--41_dml_not_eligible, liminal--42_run_not_eligible).
fn parse_directive(line: &str) -> Result<Option<EmbeddedDirective>> {
    // A trailing `//` comment must not silently un-recognize a directive
    // (bugs/directive-trailing-comment): strip it before the
    // whole-line check below.
    let line = strip_trailing_line_comment(line);

    // Look for the name!( pattern
    let Some(bang_pos) = line.find("!(") else {
        return Ok(None);
    };

    let name = &line[..bang_pos];

    // Must be a simple identifier (no spaces, operators, etc.)
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Ok(None);
    }

    // The whole line must be exactly `name!(args)`: find the close paren
    // matching `name!(` and require nothing after it. An effect-rule clause
    // (`touch!(*) :- …`) or an expression line (`run_namespace!(fx)(*)`)
    // has trailing content and flows to the real parsers (2.2 narrowing;
    // pinned by `effect_rule_clause_line_flows_to_the_parser`).
    let Some(close_pos) = find_matching_close_paren(line, bang_pos + 1) else {
        return Ok(None);
    };
    if !line[close_pos + 1..].trim().is_empty() {
        return Ok(None);
    }

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
    if !crate::pipeline::asts::effects::is_liminal_eligible(name) {
        return Err(DelightQLError::validation_error_categorized(
            crate::pipeline::asts::effects::LIMINAL_NOT_ELIGIBLE_BADGE,
            crate::pipeline::asts::effects::liminal_not_eligible_message(name),
            "not liminal-eligible",
        ));
    }

    // Extract the arguments between !( and the matching )
    let inner = &line[bang_pos + 2..close_pos];
    let args: Vec<String> = inner
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    Ok(Some(EmbeddedDirective {
        name: name.to_string(),
        args,
    }))
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
    //! bugs/liminal-abort-state-leak: a consult that aborts mid-file must
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
             main!(*) :- _(msg @ \"x\") |> insert!(audit_log(msg))(*)\n",
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
    //! foo/foo! name collision (IMPLEMENTATION-PLAN §3.0, ruled 2026-07-11).
    //! The two ruled directions are pinned red-first by effects-ball
    //! rules--47/rules--48 (views vs effect rules). This module pins the
    //! §3.0 scope FINDING: colon-functions (`foo:(x)`) register plain-named
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
             foo!(*) :- _(msg @ \"x\") |> insert!(audit_log(msg))(*)\n",
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
            "foo!(*) :- _(msg @ \"x\") |> insert!(audit_log(msg))(*)\n\n\
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
    //! bugs/directive-trailing-comment: a trailing `//` comment must not
    //! silently un-recognize a liminal directive line, and `//` inside a
    //! quoted string argument (a URL/path) is NOT a comment.

    use super::*;

    /// RED before the comment-strip: the ends-with-`)` check failed, the
    /// line fell through to the DDL parser, and died as a garbled parse
    /// error far from the real cause.
    #[test]
    fn directive_with_trailing_comment_is_recognized() {
        let d = parse_directive(r#"consult!("lib.dql", "lib")   // load the library"#)
            .expect("directive line must not error")
            .expect("directive with trailing comment must be recognized");
        assert_eq!(d.name, "consult");
        assert_eq!(d.args, vec!["lib.dql", "lib"]);
    }

    /// `//` inside a quoted string argument (e.g. a URL or path) must
    /// survive comment stripping — with and without a real trailing comment.
    #[test]
    fn double_slash_inside_string_argument_is_not_a_comment() {
        let d = parse_directive(r#"mount!("http://host//share/db.sqlite", "remote")"#)
            .expect("directive line must not error")
            .expect("directive must be recognized");
        assert_eq!(d.name, "mount");
        assert_eq!(d.args, vec!["http://host//share/db.sqlite", "remote"]);

        let d = parse_directive(r#"mount!("http://host//share/db.sqlite", "remote") // attach"#)
            .expect("directive line must not error")
            .expect("directive with trailing comment must be recognized");
        assert_eq!(d.args, vec!["http://host//share/db.sqlite", "remote"]);
    }

    /// A whole-line directive with a non-session name must still error
    /// CLEARLY — including when a trailing comment follows it. Message
    /// updated by the 2.2 extraction narrowing: eligibility is the §8
    /// category, so unknown names and known non-session names share the
    /// liminal-eligibility refusal (was "unknown pseudo-predicate").
    #[test]
    fn malformed_directive_line_still_errors_clearly() {
        let err = parse_directive(r#"frobnicate!("x")"#)
            .expect_err("non-session directive statement must error");
        assert!(
            err.to_string()
                .contains("only session directives are liminal-eligible"),
            "{err}"
        );

        let err = parse_directive(r#"frobnicate!("x") // huh"#)
            .expect_err("non-session directive with trailing comment must error");
        assert!(
            err.to_string()
                .contains("only session directives are liminal-eligible"),
            "{err}"
        );
    }

    /// 2.2 narrowing (IMPLEMENTATION-PLAN §2.2 textual-extraction ruling):
    /// a single-line effect-rule CLAUSE and an expression-position directive
    /// line must flow to the real parsers — Ok(None), not extraction, not an
    /// error. RED before the narrowing: the clause was intercepted as
    /// `unknown pseudo-predicate touch!() in DDL file`.
    #[test]
    fn effect_rule_clause_line_flows_to_the_parser() {
        let clause =
            parse_directive(r#"touch!(*) :- _(msg @ "touched") |> insert!(audit_log(msg))(*)"#)
                .expect("clause line must not error at extraction");
        assert!(clause.is_none(), "effect-rule clause must flow to the parser");

        let expr = parse_directive("run_namespace!(fx)(*)")
            .expect("expression line must not error at extraction");
        assert!(
            expr.is_none(),
            "expression-position directive must flow to the parser"
        );
    }

    /// End-to-end through the scanner: a commented directive line is
    /// extracted as a directive AND removed from the cleaned source.
    #[test]
    fn extract_strips_commented_directive_line_from_cleaned_source() {
        let src = "consult!(\"lib.dql\", \"lib\") // load\nmyview(*) :- _(z @ 1) |> (z)\n";
        let (cleaned, directives) =
            extract_embedded_directives(src).expect("extraction must succeed");
        assert_eq!(directives.len(), 1, "directive must be extracted");
        assert_eq!(directives[0].name, "consult");
        assert!(
            !cleaned.contains("consult!"),
            "directive line must be removed from cleaned source: {cleaned}"
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
             bad(*) :- _(msg @ \"x\") |> insert!(audit_log(msg))(*)\n",
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
