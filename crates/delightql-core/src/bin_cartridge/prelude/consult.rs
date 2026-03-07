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

    // Deferred expose directives: expose! must run after consult_file creates
    // this DDL's namespace, so we validate args now but execute later.
    let mut deferred_exposes: Vec<Vec<String>> = Vec::new();

    for directive in directives {
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

    // Parse the cleaned source as DDL
    let ddl = parse_ddl_file(&cleaned_source).map_err(|e| {
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
    if ddl.definitions.is_empty() {
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

    // Store in system
    let result = system
        .consult_file(file_path, namespace, ddl)
        .map(|cr| cr.definitions_loaded);

    // Execute deferred expose directives now that the namespace exists
    for resolved_args in deferred_exposes {
        for resolved_ns in &resolved_args {
            system.expose_namespace(namespace, resolved_ns)?;
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

const KNOWN_PSEUDO_PREDICATES: &[&str] = &[
    "mount",
    "enlist",
    "delist",
    "run",
    "consult",
    "consult_tree",
    "ground",
    "imprint",
    "alias",
    "unmount",
    "unconsult",
    "refresh",
    "reconsult",
    "expose",
];

const RENAMED_PSEUDO_PREDICATES: &[(&str, &str)] = &[
    ("engage", "enlist"),
    ("part", "delist"),
    ("ground_into", "ground"),
];

pub(crate) struct EmbeddedDirective {
    pub name: String,
    pub args: Vec<String>,
}

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

/// Try to parse a `name!("arg1", "arg2", ...)` directive from a trimmed line.
/// Returns Ok(Some) for known directives, Ok(None) for non-directive lines,
/// and Err for unknown !-suffixed names.
fn parse_directive(line: &str) -> Result<Option<EmbeddedDirective>> {
    // Look for the name!( pattern
    let Some(bang_pos) = line.find("!(") else {
        return Ok(None);
    };

    let name = &line[..bang_pos];

    // Must be a simple identifier (no spaces, operators, etc.)
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Ok(None);
    }

    // Must end with )
    if !line.ends_with(')') {
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

    // Reject unknown pseudo-predicates
    if !KNOWN_PSEUDO_PREDICATES.contains(&name) {
        return Err(DelightQLError::database_error(
            format!("unknown pseudo-predicate {}!() in DDL file", name),
            "Unknown directive",
        ));
    }

    // Extract the arguments between !( and the final )
    let inner = &line[bang_pos + 2..line.len() - 1];
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
