//! Phase 1.X: Effect Executor
//!
//! This phase executes pseudo-predicates (state-mutating relations) and rewrites
//! the AST by replacing them with inline result tables.
//!
//! ## Overview
//!
//! Pseudo-predicates are special relations ending with `!` that:
//! 1. Execute immediately when encountered
//! 2. Mutate system state (open connections, register namespaces, etc.)
//! 3. Return result tables that replace them in the AST
//!
//! ## Supported Pseudo-Predicates (MVP)
//!
//! - `mount!(db_path, namespace)` - Opens a database connection and registers a namespace
//!
//! ## Architecture
//!
//! Phase 1.X hooks between Builder (Phase 1) and Resolver (Phase 2):
//! ```
//! CST → Builder → Effect Executor → CFE Precompiler → Resolver → ...
//!      (Phase 1)   (Phase 1.X)        (Phase 1.5)      (Phase 2)
//! ```
//!
//! The Effect Executor:
//! 1. Traverses the unresolved AST to find pseudo-predicates
//! 2. Executes each pseudo-predicate in order
//! 3. Replaces the pseudo-predicate node with an inline table containing the result
//! 4. Returns the modified AST for subsequent phases
//!
//! ## Error Handling
//!
//! All pseudo-predicate execution errors are fatal - if a pseudo-predicate fails,
//! the entire query fails. This is appropriate because pseudo-predicates represent
//! essential setup operations (like mounting databases).

use crate::bin_cartridge::EffectExecutable;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::Pipeline;
use crate::system::DelightQLSystem;

/// Execute all pseudo-predicates in a query and rewrite the AST
///
/// This is the main entry point for Phase 1.X. It:
/// 1. Detects pseudo-predicates in the query
/// 2. Executes them in order (top-to-bottom, left-to-right)
/// 3. Replaces them with inline result tables
/// 4. Returns the rewritten query
pub fn execute_effects(query: Query, system: &mut DelightQLSystem) -> Result<Query> {
    // For now, we only support relational queries
    match query {
        Query::Relational(expression) => {
            let rewritten_expression = execute_effects_in_expression(expression, &[], system)?;
            Ok(Query::Relational(rewritten_expression))
        }
        Query::WithCtes { ctes, query } => {
            // Rewrite the main query expression, passing CTE bindings for resolution
            let rewritten_query = execute_effects_in_expression(query, &ctes, system)?;
            Ok(Query::WithCtes {
                ctes,
                query: rewritten_query,
            })
        }
        Query::WithCfes { cfes, query } => {
            // Rewrite the inner query recursively
            let rewritten_query = Box::new(execute_effects(*query, system)?);
            Ok(Query::WithCfes {
                cfes,
                query: rewritten_query,
            })
        }
        Query::WithPrecompiledCfes { cfes, query } => {
            // Rewrite the inner query recursively
            let rewritten_query = Box::new(execute_effects(*query, system)?);
            Ok(Query::WithPrecompiledCfes {
                cfes,
                query: rewritten_query,
            })
        }
        // REPL commands don't have pseudo-predicates
        _ => Ok(query),
    }
}

/// Recursively traverse a relational expression and execute pseudo-predicates
#[stacksafe::stacksafe]
fn execute_effects_in_expression(
    expression: RelationalExpression,
    ctes: &[CteBinding],
    system: &mut DelightQLSystem,
) -> Result<RelationalExpression> {
    match expression {
        RelationalExpression::Relation(relation) => {
            let rewritten_relation = execute_effects_in_relation(relation, system)?;
            Ok(RelationalExpression::Relation(rewritten_relation))
        }
        RelationalExpression::SetOperation {
            operator,
            operands,
            correlation,
            cpr_schema,
        } => {
            let rewritten_operands = operands
                .into_iter()
                .map(|operand| execute_effects_in_expression(operand, ctes, system))
                .collect::<Result<Vec<_>>>()?;
            Ok(RelationalExpression::SetOperation {
                operator,
                operands: rewritten_operands,
                correlation,
                cpr_schema,
            })
        }
        RelationalExpression::Pipe(pipe) => {
            let PipeExpression {
                source, operator, ..
            } = (*pipe).into_inner();

            if let UnaryRelationalOperator::DirectiveTerminal { name, arguments } = operator {
                execute_directive_pipe(source, &name, &arguments, ctes, system)
            } else if let UnaryRelationalOperator::HoViewApplication {
                ref function,
                namespace: Some(ref ns),
                ..
            } = operator
            {
                // Check bin registry for namespace-qualified piped invocation
                let ns_strs: Vec<&str> = ns.iter().map(|item| item.name.as_str()).collect();
                if let Some(entity) = system
                    .bin_registry()
                    .lookup_qualified_entity(&ns_strs, function)
                {
                    if let Some(executable) = entity.as_effect_executable() {
                        return execute_bin_entity_pipe(source, executable, ctes, system);
                    }
                }
                // Not a bin entity — regular pipe
                let executed_source = execute_effects_in_expression(source, ctes, system)?;
                Ok(RelationalExpression::Pipe(Box::new(
                    stacksafe::StackSafe::new(PipeExpression {
                        source: executed_source,
                        operator,
                        cpr_schema: PhaseBox::phantom(),
                    }),
                )))
            } else {
                // Regular pipe — recurse into source, preserve operator
                let executed_source = execute_effects_in_expression(source, ctes, system)?;
                Ok(RelationalExpression::Pipe(Box::new(
                    stacksafe::StackSafe::new(PipeExpression {
                        source: executed_source,
                        operator,
                        cpr_schema: PhaseBox::phantom(),
                    }),
                )))
            }
        }
        // Other expression types: recurse into joins, filters, etc.
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => {
            let left = Box::new(execute_effects_in_expression(*left, ctes, system)?);
            let right = Box::new(execute_effects_in_expression(*right, ctes, system)?);
            Ok(RelationalExpression::Join {
                left,
                right,
                join_condition,
                join_type,
                cpr_schema,
            })
        }
        RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            let source = Box::new(execute_effects_in_expression(*source, ctes, system)?);
            Ok(RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            })
        }
        // ErJoinChain and ErTransitiveJoin don't contain pseudo-predicates
        _ => Ok(expression),
    }
}

/// Execute pseudo-predicates in a relation
fn execute_effects_in_relation(
    relation: Relation,
    system: &mut DelightQLSystem,
) -> Result<Relation> {
    match relation {
        // This is the key case: execute the pseudo-predicate!
        Relation::PseudoPredicate {
            name,
            arguments,
            alias,
            ..
        } => execute_pseudo_predicate(&name, &arguments, alias, system),

        // InnerRelation contains a subquery that might have pseudo-predicates
        Relation::InnerRelation {
            pattern,
            alias,
            outer,
            ..
        } => {
            let rewritten_pattern = match pattern {
                InnerRelationPattern::Indeterminate {
                    identifier,
                    subquery,
                } => {
                    let rewritten_subquery =
                        Box::new(execute_effects_in_expression(*subquery, &[], system)?);
                    InnerRelationPattern::Indeterminate {
                        identifier,
                        subquery: rewritten_subquery,
                    }
                }
                // Other patterns are classified later, no need to handle here
                other => other,
            };
            Ok(Relation::InnerRelation {
                pattern: rewritten_pattern,
                alias,
                outer,
                cpr_schema: PhaseBox::phantom(),
            })
        }

        // Check if a Ground relation is a namespace-qualified bin entity
        // (e.g., sys::execution.compile("stage", "source"))
        Relation::Ground {
            ref identifier,
            ref domain_spec,
            ref alias,
            ..
        } if !identifier.namespace_path.is_empty() => {
            let ns_strs: Vec<&str> = identifier
                .namespace_path
                .iter()
                .map(|item| item.name.as_str())
                .collect();
            let entity_opt = system
                .bin_registry()
                .lookup_qualified_entity(&ns_strs, identifier.name.as_str());

            if let Some(entity) = entity_opt {
                if let Some(executable) = entity.as_effect_executable() {
                    let arguments = match domain_spec {
                        DomainSpec::Positional(args) => args.clone(),
                        _ => {
                            return Err(DelightQLError::database_error(
                                format!(
                                    "Bin relation '{}' requires positional arguments",
                                    identifier.name
                                ),
                                "Invalid domain spec for bin relation",
                            ))
                        }
                    };
                    let alias_str = alias.as_ref().map(|s| s.to_string());
                    let result = executable.execute(&arguments, alias_str, system)?;
                    let crate::bin_cartridge::EntityResult::Relation(r) = result;
                    return Ok(r);
                }
            }
            // Not a bin entity — pass through for resolver
            Ok(relation)
        }

        // Check if a TVF is a namespace-qualified bin entity
        // (e.g., sys::execution.compile("sql", "users(*)")(*)  → Relation::TVF)
        Relation::TVF {
            ref function,
            ref arguments,
            ref alias,
            ref namespace,
            ..
        } if namespace.as_ref().map_or(false, |ns| !ns.is_empty()) => {
            let ns = namespace.as_ref().unwrap();
            let ns_strs: Vec<&str> = ns.iter().map(|item| item.name.as_str()).collect();
            if let Some(entity) = system
                .bin_registry()
                .lookup_qualified_entity(&ns_strs, function.as_str())
            {
                if let Some(executable) = entity.as_effect_executable() {
                    let dom_args: Vec<DomainExpression> = arguments
                        .iter()
                        .map(|s| {
                            // TVF argument strings may still have enclosing quotes
                            // (b64 strings are already decoded by the builder).
                            let value = if (s.starts_with('"') && s.ends_with('"'))
                                || (s.starts_with('\'') && s.ends_with('\''))
                            {
                                s[1..s.len() - 1].to_string()
                            } else {
                                s.clone()
                            };
                            DomainExpression::Literal {
                                value: LiteralValue::String(value),
                                alias: None,
                            }
                        })
                        .collect();
                    let alias_str = alias.as_ref().map(|s| s.to_string());
                    let result = executable.execute(&dom_args, alias_str, system)?;
                    let crate::bin_cartridge::EntityResult::Relation(r) = result;
                    return Ok(r);
                }
            }
            // Not a bin entity — pass through for resolver
            Ok(relation)
        }

        // All other relation types don't contain pseudo-predicates
        _ => Ok(relation),
    }
}

/// Execute a directive pipe: source |> terminal!(args)
///
/// 1. Execute the source expression (recursively handles chained pipes)
/// 2. Extract rows from the source (anonymous fast path, or full pipeline)
/// 3. For each row, bind the terminal arguments and execute the terminal directive
/// 4. Combine all results into a single Anonymous relation
fn execute_directive_pipe(
    source: RelationalExpression,
    terminal_name: &str,
    terminal_args: &[DomainExpression],
    ctes: &[CteBinding],
    system: &mut DelightQLSystem,
) -> Result<RelationalExpression> {
    // 1. Execute source (recursively handles chained directive pipes and pseudo-predicates)
    let executed_source = execute_effects_in_expression(source, ctes, system)?;

    // 2. Extract rows — fast path for anonymous, full pipeline for anything else
    let (headers, rows) = extract_rows(executed_source, ctes, system)?;

    // 3. For each row, bind arguments and execute the terminal directive
    let mut all_result_rows = Vec::new();
    let mut result_headers: Option<Vec<DomainExpression>> = None;

    for row_values in &rows {
        let bound_args = bind_directive_args(&headers, row_values, terminal_args)?;
        let result = execute_pseudo_predicate(terminal_name, &bound_args, None, system)?;

        if let Relation::Anonymous {
            column_headers,
            rows: result_rows,
            ..
        } = result
        {
            if result_headers.is_none() {
                result_headers = column_headers;
            }
            all_result_rows.extend(result_rows);
        } else {
            // Non-anonymous results (e.g., Ground relations) — skip collecting rows,
            // the directive executed its side effects successfully
        }
    }

    // 4. Return combined Anonymous relation
    Ok(RelationalExpression::Relation(Relation::Anonymous {
        column_headers: result_headers,
        rows: all_result_rows,
        alias: None,
        outer: false,
        exists_mode: false,
        qua_target: None,
        cpr_schema: PhaseBox::phantom(),
    }))
}

/// Execute a bin entity in a piped context: source |> ns::entity(*)
///
/// 1. Execute the source expression
/// 2. Extract rows (anonymous fast path, or full pipeline for any other source)
/// 3. For each row, execute the bin entity with that row's values as arguments
/// 4. Combine all results into a single Anonymous relation
fn execute_bin_entity_pipe(
    source: RelationalExpression,
    executable: &dyn EffectExecutable,
    ctes: &[CteBinding],
    system: &mut DelightQLSystem,
) -> Result<RelationalExpression> {
    let executed_source = execute_effects_in_expression(source, ctes, system)?;
    let (_headers, rows) = extract_rows(executed_source, ctes, system)?;
    if rows.is_empty() {
        return Err(DelightQLError::database_error(
            "No rows to pass to bin entity",
            "Empty source",
        ));
    }

    let mut all_result_rows = Vec::new();
    let mut result_headers: Option<Vec<DomainExpression>> = None;

    for row_values in &rows {
        let result = executable.execute(row_values, None, system)?;
        match result {
            crate::bin_cartridge::EntityResult::Relation(Relation::Anonymous {
                column_headers,
                rows: result_rows,
                ..
            }) => {
                if result_headers.is_none() {
                    result_headers = column_headers;
                }
                all_result_rows.extend(result_rows);
            }
            crate::bin_cartridge::EntityResult::Relation(_) => {
                // Non-anonymous relation result — skip row collection
            }
        }
    }

    Ok(RelationalExpression::Relation(Relation::Anonymous {
        column_headers: result_headers,
        rows: all_result_rows,
        alias: None,
        outer: false,
        exists_mode: false,
        qua_target: None,
        cpr_schema: PhaseBox::phantom(),
    }))
}

/// Extract rows from a source expression.
///
/// Fast path: if the source is an Anonymous relation, extract rows directly.
/// Otherwise: wrap in a Query, compile through the full pipeline to SQL,
/// execute against the database, and convert result rows to DomainExpressions.
/// This allows ANY query (filtered, joined, CTE, actual table) to be piped
/// into bin entities and directives.
fn extract_rows(
    expr: RelationalExpression,
    ctes: &[CteBinding],
    system: &mut DelightQLSystem,
) -> Result<(Vec<String>, Vec<Vec<DomainExpression>>)> {
    // Fast path: anonymous table literal — extract rows directly from AST
    if let Ok(result) = extract_anonymous_rows(&expr) {
        return Ok(result);
    }

    // Full pipeline path: compile the source to SQL and execute it
    let query = if ctes.is_empty() {
        Query::Relational(expr)
    } else {
        Query::WithCtes {
            ctes: ctes.to_vec(),
            query: expr,
        }
    };

    let mut pipeline = Pipeline::new_from_unresolved_query(query, system);
    let sql = pipeline.execute_to_sql().map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to compile pipe source to SQL: {}", e),
            "Pipe source compilation",
        )
    })?;
    let sql = sql.to_string();

    let conn = system.connection.lock().map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to acquire connection lock: {}", e),
            "Connection lock",
        )
    })?;

    let (col_names, string_rows) = conn.query_all_string_rows(&sql, &[]).map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to execute pipe source query: {}", e),
            "Pipe source execution",
        )
    })?;

    let rows: Vec<Vec<DomainExpression>> = string_rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|val| DomainExpression::Literal {
                    value: LiteralValue::String(val),
                    alias: None,
                })
                .collect()
        })
        .collect();

    Ok((col_names, rows))
}

/// Extract column headers and row values from an Anonymous relation
fn extract_anonymous_rows(
    expr: &RelationalExpression,
) -> Result<(Vec<String>, Vec<Vec<DomainExpression>>)> {
    match expr {
        RelationalExpression::Relation(Relation::Anonymous {
            column_headers,
            rows,
            ..
        }) => {
            // Extract header names from domain expressions
            let headers: Vec<String> = match column_headers {
                Some(exprs) => exprs
                    .iter()
                    .map(|e| match e {
                        DomainExpression::Lvar {
                            name, alias: None, ..
                        } => name.to_string(),
                        DomainExpression::Lvar { alias: Some(a), .. } => a.to_string(),
                        DomainExpression::Literal {
                            value: LiteralValue::String(s),
                            ..
                        } => s.clone(),
                        _ => format!("{:?}", e),
                    })
                    .collect(),
                None => {
                    // No headers — generate positional names
                    if let Some(first_row) = rows.first() {
                        (0..first_row.values.len())
                            .map(|i| format!("col{}", i))
                            .collect()
                    } else {
                        Vec::new()
                    }
                }
            };

            let row_values: Vec<Vec<DomainExpression>> =
                rows.iter().map(|r| r.values.clone()).collect();

            Ok((headers, row_values))
        }
        _ => Err(DelightQLError::database_error(
            "Directive pipe terminal requires a directive source (e.g., consult!, mount!), \
             not a table or subquery. Only directive results can be piped to other directives.",
            "Invalid directive pipe source",
        )),
    }
}

/// Bind directive terminal arguments against a source row
///
/// For each terminal argument:
/// - Glob (*) → expand to all column values from the row (in header order)
/// - Lvar (column reference) → look up that column name in headers
/// - Literal → pass through unchanged
fn bind_directive_args(
    headers: &[String],
    row_values: &[DomainExpression],
    terminal_args: &[DomainExpression],
) -> Result<Vec<DomainExpression>> {
    let mut bound = Vec::new();

    for arg in terminal_args {
        match arg {
            DomainExpression::Projection(ProjectionExpr::Glob { .. }) => {
                // Expand glob to all column values
                bound.extend(row_values.iter().cloned());
            }
            DomainExpression::Lvar {
                name,
                namespace_path,
                ..
            } if namespace_path.is_empty() => {
                // Look up column name in headers
                let col_name = name.as_ref();
                if let Some(idx) = headers.iter().position(|h| h == col_name) {
                    if idx < row_values.len() {
                        bound.push(row_values[idx].clone());
                    } else {
                        return Err(DelightQLError::database_error(
                            format!(
                                "Column '{}' found in headers but row has too few values",
                                col_name
                            ),
                            "Directive pipe argument binding",
                        ));
                    }
                } else {
                    return Err(DelightQLError::database_error(
                        format!(
                            "Column '{}' not found in directive source. Available columns: {:?}",
                            col_name, headers
                        ),
                        "Directive pipe argument binding",
                    ));
                }
            }
            // Literals and other expressions pass through unchanged
            _ => {
                bound.push(arg.clone());
            }
        }
    }

    Ok(bound)
}

/// Execute a specific pseudo-predicate and return its result as an inline table
fn execute_pseudo_predicate(
    name: &str,
    arguments: &[DomainExpression],
    alias: Option<String>,
    system: &mut DelightQLSystem,
) -> Result<Relation> {
    // Look up the entity in the bin cartridge registry
    // lookup_entity() returns Arc<dyn BinEntity>, which we can hold after
    // the registry borrow is released (Arc keeps the entity alive)
    let entity = system.bin_registry().lookup_entity(name).ok_or_else(|| {
        // Check for renamed pseudo-predicates and give a helpful error
        const RENAMED: &[(&str, &str)] = &[
            ("engage!", "enlist!"),
            ("part!", "delist!"),
            ("ground_into!", "ground!"),
        ];
        if let Some((_, new_name)) = RENAMED.iter().find(|(old, _)| *old == name) {
            DelightQLError::database_error(
                format!("{}() has been renamed to {}(). Please update your code.", name, new_name),
                "Renamed directive",
            )
        } else {
            DelightQLError::database_error(
                format!("Unknown pseudo-predicate: {}", name),
                "Pseudo-predicate not found in registry. Make sure it's registered in a bin cartridge.",
            )
        }
    })?;
    // Registry borrow ends here, but Arc keeps entity alive

    // Downcast to EffectExecutable
    // This is safe because we only look up pseudo-predicates here, and they all implement EffectExecutable
    let executable = entity.as_effect_executable().ok_or_else(|| {
        DelightQLError::database_error(
            format!(
                "Entity '{}' is not executable at Phase 1.X (Effect Executor). \
                 Only entities implementing EffectExecutable can be executed here.",
                name
            ),
            "Not an effect-executable entity",
        )
    })?;

    // Now we can execute with a mutable borrow of system
    let result = executable.execute(arguments, alias, system)?;

    // Convert EntityResult to Relation
    let crate::bin_cartridge::EntityResult::Relation(relation) = result;
    Ok(relation)
}
