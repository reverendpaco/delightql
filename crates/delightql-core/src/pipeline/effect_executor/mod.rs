// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
use crate::pipeline::ast_visit::{
    walk_visit_boolean, walk_visit_domain, walk_visit_operator, walk_visit_query,
    walk_visit_relation, walk_visit_relational, walk_visit_sigma, AstVisit, Descent,
};
use crate::pipeline::asts::effects::{directive_category, DirectiveCategory};
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::Unresolved;
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
            // A CTE definition is a data position, not the REPL/CLI top level.
            // Walk the complete definition before resolution so an R9-illegal
            // session directive cannot survive as a pseudo-predicate panic.
            for cte in &ctes {
                refuse_nested_session_directives_in_relational(&cte.expression)?;
            }
            // Rewrite the main query expression, passing CTE bindings for resolution
            let rewritten_query = execute_effects_in_expression(query, &ctes, system)?;
            Ok(Query::WithCtes {
                ctes,
                query: rewritten_query,
            })
        }
        Query::WithCfes { cfes, query } => {
            for cfe in &cfes {
                refuse_nested_session_directives_in_domain(&cfe.body)?;
            }
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
        Query::ReplTempTable { query, table_name } => {
            refuse_nested_session_directives_in_query(&query)?;
            Ok(Query::ReplTempTable {
                query,
                table_name,
            })
        }
        Query::WithErContext { context, query } => {
            // R9 (DIRECTIVE-CONVERGENCE-PLAN Phase 1B): a transparent ER
            // context is not the REPL/CLI top level, so a session-directive
            // demand beneath it must refuse with the rule-citing diagnostic
            // instead of surviving into the resolver as a pseudo-predicate
            // panic. Recursive DISCOVERY only — do not recurse execution
            // here: blind recursion was tried during wvvrqxzv and regressed
            // snapshot--82/83/84 and torture--99 through duplicate/altered
            // enlist context. Pinned by directive_contract/04_er_wrapper_r9.
            refuse_nested_session_directives_in_query(&query)?;
            Ok(Query::WithErContext { context, query })
        }
        Query::ReplTempView { query, view_name } => {
            refuse_nested_session_directives_in_query(&query)?;
            Ok(Query::ReplTempView {
                query,
                view_name,
            })
        }
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
            let rewritten_relation = execute_effects_in_relation(relation, ctes, system)?;
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
                refuse_nested_session_directives_in_operator_arguments(&arguments)?;
                execute_directive_pipe(source, &name, &arguments, ctes, system)
            } else if let UnaryRelationalOperator::HoViewApplication {
                ref function,
                namespace: Some(ref ns),
                ..
            } = operator
            {
                refuse_nested_session_directives_in_operator(&operator)?;
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
                refuse_nested_session_directives_in_operator(&operator)?;
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
            if let Some(condition) = &join_condition {
                refuse_nested_session_directives_in_boolean(condition)?;
            }
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
            // Predicate subqueries are data positions. R9 makes session
            // directives illegal here; the complete visitor turns every such
            // occurrence into a clean refusal before the resolver.
            refuse_nested_session_directives_in_sigma(&condition)?;
            Ok(RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            })
        }
        RelationalExpression::ErJoinChain { relations } => Ok(
            RelationalExpression::ErJoinChain {
                relations: relations
                    .into_iter()
                    .map(|r| execute_effects_in_relation(r, ctes, system))
                    .collect::<Result<Vec<_>>>()?,
            },
        ),
        RelationalExpression::ErTransitiveJoin { left, right } => Ok(
            RelationalExpression::ErTransitiveJoin {
                left: Box::new(execute_effects_in_expression(*left, ctes, system)?),
                right: Box::new(execute_effects_in_expression(*right, ctes, system)?),
            },
        ),
        RelationalExpression::IntersectCorresponding { .. } => {
            unreachable!("IntersectCorresponding does not exist in the unresolved phase")
        }
    }
}

const NESTED_SESSION_DIRECTIVE_MESSAGE: &str =
    "session directives are legal only at the REPL/CLI top level or the liminal space — not nested in a query";

fn nested_session_directive_error(name: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "effect/session/position",
        format!("{name}: {NESTED_SESSION_DIRECTIVE_MESSAGE}"),
        "EFFECT-ALGEBRA R9",
    )
}

/// A tenant on the shared whole-tree visitor. It names only the semantic
/// boundary; the visitor owns the complete recursion over every carrier.
struct NestedSessionDirectiveGuard {
    skip_root_relation: bool,
    skip_root_operator: bool,
}

impl AstVisit<Unresolved> for NestedSessionDirectiveGuard {
    fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
        if std::mem::take(&mut self.skip_root_relation) {
            return Ok(Descent::Continue);
        }
        if let Relation::PseudoPredicate { name, .. } = relation {
            if directive_category(name) == DirectiveCategory::Session {
                return Err(nested_session_directive_error(name));
            }
        }
        Ok(Descent::Continue)
    }

    fn enter_operator(&mut self, operator: &UnaryRelationalOperator) -> Result<Descent> {
        if std::mem::take(&mut self.skip_root_operator) {
            return Ok(Descent::Continue);
        }
        let name = if let UnaryRelationalOperator::DirectiveTerminal { name, .. }
        | UnaryRelationalOperator::DirectivePipeInvocation { name, .. } = operator
        {
            Some(name)
        } else {
            None
        };
        if let Some(name) = name {
            if directive_category(name) == DirectiveCategory::Session {
                return Err(nested_session_directive_error(name));
            }
        }
        Ok(Descent::Continue)
    }
}

fn nested_session_guard() -> NestedSessionDirectiveGuard {
    NestedSessionDirectiveGuard {
        skip_root_relation: false,
        skip_root_operator: false,
    }
}

/// Compile purity (DIRECTIVE-CONVERGENCE-PLAN Phase 1B): find every demand
/// that Phase 1.X would EXECUTE, so a pure inspection surface can refuse it
/// cleanly before any mutation. The executing positions mirror this module's
/// dispatch exactly: pseudo-predicate relations, directive pipe terminals,
/// and namespace-qualified bin executables in Ground/TVF/piped positions.
/// DML terminals are absent deliberately — they lower to SQL and execute
/// only when the compiled query runs, so inspecting them is already pure
/// (pinned by directive_contract/06_compile_is_pure).
struct ExecutingDemandGuard<'a> {
    registry: &'a crate::bin_cartridge::registry::BinCartridgeRegistry,
    stage: &'a str,
}

impl ExecutingDemandGuard<'_> {
    fn refuse(&self, name: &str) -> DelightQLError {
        DelightQLError::validation_error_categorized(
            "effect/compile/purity",
            format!(
                "sys::execution.compile is pure: compiling to stage '{}' would \
                 execute '{}' — inspection must never mutate the namespace, \
                 database, filesystem, output, or session. Compile to 'cst' or \
                 'ast-unresolved' to inspect this source, or run it as a query \
                 to execute it.",
                self.stage, name
            ),
            "compile purity",
        )
    }

    fn is_bin_executable(&self, ns: &[&str], name: &str) -> bool {
        self.registry
            .lookup_qualified_entity(ns, name)
            .and_then(|entity| entity.as_effect_executable().map(|_| ()))
            .is_some()
    }
}

impl AstVisit<Unresolved> for ExecutingDemandGuard<'_> {
    fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
        match relation {
            Relation::PseudoPredicate { name, .. } => Err(self.refuse(name)),
            Relation::Ground {
                identifier,
                ..
            } if !identifier.namespace_path.is_empty() => {
                let ns: Vec<&str> = identifier
                    .namespace_path
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect();
                if self.is_bin_executable(&ns, identifier.name.as_str()) {
                    return Err(self.refuse(identifier.name.as_str()));
                }
                Ok(Descent::Continue)
            }
            Relation::TVF {
                function,
                namespace: Some(ns),
                ..
            } if !ns.is_empty() => {
                let ns: Vec<&str> = ns.iter().map(|item| item.name.as_str()).collect();
                if self.is_bin_executable(&ns, function.as_str()) {
                    return Err(self.refuse(function.as_str()));
                }
                Ok(Descent::Continue)
            }
            _ => Ok(Descent::Continue),
        }
    }

    fn enter_operator(&mut self, operator: &UnaryRelationalOperator) -> Result<Descent> {
        match operator {
            UnaryRelationalOperator::DirectiveTerminal { name, .. }
            | UnaryRelationalOperator::DirectivePipeInvocation { name, .. } => {
                Err(self.refuse(name))
            }
            UnaryRelationalOperator::HoViewApplication {
                function,
                namespace: Some(ns),
                ..
            } => {
                let ns: Vec<&str> = ns.iter().map(|item| item.name.as_str()).collect();
                if self.is_bin_executable(&ns, function.as_str()) {
                    return Err(self.refuse(function.as_str()));
                }
                Ok(Descent::Continue)
            }
            _ => Ok(Descent::Continue),
        }
    }
}

/// Refuse every demand Phase 1.X would execute, for a pure inspection of
/// `query` rendered at `stage`. See `ExecutingDemandGuard`.
pub(crate) fn refuse_executing_demands_for_inspection(
    query: &Query,
    registry: &crate::bin_cartridge::registry::BinCartridgeRegistry,
    stage: &str,
) -> Result<()> {
    let mut guard = ExecutingDemandGuard { registry, stage };
    walk_visit_query(&mut guard, query)?;
    Ok(())
}

fn refuse_nested_session_directives_in_relational(expr: &RelationalExpression) -> Result<()> {
    walk_visit_relational(&mut nested_session_guard(), expr)?;
    Ok(())
}

fn refuse_nested_session_directives_in_query(query: &Query) -> Result<()> {
    walk_visit_query(&mut nested_session_guard(), query)?;
    Ok(())
}

fn refuse_nested_session_directives_in_domain(expression: &DomainExpression) -> Result<()> {
    walk_visit_domain(&mut nested_session_guard(), expression)?;
    Ok(())
}

fn refuse_nested_session_directives_in_sigma(condition: &SigmaCondition) -> Result<()> {
    walk_visit_sigma(&mut nested_session_guard(), condition)?;
    Ok(())
}

fn refuse_nested_session_directives_in_boolean(condition: &BooleanExpression) -> Result<()> {
    walk_visit_boolean(&mut nested_session_guard(), condition)?;
    Ok(())
}

fn refuse_nested_session_directives_in_operator(operator: &UnaryRelationalOperator) -> Result<()> {
    let mut guard = nested_session_guard();
    guard.skip_root_operator = true;
    walk_visit_operator(&mut guard, operator)?;
    Ok(())
}

fn refuse_nested_session_directives_in_operator_arguments(
    arguments: &[DomainExpression],
) -> Result<()> {
    let operator = UnaryRelationalOperator::DirectiveTerminal {
        name: "__root__".to_string(),
        arguments: arguments.to_vec(),
    };
    refuse_nested_session_directives_in_operator(&operator)
}

/// Execute pseudo-predicates in a relation
fn execute_effects_in_relation(
    relation: Relation,
    ctes: &[CteBinding],
    system: &mut DelightQLSystem,
) -> Result<Relation> {
    // The relation itself is on the executable source spine; all fields below
    // it are data positions. Skip only that root and inspect every child edge.
    let mut guard = nested_session_guard();
    guard.skip_root_relation = true;
    walk_visit_relation(&mut guard, &relation)?;

    match relation {
        // This is the key case: execute the pseudo-predicate!
        Relation::PseudoPredicate {
            name,
            namespace,
            arguments,
            access,
            alias,
            ..
        } => execute_pseudo_predicate(&name, &namespace, &arguments, &access, alias, system),

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
                        Box::new(execute_effects_in_expression(*subquery, ctes, system)?);
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
                    system.note_effect_executed();
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
            ref ho_arguments,
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
                    let dom_args: Vec<DomainExpression> = ho_arguments
                        .iter()
                        .filter_map(|arg| match arg {
                            crate::pipeline::asts::core::operators::HoArgument::Scalar(dom) => {
                                Some(dom.clone())
                            }
                            _ => None,
                        })
                        .collect();
                    let alias_str = alias.as_ref().map(|s| s.to_string());
                    system.note_effect_executed();
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
/// THE SESSION-CHAIN DISPATCHER (M3's legal Phase-1.X path, RECONCILED
/// by D3c — see EFFECT-BARRIER-DESIGN M4): top-level session
/// orchestration lives here deliberately, now set-at-a-time (one lifted
/// call, never a row loop). Its stringly binding
/// (`bind_directive_args`) remains the fenced interim seam, retired
/// when `execute_lifted` takes a typed relation — the typed-program
/// consolidation step.
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
    // EFFECT-ALGEBRA §3 (amended 2026-07-15): piping a WHOLE receipt where
    // a directive's argumentative functor expects its PAYLOAD is a shape
    // error, taught as such. Detected structurally, before anything
    // executes: the source is itself a directive invocation (its value is
    // a receipt) and the receipt's declared width does not match the
    // terminal's parameter list. Pinned by directive_contract
    // 34_bare_receipt_chain_refused.
    if let RelationalExpression::Relation(Relation::PseudoPredicate {
        name: source_name,
        namespace: source_ns,
        ..
    }) = &source
    {
        let bare = terminal_name.strip_suffix('!').unwrap_or(terminal_name);
        let terminal_arity = crate::pipeline::asts::effects::descriptor(terminal_name)
            .map(|d| d.params.len());
        let ns_strs: Vec<&str> = source_ns.iter().map(|s| s.as_str()).collect();
        let source_entity = if ns_strs.is_empty() {
            system.bin_registry().lookup_entity(source_name)
        } else {
            system
                .bin_registry()
                .lookup_qualified_entity(&ns_strs, source_name)
        };
        let receipt_width = source_entity.and_then(|e| match e.signature().output_schema {
            crate::bin_cartridge::OutputSchema::Relation(cols) => Some(cols.len()),
            crate::bin_cartridge::OutputSchema::Void => None,
        });
        if let (Some(arity), Some(width)) = (terminal_arity, receipt_width) {
            if width != arity {
                return Err(DelightQLError::validation_error_categorized(
                    "directive/chain/receipt_shape",
                    format!(
                        "{source_name} |> {terminal_name} pipes a WHOLE receipt \
                         ({width} declared column(s)) into {bare}!'s \
                         {arity}-parameter argumentative functor — a shape error. \
                         Release the payload first: \
                         {source_name}(…) |> .returned(*) |> {terminal_name}(*) \
                         (EFFECT-ALGEBRA §3)",
                    ),
                    "receipt into directive",
                ));
            }
        }
    }

    // 1. Execute source (recursively handles chained directive pipes and pseudo-predicates)
    let executed_source = execute_effects_in_expression(source, ctes, system)?;

    // 2. Extract rows — fast path for anonymous, full pipeline for anything else
    let (headers, rows) = extract_rows(executed_source, ctes, system)?;

    // 3. ONE set-at-a-time application (D3b, subsuming M1's boundary):
    // the rowwise loop is DELETED. A piped relation is one demand of the
    // terminal's argumentative functor (M2 reframe) — the lifted rows
    // bind once and the entity executes ONCE:
    //
    //   - doc! (the first setwise override) receives the whole lifted
    //     relation and answers ONE receipt (directive_contract 38);
    //   - a ONE-row lift takes the scalar path (execute_pseudo_predicate,
    //     with its descriptor arity and receipt-access discipline) — the
    //     shape every pinned session chain uses;
    //   - a MULTI-row lift to any non-overriding terminal refuses with
    //     not-yet (the E1a precedent; directive_contract 37) — the
    //     refusal now lives in EffectExecutable::execute_lifted's
    //     default, uniformly for every category;
    //   - an EMPTY lift stays the status-quo no-op.
    let bound_rows: Vec<Vec<DomainExpression>> = rows
        .iter()
        .map(|row_values| bind_directive_args(&headers, row_values, terminal_args))
        .collect::<Result<_>>()?;

    let bare = terminal_name.strip_suffix('!').unwrap_or(terminal_name);
    let result = if bare == "doc" || bound_rows.len() != 1 {
        let entity = system.bin_registry().lookup_entity(terminal_name).ok_or_else(|| {
            DelightQLError::database_error(
                format!("Unknown pseudo-predicate: {}", terminal_name),
                "Entity not found",
            )
        })?;
        let executable = entity.as_effect_executable().ok_or_else(|| {
            DelightQLError::database_error(
                format!("Entity '{}' is not effect-executable", terminal_name),
                "Not an effect-executable entity",
            )
        })?;
        system.note_effect_executed();
        let crate::bin_cartridge::EntityResult::Relation(r) =
            executable.execute_lifted(&bound_rows, None, system)?;
        r
    } else {
        execute_pseudo_predicate(
            terminal_name,
            &[],
            &bound_rows[0],
            &crate::pipeline::asts::core::DomainSpec::Glob,
            None,
            system,
        )?
    };

    Ok(RelationalExpression::Relation(result))
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

    // D3c: ONE set-at-a-time application (the last rowwise loop deleted).
    // Finding 1 (CODE-REVIEW-zzpmxuzp::otolxyzl): an EMPTY source is not
    // refused here — pipe is application, so the lift reaches the entity
    // once regardless of cardinality; execute_lifted (or its override)
    // owns the semantics.
    // The lifted default delegates one-row lifts to the scalar execute
    // and refuses multi-row lifts with not-yet; a setwise entity (doc!)
    // receives the whole relation and answers one receipt.
    system.note_effect_executed();
    let crate::bin_cartridge::EntityResult::Relation(r) =
        executable.execute_lifted(&rows, None, system)?;
    Ok(RelationalExpression::Relation(r))
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
///
/// Resolution (DIRECTIVE-CONVERGENCE-PLAN Phase 2): identity is
/// (namespace, name). Qualified invocations resolve their spelled path
/// exactly; unqualified invocations reach only universal-visibility
/// namespaces. Contextual absences refuse by DESCRIPTOR POLICY with a
/// teaching diagnostic, never with a bare "unknown" error, and the
/// descriptor's arity is authoritative before the entity runs.
fn execute_pseudo_predicate(
    name: &str,
    namespace: &[String],
    arguments: &[DomainExpression],
    access: &crate::pipeline::asts::core::DomainSpec,
    alias: Option<String>,
    system: &mut DelightQLSystem,
) -> Result<Relation> {
    use crate::pipeline::asts::effects::{descriptor, DirectiveRealization, RENAMED_DIRECTIVES};

    let ns_strs: Vec<&str> = namespace.iter().map(|s| s.as_str()).collect();
    let entity = if ns_strs.is_empty() {
        system.bin_registry().lookup_entity(name)
    } else {
        system
            .bin_registry()
            .lookup_qualified_entity(&ns_strs, name)
    };

    let entity = entity.ok_or_else(|| {
        let bare = name.strip_suffix('!').unwrap_or(name);
        // Renamed pseudo-predicates get the migration hint first.
        if let Some((_, new_name)) = RENAMED_DIRECTIVES.iter().find(|(old, _)| *old == bare) {
            return DelightQLError::database_error(
                format!(
                    "{}!() has been renamed to {}!(). Please update your code.",
                    bare, new_name
                ),
                "Renamed directive",
            );
        }
        // Descriptor policy: an intentional contextual absence refuses with
        // a rule-citing diagnostic, never a registration accident.
        if let Some(desc) = descriptor(name) {
            match desc.realization {
                DirectiveRealization::SyntaxPipeTerminal => {
                    return DelightQLError::validation_error_categorized(
                        "directive/context/pipe_terminal",
                        format!(
                            "'{bare}!' is a pipe terminal, not a callable \
                             pseudo-predicate — it needs its piped input \
                             relation: source |> {bare}!(…)(*)"
                        ),
                        "directive policy",
                    );
                }
                DirectiveRealization::LiminalOnly => {
                    return DelightQLError::validation_error_categorized(
                        "directive/context/liminal_only",
                        format!(
                            "'{bare}!' is legal only in the liminal space of a \
                             consulted file, not as a query invocation"
                        ),
                        "directive policy",
                    );
                }
                DirectiveRealization::Entity => {
                    // A registered entity that this lookup could not see:
                    // either a wrong qualifier or non-universal visibility.
                    return DelightQLError::database_error(
                        format!(
                            "'{}!' is not visible {} — its identity is {}.{bare}!",
                            bare,
                            if ns_strs.is_empty() {
                                "unqualified".to_string()
                            } else {
                                format!("in namespace '{}'", ns_strs.join("::"))
                            },
                            desc.namespace,
                        ),
                        "Directive not visible",
                    );
                }
            }
        }
        DelightQLError::database_error(
            format!("Unknown pseudo-predicate: {}", name),
            "Pseudo-predicate not found in registry. Make sure it's registered in a bin cartridge.",
        )
    })?;
    // Registry borrow ends here, but Arc keeps entity alive

    // Descriptor arity is AUTHORITATIVE for binding: refuse before the
    // entity runs. (Entities keep internal checks as redundancy; agreement
    // between descriptor and entity signature is pinned by unit tests.)
    if let Some(desc) = descriptor(name) {
        let required = desc.params.iter().filter(|p| !p.optional).count();
        let maximum = desc.params.len();
        if arguments.len() < required || arguments.len() > maximum {
            let bare = name.strip_suffix('!').unwrap_or(name);
            let expectation = if required == maximum {
                format!("{required}")
            } else {
                format!("{required}..{maximum}")
            };
            return Err(DelightQLError::validation_error_categorized(
                "directive/binding/arity",
                format!(
                    "{bare}! expects {expectation} argument(s) ({}), got {}",
                    desc.params
                        .iter()
                        .map(|p| p.name)
                        .collect::<Vec<_>>()
                        .join(", "),
                    arguments.len()
                ),
                "directive arity",
            ));
        }
    }

    // EFFECT DISCIPLINE (Phase 3a review remediation, P1): a rejected
    // program must not leave behind the effect whose result it failed to
    // bind. Receipt access is validated against the entity's DECLARED
    // output schema BEFORE execution — never discovered by executing first.
    prevalidate_receipt_access(&entity.signature().output_schema, access, name)?;

    // Downcast to EffectExecutable
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
    system.note_effect_executed();
    let result = executable.execute(arguments, alias, system)?;

    // Convert EntityResult to Relation, then apply the returned-relation
    // access specification (the SECOND parentheses — Phase 3 canonical
    // invocation: parameters first, receipt access second).
    let crate::bin_cartridge::EntityResult::Relation(relation) = result;
    apply_receipt_access(relation, access, name, &entity.signature().output_schema)
}

/// Prevalidate a returned-relation access specification against a
/// directive's DECLARED output schema — before any effect executes.
///
/// INTERIM GATE (pre-Phase-4): only `(*)` and a positional list of plain
/// unqualified names whose arity matches the declared receipt heading are
/// permitted. Everything else refuses HERE, so a malformed access can
/// never consult/enlist/mount first and fail second. Phase 4's canonical
/// receipts replace this gate (and the binder below) with ordinary
/// relation-access machinery over authoritative receipt headings.
fn prevalidate_receipt_access(
    schema: &crate::bin_cartridge::OutputSchema,
    access: &crate::pipeline::asts::core::DomainSpec,
    directive_name: &str,
) -> Result<()> {
    use crate::pipeline::asts::core::DomainSpec;

    let exprs = match access {
        DomainSpec::Glob | DomainSpec::Bare => return Ok(()),
        DomainSpec::Positional(exprs) => exprs,
        other => {
            return Err(DelightQLError::validation_error_categorized(
                "directive/invocation/access",
                format!(
                    "unsupported receipt access on {directive_name}: {other:?} — \
                     use (*) for the whole receipt or a positional binding list"
                ),
                "receipt access",
            ))
        }
    };
    if exprs.len() == 1
        && matches!(
            &exprs[0],
            DomainExpression::Projection(
                crate::pipeline::asts::core::expressions::domain::ProjectionExpr::Glob { .. }
            )
        )
    {
        return Ok(());
    }
    for e in exprs {
        if !matches!(
            e,
            DomainExpression::Lvar {
                qualifier: None,
                ..
            }
        ) {
            return Err(DelightQLError::validation_error_categorized(
                "directive/invocation/access",
                format!(
                    "receipt access on {directive_name} must be a positional \
                     binding list of plain names, got {e:?}"
                ),
                "receipt access",
            ));
        }
    }
    match schema {
        crate::bin_cartridge::OutputSchema::Relation(cols) => {
            if exprs.len() != cols.len() {
                let heading = cols
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(DelightQLError::validation_error_categorized(
                    "directive/invocation/access",
                    format!(
                        "receipt access on {directive_name} binds {} column(s) but \
                         the declared receipt has {} ({heading}) — positional \
                         binding requires the exact arity, or (*) for the whole \
                         receipt",
                        exprs.len(),
                        cols.len()
                    ),
                    "receipt access",
                ));
            }
        }
        crate::bin_cartridge::OutputSchema::Void => {
            return Err(DelightQLError::validation_error_categorized(
                "directive/invocation/access",
                format!(
                    "receipt access on {directive_name}: this entity declares no \
                     receipt columns — use (*)"
                ),
                "receipt access",
            ));
        }
    }
    Ok(())
}

/// Apply a returned-relation access specification to a directive's result.
///
/// INTERIM GATE (pre-Phase-4, see `prevalidate_receipt_access`): NOT the
/// final binder. `(*)` is the full receipt; a positional access list binds
/// the receipt's columns positionally by RENAMING — prevalidated against
/// the declared schema before execution, so a failure here means the
/// entity broke its own declaration. Phase 4 replaces this with ordinary
/// relation-access machinery over canonical receipts (unification,
/// placeholders, and qualified access included).
fn apply_receipt_access(
    relation: Relation,
    access: &crate::pipeline::asts::core::DomainSpec,
    directive_name: &str,
    declared_schema: &crate::bin_cartridge::OutputSchema,
) -> Result<Relation> {
    use crate::pipeline::asts::core::DomainSpec;

    let exprs = match access {
        DomainSpec::Glob | DomainSpec::Bare => return Ok(relation),
        DomainSpec::Positional(exprs) => exprs,
        other => {
            return Err(DelightQLError::validation_error_categorized(
                "directive/invocation/access",
                format!(
                    "unsupported receipt access on {directive_name}: {other:?} — \
                     use (*) for the whole receipt or a positional binding list"
                ),
                "receipt access",
            ))
        }
    };
    // A lone glob inside the positional list is still the full receipt.
    if exprs.len() == 1
        && matches!(
            &exprs[0],
            DomainExpression::Projection(
                crate::pipeline::asts::core::expressions::domain::ProjectionExpr::Glob { .. }
            )
        )
    {
        return Ok(relation);
    }

    let binder_names: Vec<String> = exprs
        .iter()
        .map(|e| match e {
            DomainExpression::Lvar {
                name,
                qualifier: None,
                ..
            } => Ok(name.to_string()),
            other => Err(DelightQLError::validation_error_categorized(
                "directive/invocation/access",
                format!(
                    "receipt access on {directive_name} must be a positional \
                     binding list of plain names, got {other:?}"
                ),
                "receipt access",
            )),
        })
        .collect::<Result<Vec<_>>>()?;

    // An interior-bearing receipt (EFFECT-ALGEBRA §3 amended: the
    // construction is an inner relation, not an inline table) binds
    // positionally by RENAMING through an ordinary projection over the
    // DECLARED heading — prevalidation already matched the arity.
    if let Relation::InnerRelation { .. } = &relation {
        let crate::bin_cartridge::OutputSchema::Relation(cols) = declared_schema else {
            return Err(DelightQLError::validation_error_categorized(
                "directive/invocation/access",
                format!(
                    "receipt access on {directive_name}: this entity declares no \
                     receipt columns — use (*)"
                ),
                "receipt access",
            ));
        };
        use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
        use crate::pipeline::asts::core::expressions::PipeExpression;
        use crate::pipeline::asts::core::ContainmentSemantic;
        let renamed = RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
            PipeExpression {
                source: RelationalExpression::Relation(relation),
                operator: UnaryRelationalOperator::General {
                    containment_semantic: ContainmentSemantic::Parenthesis,
                    expressions: cols
                        .iter()
                        .zip(binder_names.iter())
                        .map(|((declared, _), bound)| {
                            let mut b =
                                DomainExpression::lvar_builder(declared.clone());
                            if bound != declared {
                                b = b.with_alias(bound.clone());
                            }
                            b.build()
                        })
                        .collect(),
                },
                cpr_schema: PhaseBox::phantom(),
            },
        )));
        let wrapper = format!("__b_{}", directive_name.trim_end_matches('!'));
        return Ok(Relation::InnerRelation {
            pattern: InnerRelationPattern::Indeterminate {
                identifier: crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                    namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
                    name: wrapper.clone().into(),
                    grounding: None,
                },
                subquery: Box::new(renamed),
            },
            alias: Some(wrapper.into()),
            outer: false,
            cpr_schema: PhaseBox::phantom(),
        });
    }

    let Relation::Anonymous {
        column_headers,
        rows,
        alias,
        outer,
        exists_mode,
        qua_target,
        ..
    } = relation
    else {
        return Err(DelightQLError::validation_error_categorized(
            "directive/invocation/access",
            format!(
                "receipt access on {directive_name}: the directive result is \
                 not an inline receipt relation"
            ),
            "receipt access",
        ));
    };

    let receipt_width = column_headers
        .as_ref()
        .map(|h| h.len())
        .or_else(|| rows.first().map(|r| r.values.len()))
        .unwrap_or(0);
    if binder_names.len() != receipt_width {
        let heading = column_headers
            .as_ref()
            .map(|hs| {
                hs.iter()
                    .filter_map(|h| match h {
                        DomainExpression::Lvar { name, .. } => Some(name.to_string()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        return Err(DelightQLError::validation_error_categorized(
            "directive/invocation/access",
            format!(
                "receipt access on {directive_name} binds {} column(s) but the \
                 receipt has {} ({heading}) — positional binding requires the \
                 exact arity, or (*) for the whole receipt",
                binder_names.len(),
                receipt_width
            ),
            "receipt access",
        ));
    }

    let bound_headers = binder_names
        .into_iter()
        .map(|n| DomainExpression::lvar_builder(n).build())
        .collect();
    Ok(Relation::Anonymous {
        column_headers: Some(bound_headers),
        rows,
        alias,
        outer,
        exists_mode,
        qua_target,
        cpr_schema: PhaseBox::phantom(),
    })
}
