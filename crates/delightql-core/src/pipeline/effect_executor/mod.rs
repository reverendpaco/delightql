// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The effect executor
//!
//! This stage executes pseudo-predicates (state-mutating relations) and rewrites
//! the AST by replacing them with inline result tables.
//!
//! ## Overview
//!
//! Pseudo-predicates are special relations ending with `!` that:
//! 1. Execute immediately when encountered
//! 2. Mutate system state (open connections, register namespaces, etc.)
//! 3. Return result tables that replace them in the AST
//!
//! ## Dispatch
//!
//! Directives dispatch through the descriptor registry (`descriptor(name)`,
//! `DirectiveRealization`, renamed-directive teaching) — the supported set
//! is the registry's, never a list maintained here.
//!
//! ## Architecture
//!
//! The effect executor hooks between the builder and the resolver:
//! ```text
//! CST → Builder → Effect Executor → CFE Precompiler → Resolver → ...
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
use crate::names::Registry;
use crate::pipeline::ast_visit::{
    walk_visit_boolean, walk_visit_domain, walk_visit_operator, walk_visit_query,
    walk_visit_relation, walk_visit_relational, AstVisit, Descent,
};
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::MemberCorrelation;
use crate::pipeline::asts::core::Unresolved;
use crate::pipeline::asts::core::{GroundForm, NamedReference, Reference};
use crate::pipeline::asts::effects::DirectiveCategory;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::Pipeline;
use crate::system::DelightQLSystem;
use std::rc::Rc;

/// Execute all pseudo-predicates in a query and rewrite the AST
///
/// This is the effect executor's main entry point. It:
/// 1. Detects pseudo-predicates in the query
/// 2. Executes them in order (top-to-bottom, left-to-right)
/// 3. Replaces them with inline result tables
/// 4. Returns the rewritten query
pub fn execute_effects(
    query: Query,
    system: &mut DelightQLSystem,
    registry: &Rc<Registry>,
) -> Result<Query> {
    let Query { mut locals, body } = query;
    for cfe in locals.cfes() {
        // A DIRECTIVE IS A RELATION: the scan reads the value body.
        refuse_nested_session_directives_in_domain(&cfe.body)?;
    }
    // A CTE definition is a data position, not the REPL/CLI top level.
    // Walk the complete definition before resolution so a nested
    // session directive — illegal outside the REPL/CLI top level or
    // liminal space — cannot survive as a pseudo-predicate panic.
    for cte in locals.ctes() {
        refuse_nested_session_directives_in_relational(cte.body())?;
    }
    // A CTE body is walked like the main expression: a pure bin
    // relation piped inside a binding is intercepted here or nowhere,
    // because the resolver knows that entity only to refuse it. Each
    // body sees the bindings before it, matching the scope a body
    // may reference.
    locals.restate_ctes_in_order(|cte, reached| {
        cte.map_body(|body| execute_effects_in_expression(body, reached, system, registry))
    })?;
    // Rewrite the body, standing in the same block
    let rewritten_body = execute_effects_in_expression(body, &locals, system, registry)?;
    Ok(Query::binding(locals, rewritten_body))
}

/// Recursively traverse a relational expression and execute pseudo-predicates
#[stacksafe::stacksafe]
fn execute_effects_in_expression(
    expression: Chain,
    locals: &crate::pipeline::asts::core::QueryLocals<Unresolved>,
    system: &mut DelightQLSystem,
    registry: &Rc<Registry>,
) -> Result<Chain> {
    // Read the chain from the OUTSIDE in: the last continuation is the one
    // whose operand is everything before it.
    let mut expression = expression;
    let Some(last) = expression.pop_step() else {
        let (head, access, _) = expression.split_head_access();
        return match head.into_form() {
            GroundForm::Reference(relation) => {
                execute_effects_in_read(relation, access, locals, system, registry)
            }
            head => Ok(Chain::authored(head)),
        };
    };
    match last.into_form() {
        Continuation::BagOp {
            operator,
            arm,
            correlation,
        } => {
            let left = execute_effects_in_expression(expression, locals, system, registry)?;
            let arm = execute_effects_in_expression(arm, locals, system, registry)?;
            Ok(left.bag_op(operator, arm, correlation))
        }
        Continuation::Pipe {
            operator, named, ..
        } => {
            let source = expression;
            refuse_nested_session_directives_in_operator(&operator)?;
            // Regular pipe — recurse into the operand, preserve operator
            let executed_source = execute_effects_in_expression(source, locals, system, registry)?;
            Ok(executed_source.then(Step::authored(Continuation::Pipe { operator, named })))
        }
        Continuation::Member {
            rhs,
            correlation,
            join_type,
        } => {
            let left = execute_effects_in_expression(expression, locals, system, registry)?;
            let right = execute_effects_in_expression(rhs, locals, system, registry)?;
            // A correspondence names columns; only a condition can hold a
            // directive to refuse.
            if let Some(condition) = correlation.as_ref().and_then(MemberCorrelation::condition) {
                refuse_nested_session_directives_in_boolean(condition)?;
            }
            Ok(left.then(Step::authored(Continuation::Member {
                rhs: right,
                correlation,
                join_type,
            })))
        }
        Continuation::Restrict { condition, origin } => {
            let source = execute_effects_in_expression(expression, locals, system, registry)?;
            // Predicate subqueries are data positions, so session directives
            // are illegal here; the complete visitor turns every such
            // occurrence into a clean refusal before the resolver.
            refuse_nested_session_directives_in_boolean(&condition)?;
            Ok(source.then(Step::authored(Continuation::Restrict { condition, origin })))
        }
        // An access and a bound name no expression, and a destructure's own
        // source is a value position the boolean guard already covers. The
        // structural steps — ordering, reposition, meta, the witnesses,
        // drill and narrowing — are pure by construction: none can hold a
        // directive call.
        step @ (Continuation::Access { .. }
        | Continuation::Bound { .. }
        | Continuation::Correlate { .. }
        | Continuation::Destructure { .. }
        | Continuation::Structural(_)) => {
            let source = execute_effects_in_expression(expression, locals, system, registry)?;
            if let Continuation::Destructure { source: src, .. } = &step {
                refuse_nested_session_directives_in_domain(src)?;
            }
            Ok(source.then(Step::authored(step)))
        }
        Continuation::ErJoin(step) => {
            let left = execute_effects_in_expression(expression, locals, system, registry)?;
            Ok(left.then(Step::authored(Continuation::ErJoin(ErJoinStep {
                transitive: step.transitive,
                context: step.context,
                left_spelling: step.left_spelling,
                right_spelling: step.right_spelling,
                rhs: execute_effects_in_expression(step.rhs, locals, system, registry)?,
            }))))
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
}

impl AstVisit<Unresolved> for NestedSessionDirectiveGuard {
    fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
        if std::mem::take(&mut self.skip_root_relation) {
            return Ok(Descent::Continue);
        }
        if let Relation::FunctorCall { call, .. } = relation {
            let Some(reference) = Some(&call.call().callee) else {
                return Ok(Descent::Continue);
            };
            let name = reference.name_text();
            if crate::pipeline::asts::effects::kind_for_reference(reference)
                .is_some_and(|kind| kind.descriptor().category == DirectiveCategory::Session)
            {
                return Err(nested_session_directive_error(&name));
            }
        }
        Ok(Descent::Continue)
    }
}

fn nested_session_guard() -> NestedSessionDirectiveGuard {
    NestedSessionDirectiveGuard {
        skip_root_relation: false,
    }
}

/// Compile purity: find every demand the effect executor would EXECUTE, so a
/// pure inspection surface can refuse it cleanly before any mutation. The
/// executing positions mirror this module's
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

    fn is_bin_executable<S: AsRef<str>>(&self, ns: &[S], name: &str) -> bool {
        self.registry
            .lookup_qualified_entity(ns, name)
            .and_then(|entity| entity.as_effect_executable().map(|_| ()))
            .is_some()
    }
}

impl AstVisit<Unresolved> for ExecutingDemandGuard<'_> {
    fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
        match relation {
            Relation::FunctorCall { call, .. } => {
                if let Some(reference) = Some(&call.call().callee) {
                    if reference.name_text().ends_with('!') {
                        return Err(self.refuse(&reference.name_text()));
                    }
                }
                Ok(Descent::Continue)
            }
            Relation::Ground {
                mention: GroundMention::Named { identifier, .. },
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
            _ => Ok(Descent::Continue),
        }
    }
}

/// Refuse every demand the effect executor would execute, for a pure inspection of
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

fn refuse_nested_session_directives_in_relational(expr: &Chain) -> Result<()> {
    walk_visit_relational(&mut nested_session_guard(), expr)?;
    Ok(())
}

fn refuse_nested_session_directives_in_domain(expression: &DomainExpression) -> Result<()> {
    walk_visit_domain(&mut nested_session_guard(), expression)?;
    Ok(())
}

fn refuse_nested_session_directives_in_boolean(condition: &TruthExpression) -> Result<()> {
    walk_visit_boolean(&mut nested_session_guard(), condition)?;
    Ok(())
}

fn refuse_nested_session_directives_in_operator(operator: &PipeOp) -> Result<()> {
    let mut guard = nested_session_guard();
    walk_visit_operator(&mut guard, operator)?;
    Ok(())
}
fn execute_effects_in_read(
    relation: Relation,
    access: Option<Access>,
    locals: &crate::pipeline::asts::core::QueryLocals<Unresolved>,
    system: &mut DelightQLSystem,
    registry: &Rc<Registry>,
) -> Result<Chain> {
    let carried = access.clone();
    let restore = |head: GroundForm| match carried.clone() {
        Some(access) => Chain::read_head(head, access),
        None => Chain::authored(head),
    };
    // The relation itself is on the executable source spine; all fields below
    // it are data positions. A call HOLDING a source is the collapsed source
    // application, so that argument is executable source rather than a
    // nested authored query; execute_functor_call owns that boundary.
    let collapsed_pipe = match &relation {
        Relation::FunctorCall { call, .. } => call.call().arguments.judged()?.landed().is_some(),
        _ => false,
    };
    if !collapsed_pipe {
        let mut guard = nested_session_guard();
        guard.skip_root_relation = true;
        walk_visit_relation(&mut guard, &relation)?;
    }

    match relation {
        Relation::FunctorCall { call, alias, .. } => execute_functor_call(
            call,
            alias,
            carried.clone().unwrap_or(Access::Unasked),
            locals,
            system,
            registry,
        )
        .map(|executed| spend_receipt(executed, carried.clone())),

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
                    let rewritten_subquery = Box::new(execute_effects_in_expression(
                        *subquery, locals, system, registry,
                    )?);
                    InnerRelationPattern::Indeterminate {
                        identifier,
                        subquery: rewritten_subquery,
                    }
                }
                // Other patterns are classified later, no need to handle here
                other => other,
            };
            Ok(restore(GroundForm::Reference(Relation::InnerRelation {
                pattern: rewritten_pattern,
                alias,
                outer,
            })))
        }

        // Check if a Ground relation is a namespace-qualified bin entity
        // (e.g., sys::execution.compile("stage", "source"))
        Relation::Ground {
            mention:
                GroundMention::Named {
                    ref identifier,
                    ref alias,
                    ..
                },
            ..
        } if !identifier.namespace_path.is_empty() && access.is_some() => {
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
                    let arguments = match access.as_ref().expect("guarded above") {
                        // A namespaced bin relation writes its ARGUMENTS in
                        // the parens, and EVERY slot is one. A slot this
                        // executable cannot take is refused in its own
                        // position — dropping it would hand the executable a
                        // shorter row and silently promote each later
                        // argument one place left.
                        Access::Slots(slots) => slots
                            .iter()
                            .enumerate()
                            .map(|(index, slot)| {
                                slot.term().ok_or_else(|| {
                                    DelightQLError::validation_error_categorized(
                                        "effect/bin/valueless_argument",
                                        format!(
                                            "'{}' was given a slot that supplies no value at \
                                             argument {}; this bin relation takes values there",
                                            identifier.name,
                                            index + 1
                                        ),
                                        "write the value the argument names",
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                        _ => {
                            return Err(DelightQLError::database_error(
                                format!(
                                    "Bin relation '{}' requires positional arguments",
                                    identifier.name
                                ),
                                "Invalid access for bin relation",
                            ))
                        }
                    };
                    let alias_str = alias.as_ref().map(|s| s.to_string());
                    system.note_effect_executed();
                    let result = executable.execute(&arguments, alias_str, system)?;
                    let crate::bin_cartridge::EntityResult::Relation(r) = result;
                    return Ok(Chain::authored(r));
                }
            }
            // Not a bin entity. A QUALIFIED consulted wrapper of a
            // runtime-served relation is the same call the bare spelling
            // makes, so it reaches the same pre-resolution expansion; only
            // if it is not such a view does the resolver receive it.
            let instances = crate::defuse::instance::InstanceTable::default();
            // The pre-splice reads the catalog under one shared borrow; the
            // executing walk below takes the exclusive borrow only after
            // the expanded chain is in hand.
            match expand_runtime_served_view(identifier, &*system, &instances)? {
                Some(expanded) => {
                    let expanded =
                        execute_effects_in_expression(expanded, locals, system, registry)?;
                    Ok(match carried {
                        Some(access) => expanded.then(Step::authored(Continuation::Access {
                            access,
                            named: Default::default(),
                        })),
                        None => expanded,
                    })
                }
                None => Ok(restore(GroundForm::Reference(relation))),
            }
        }

        // All other relation types don't contain pseudo-predicates
        // A CONSULTED DEFINITION WRAPPING A RUNTIME-SERVED RELATION is the
        // same call the top-level spelling makes, so it reaches the same
        // execution road: the boundary expands the definition here — before
        // resolution, where the runtime is still reachable — and the
        // ordinary walk below executes the served call the body carries.
        // Anything the expansion cannot carry (an alias, an outer read, a
        // multi-clause or parameterized definition, a view reaching the
        // served relation only through another view) is left standing, and
        // the resolver's fail-closed fence keeps it out of the generic-TVF
        // fallback.
        Relation::Ground {
            mention:
                GroundMention::Named {
                    ref identifier,
                    alias: None,
                    ..
                },
            outer: false,
            ..
        } => {
            let instances = crate::defuse::instance::InstanceTable::default();
            // The pre-splice reads the catalog under one shared borrow; the
            // executing walk below takes the exclusive borrow only after
            // the expanded chain is in hand.
            match expand_runtime_served_view(identifier, &*system, &instances)? {
                Some(expanded) => {
                    let expanded =
                        execute_effects_in_expression(expanded, locals, system, registry)?;
                    Ok(match carried {
                        Some(access) => expanded.then(Step::authored(Continuation::Access {
                            access,
                            named: Default::default(),
                        })),
                        None => expanded,
                    })
                }
                None => Ok(restore(GroundForm::Reference(relation))),
            }
        }

        _ => Ok(restore(GroundForm::Reference(relation))),
    }
}

/// Expand a reference to a consulted view whose body DIRECTLY references a
/// runtime-served bin relation, transitively pre-splicing any such
/// references the body itself carries so the executing walk meets none it
/// has not already expanded. `None` = not such a view, or a shape the
/// expansion does not carry (the resolver's fence answers those).
fn expand_runtime_served_view(
    identifier: &crate::pipeline::asts::core::QualifiedName,
    system: &DelightQLSystem,
    instances: &crate::defuse::instance::InstanceTable,
) -> Result<Option<Chain>> {
    let namespace_fq =
        (!identifier.namespace_path.is_empty()).then(|| identifier.namespace_path.to_string());
    crate::defuse::bound_use::use_runtime_served_view(
        system,
        &identifier.name,
        namespace_fq.as_deref(),
        identifier.name.as_str(),
        instances,
        |system, body, instances| pre_expand_runtime_served(body, system, instances),
    )
}

/// Pre-splice every expandable reference the body carries, so the walk that
/// executes the composed chain meets only already-expanded relations.
fn pre_expand_runtime_served(
    chain: Chain,
    system: &DelightQLSystem,
    instances: &crate::defuse::instance::InstanceTable,
) -> Result<Chain> {
    match chain.head().form() {
        GroundForm::Reference(Relation::Ground {
            mention:
                GroundMention::Named {
                    ref identifier,
                    alias: None,
                    ..
                },
            outer: false,
            ..
        }) => match expand_runtime_served_view(identifier, system, instances)? {
            Some(expanded) => {
                let mut expanded = expanded;
                let mut chain = chain;
                let tail = std::mem::take(chain.continuations_mut());
                expanded.continuations_mut().extend(tail);
                return pre_expand_continuations(expanded, system, instances);
            }
            None => (),
        },
        _ => (),
    }
    pre_expand_continuations(chain, system, instances)
}

/// The continuation halves of the pre-splice: members and pipe operands may
/// carry references of their own.
fn pre_expand_continuations(
    mut chain: Chain,
    system: &DelightQLSystem,
    instances: &crate::defuse::instance::InstanceTable,
) -> Result<Chain> {
    for continuation in chain.continuations_mut() {
        if let Continuation::Member { rhs, .. } = continuation.form_mut() {
            let expanded = pre_expand_runtime_served(rhs.clone(), system, instances)?;
            *rhs = expanded;
        }
    }
    Ok(chain)
}

/// A RECEIPT IS SPENT BY THE EXECUTION THAT PRODUCES IT. When a directive
/// runs here its result already carries the heading the receipt named, so the
/// access does not stand again over that result. A call the executor left
/// alone is an ordinary relation and keeps what its parens asked for.
fn spend_receipt(executed: GroundForm, receipt: Option<Access>) -> Chain {
    match (&executed, receipt) {
        (GroundForm::Reference(Relation::FunctorCall { .. }), Some(access)) => {
            Chain::read_head(executed, access)
        }
        _ => Chain::authored(executed),
    }
}
fn execute_functor_call(
    mut call: crate::pipeline::asts::core::SealedCall,
    // The name the READ answers to. A directive's receipt relation is named
    // where the read stands, not by the call it stands on.
    alias: Option<delightql_types::SqlIdentifier>,
    receipt: Access,
    locals: &crate::pipeline::asts::core::QueryLocals<Unresolved>,
    system: &mut DelightQLSystem,
    registry: &Rc<Registry>,
) -> Result<GroundForm> {
    let (name, namespace, effect_mark) = match &call.call().callee {
        reference => (
            reference.name_text(),
            reference.namespace_texts(),
            matches!(
                reference.mark(),
                crate::pipeline::asts::vocabulary::Mark::Effect
            ),
        ),
    };

    use crate::pipeline::asts::core::operators::{CallArguments, ScalarArgument};
    let mut table_arguments = Vec::new();
    let mut scalar_arguments = Vec::new();
    match &call.call().arguments {
        CallArguments::None => {}
        CallArguments::HigherOrder(part) => {
            for argument in part.members().iter() {
                match argument {
                    HoArgument::Relation(relation)
                    | HoArgument::Rule(relation)
                    | HoArgument::Landed(relation) => table_arguments.push(relation.clone()),
                    HoArgument::Value(value) => {
                        scalar_arguments.push(TerminalArg::Value(value.value.clone()))
                    }
                    // The row marks supply no value: a landing stands for a
                    // relation nothing has piped yet, and a skip names
                    // nothing. This boundary serves pure calls too, so the
                    // marks pass here and the resolver's own landing
                    // authority refuses the unspent one with its teaching.
                    HoArgument::Landing(_) | HoArgument::Skip => {}
                }
            }
        }
        CallArguments::Scalar(members) => {
            for member in members {
                match member {
                    ScalarArgument::Value(value) => {
                        scalar_arguments.push(TerminalArg::Value(value.value.clone()))
                    }
                    // A CALLABLE IS NOT A DIRECTIVE'S PARAMETER. A
                    // namespace, a path or a flag is a value; a form with an
                    // open slot is not one, so it is refused where it is
                    // read. Neither is the context-mode marker.
                    ScalarArgument::Callable(_) | ScalarArgument::Context(_) => {
                        return Err(DelightQLError::validation_error_categorized(
                            "effect/directive/valueless_argument",
                            "a directive's argument is a value; a callable or a context \
                             marker is not one",
                            "write the namespace, path, or flag the directive names",
                        ))
                    }
                    // THE GLOB IS THE WHOLE ROW: it enumerates the source's
                    // values in header order rather than naming one.
                    ScalarArgument::Spread(_) | ScalarArgument::Star => {
                        scalar_arguments.push(TerminalArg::WholeRow)
                    }
                }
            }
        }
    }

    // Pure bin relations use the same lifted application boundary as
    // directives.  A pipe substitutes its source into a source-role argument,
    // so route that common shape through the executable's typed wrapper
    // before the ordinary relation resolver can mistake it for a TVF.
    let landed = call.call().arguments.judged()?.landed().is_some();
    if table_arguments.len() == 1 && !effect_mark && landed {
        let entity = if namespace.is_empty() {
            system.bin_registry().lookup_entity(&name)
        } else {
            let ns: Vec<&str> = namespace.iter().map(String::as_str).collect();
            system.bin_registry().lookup_qualified_entity(&ns, &name)
        };
        if let Some(entity) = entity {
            if let Some(executable) = entity.as_effect_executable() {
                let source = table_arguments
                    .pop()
                    .expect("one table argument exists for lifted bin execution");
                return execute_bin_entity_pipe(source, executable, locals, system, registry)
                    .and_then(
                        |result| match result.into_bare_head().map(Grelex::into_form) {
                            Some(head) => Ok(head),
                            None => Err(DelightQLError::database_error(
                                format!("bin relation '{}' did not produce a relation", name),
                                "Bin relation result",
                            )),
                        },
                    );
            }
        }
    }

    // A direct pure-bin invocation consumes scalar arguments. A table-valued
    // argument is an arity/shape error at this boundary; it must not be
    // mistaken for a lifted pipe and expanded into source-row arguments.
    if !effect_mark && !landed {
        let entity = if namespace.is_empty() {
            system.bin_registry().lookup_entity(&name)
        } else {
            let ns: Vec<&str> = namespace.iter().map(String::as_str).collect();
            system.bin_registry().lookup_qualified_entity(&ns, &name)
        };
        if entity.is_some_and(|entity| entity.as_effect_executable().is_some())
            && !table_arguments.is_empty()
        {
            let identity = if namespace.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", namespace.join("::"), name)
            };
            scalar_bin_arguments(&identity, &call.call().arguments)?;
        }
    }

    if table_arguments.len() == 1 && effect_mark {
        let result = execute_directive_pipe(
            table_arguments.pop().expect("one table argument exists"),
            &name,
            &namespace,
            &scalar_arguments,
            locals,
            system,
            registry,
        )?;
        return match result.into_bare_head() {
            Some(head) => Ok(head.into_form()),
            None => Err(DelightQLError::database_error(
                format!("effect call '{}' did not produce a relation", name),
                "Effect call result",
            )),
        };
    }

    let has_table_arguments = !table_arguments.is_empty();
    if has_table_arguments {
        // WHAT A POSITION CARRIES, not what it is: executing the effects in
        // a relation replaces the relation, and a landed member stays landed.
        call.call_mut().arguments.rewrite_relations(|source| {
            execute_effects_in_expression(source.clone(), locals, system, registry)
        })?;
    }

    if table_arguments.is_empty() {
        if !namespace.is_empty() {
            let ns: Vec<&str> = namespace.iter().map(String::as_str).collect();
            if let Some(entity) = system.bin_registry().lookup_qualified_entity(&ns, &name) {
                if let Some(executable) = entity.as_effect_executable() {
                    let identity = format!("{}.{}", namespace.join("::"), name);
                    let arguments = scalar_bin_arguments(&identity, &call.call().arguments)?;
                    system.note_effect_executed();
                    let result =
                        executable.execute(&arguments, alias.map(|a| a.to_string()), system)?;
                    let crate::bin_cartridge::EntityResult::Relation(relation) = result;
                    return Ok(relation);
                }
            }
        }

        if effect_mark {
            let relation = execute_pseudo_predicate(
                &name,
                &namespace,
                &TerminalArg::values(&scalar_arguments),
                &receipt,
                alias.map(|a| a.to_string()),
                system,
            )?;
            return Ok(relation);
        }
    }

    Ok(GroundForm::Reference(Relation::FunctorCall { call, alias }))
}

/// A directive's parameter is a value — a namespace, a path, a flag. A truth
/// read as a value is not one of those, and it says so here.
fn scalar_bin_arguments(
    identity: &str,
    arguments: &crate::pipeline::asts::core::operators::CallArguments,
) -> Result<Vec<DomainExpression>> {
    use crate::pipeline::asts::core::operators::ScalarArgument;
    let mut scalars = Vec::new();
    for (index, member) in arguments.scalar_members().iter().enumerate() {
        match member {
            ScalarArgument::Value(value) => scalars.push(value.value.clone()),
            // The access glob is how a demand spells "whole"; it supplies no
            // parameter and is counted as none. The context marker selects a
            // calling mode and supplies none either.
            ScalarArgument::Callable(_)
            | ScalarArgument::Spread(_)
            | ScalarArgument::Star
            | ScalarArgument::Context(_) => {}
        }
        let _ = index;
    }
    for (index, argument) in arguments.ho_members().enumerate() {
        match argument {
            HoArgument::Value(value) => scalars.push(value.value.clone()),
            HoArgument::Landing(_) | HoArgument::Skip => {}
            HoArgument::Relation(_) | HoArgument::Rule(_) | HoArgument::Landed(_) => {
                return Err(DelightQLError::validation_error_categorized(
                    "effect/bin/table_argument",
                    format!(
                        "{identity} received a table-valued argument at position {}; \
                         bin executables consume scalar arguments in this call shape. \
                         The table argument cannot be discarded or shift the later \
                         arguments. Pass scalar expressions in the first parentheses.",
                        index + 1
                    ),
                    "bin executable argument shape",
                ));
            }
        }
    }
    Ok(scalars)
}

#[cfg(test)]
mod bin_argument_tests {
    use super::*;

    fn string(value: &str) -> DomainExpression {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(
            value.to_string(),
        )))
    }

    fn table() -> Chain {
        Chain::authored(GroundForm::Literal(AnonRelation::plain(
            AnonTable::from_values(None, vec![vec![string("receipt")]]).unwrap(),
        )))
    }

    #[test]
    fn bin_arguments_preserve_every_scalar_or_refuse_the_table_in_place() {
        use crate::pipeline::asts::core::operators::CallArguments;
        let scalars = CallArguments::higher_order(vec![
            HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(string(
                "stage",
            ))),
            HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(string(
                "source",
            ))),
        ]);
        let preserved = scalar_bin_arguments("sys::execution.compile", &scalars).unwrap();
        assert_eq!(preserved, vec![string("stage"), string("source")]);

        let mixed = CallArguments::higher_order(vec![
            HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(string(
                "stage",
            ))),
            HoArgument::Relation(table()),
            HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(string(
                "source",
            ))),
        ]);
        let error = scalar_bin_arguments("sys::execution.compile", &mixed).unwrap_err();
        assert_eq!(
            error.error_uri(),
            "delightql-error://semantic/effect/bin/table_argument"
        );
        assert!(error.to_string().contains("position 2"));
    }
}

/// Execute a directive pipe: source |> terminal!(args)
///
/// THE SESSION-CHAIN DISPATCHER: top-level session orchestration lives
/// here deliberately — effect execution is the legal path for session
/// directives at the REPL/CLI top level. Set-at-a-time: one lifted call,
/// never a row loop. Its stringly binding (`bind_directive_args`) remains
/// the fenced interim seam, retired when `execute_lifted` takes a typed
/// relation — the typed-program consolidation step.
///
/// 1. Execute the source expression (recursively handles chained pipes)
/// 2. Extract rows from the source (anonymous fast path, or full pipeline)
/// 3. For each row, bind the terminal arguments and execute the terminal directive
/// 4. Combine all results into a single Anonymous relation
fn execute_directive_pipe(
    source: Chain,
    terminal_name: &str,
    terminal_namespace: &[String],
    terminal_args: &[TerminalArg],
    locals: &crate::pipeline::asts::core::QueryLocals<Unresolved>,
    system: &mut DelightQLSystem,
    registry: &Rc<Registry>,
) -> Result<Chain> {
    // Piping a WHOLE receipt where a directive's argumentative functor
    // expects its PAYLOAD is a shape error, taught as such. Detected
    // structurally, before anything executes: the source is itself a
    // directive invocation (its value is a receipt) and the receipt's
    // declared width does not match the terminal's parameter list. Pinned
    // by directive_contract 34_bare_receipt_chain_refused.
    if let Some(Relation::FunctorCall {
        call: source_call, ..
    }) = source.as_read_relation()
    {
        if let Some(source_reference) = Some(&source_call.call().callee) {
            let source_name = source_reference.name_text();
            let source_ns = source_reference.namespace_texts();
            let bare = terminal_name.strip_suffix('!').unwrap_or(terminal_name);
            let terminal_qualifier =
                (!terminal_namespace.is_empty()).then(|| terminal_namespace.join("::"));
            let terminal_arity = crate::pipeline::asts::effects::DirectiveKind::select_identity(
                terminal_name,
                terminal_qualifier.as_deref(),
            )
            .map(|kind| kind.descriptor().params.len());
            let source_entity = if source_ns.is_empty() {
                system.bin_registry().lookup_entity(&source_name)
            } else {
                system
                    .bin_registry()
                    .lookup_qualified_entity(&source_ns, &source_name)
            };
            let receipt_width = source_entity
                .and_then(|e| match e.signature().output_schema {
                    crate::bin_cartridge::OutputSchema::Relation(cols) => Some(cols.len()),
                    crate::bin_cartridge::OutputSchema::Void => None,
                })
                .or_else(|| {
                    crate::pipeline::asts::effects::descriptor_for_reference(source_reference)
                        .map(|descriptor| descriptor.receipt_columns().len())
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
    }

    // 1. Execute source (recursively handles chained directive pipes and pseudo-predicates)
    let executed_source = execute_effects_in_expression(source, locals, system, registry)?;

    // 2. Extract rows — fast path for anonymous, full pipeline for anything else
    let (headers, rows) = extract_rows(executed_source, locals, system)?;

    // 3. ONE set-at-a-time application: a piped relation is one demand of
    // the terminal's argumentative functor — the lifted rows bind once and
    // the entity executes ONCE, never in a rowwise loop:
    //
    //   - doc! (the first setwise override) receives the whole lifted
    //     relation and answers ONE receipt (directive_contract 38);
    //   - a ONE-row lift takes the scalar path (execute_pseudo_predicate,
    //     with its descriptor arity and receipt-access discipline) — the
    //     shape every pinned session chain uses;
    //   - a MULTI-row lift to any non-overriding terminal refuses with
    //     not-yet (directive_contract 37) — the refusal lives in
    //     EffectExecutable::execute_lifted's default, uniformly for every
    //     category;
    //   - an EMPTY lift is a no-op.
    let terminal_qualifier =
        (!terminal_namespace.is_empty()).then(|| terminal_namespace.join("::"));
    let builtin = crate::pipeline::asts::effects::DirectiveKind::select_identity(
        terminal_name,
        terminal_qualifier.as_deref(),
    );
    let descriptor = builtin.map(|kind| kind.descriptor());
    let bound_rows: Vec<Vec<DomainExpression>> = rows
        .iter()
        .map(|row_values| bind_directive_args(descriptor, &headers, row_values, terminal_args))
        .collect::<Result<_>>()?;

    let result = if builtin == Some(crate::pipeline::asts::effects::DirectiveKind::Doc)
        || bound_rows.len() != 1
    {
        let entity = (if terminal_namespace.is_empty() {
            system.bin_registry().lookup_entity(terminal_name)
        } else {
            let namespace = terminal_namespace
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            system
                .bin_registry()
                .lookup_qualified_entity(&namespace, terminal_name)
        })
        .ok_or_else(|| {
                // A DECLARED identity whose absence here is a POLICY answers
                // with the effect machinery's own voice, never a lookup that
                // denies a name the same session reflects in full detail.
                if let Some(desc) = descriptor {
                    return DelightQLError::validation_error_categorized(
                        "effect/realization/context",
                        format!(
                            "'{terminal_name}' is a declared directive with no standalone \
                             statement realization ({}): it executes where its category \
                             places it — inside a run's effect body — and this position \
                             is not one",
                            match desc.realization {
                                crate::pipeline::asts::effects::DirectiveRealization::Entity =>
                                    "its entity form takes different arguments here",
                                crate::pipeline::asts::effects::DirectiveRealization::SyntaxPipeTerminal =>
                                    "a syntax pipe terminal has no callable entity by policy",
                                crate::pipeline::asts::effects::DirectiveRealization::LiminalOnly =>
                                    "it is legal only in a consulted file's liminal space",
                            }
                        ),
                        "directive out of its realization context",
                    );
                }
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
            terminal_namespace,
            &bound_rows[0],
            &crate::pipeline::asts::core::Access::All,
            None,
            system,
        )?
    };

    Ok(Chain::authored(result))
}

/// Execute a bin entity in a piped context: source |> ns::entity(*)
///
/// 1. Execute the source expression
/// 2. Extract rows (anonymous fast path, or full pipeline for any other source)
/// 3. For each row, execute the bin entity with that row's values as arguments
/// 4. Combine all results into a single Anonymous relation
fn execute_bin_entity_pipe(
    source: Chain,
    executable: &dyn EffectExecutable,
    locals: &crate::pipeline::asts::core::QueryLocals<Unresolved>,
    system: &mut DelightQLSystem,
    registry: &Rc<Registry>,
) -> Result<Chain> {
    let executed_source = execute_effects_in_expression(source, locals, system, registry)?;
    let (_headers, rows) = extract_rows(executed_source, locals, system)?;

    // ONE set-at-a-time application: an EMPTY source is not refused here —
    // pipe is application, so the lift reaches the entity once regardless
    // of cardinality; execute_lifted (or its override) owns the semantics.
    // The lifted default delegates one-row lifts to the scalar execute
    // and refuses multi-row lifts with not-yet; a setwise entity (doc!)
    // receives the whole relation and answers one receipt.
    system.note_effect_executed();
    let crate::bin_cartridge::EntityResult::Relation(r) =
        executable.execute_lifted(&rows, None, system)?;
    Ok(Chain::authored(r))
}

/// Extract rows from a source expression.
///
/// Fast path: if the source is an Anonymous relation, extract rows directly.
/// Otherwise: wrap in a Query, compile through the full pipeline to SQL,
/// execute against the database, and convert result rows to DomainExpressions.
/// This allows ANY query (filtered, joined, CTE, actual table) to be piped
/// into bin entities and directives.
fn extract_rows(
    expr: Chain,
    locals: &crate::pipeline::asts::core::QueryLocals<Unresolved>,
    system: &mut DelightQLSystem,
) -> Result<(Vec<String>, Vec<Vec<DomainExpression>>)> {
    // Fast path: anonymous table literal — extract rows directly from AST
    if let Ok(result) = extract_anonymous_rows(&expr) {
        return Ok(result);
    }

    // Full pipeline path: compile the source to SQL and execute it
    let query = Query::binding(locals.clone(), expr);

    // A SEPARATE COMPILATION GETS A SEPARATE ARENA. The source is compiled
    // and EXECUTED here, whole, and a compilation that finishes seals its
    // semantic epoch — sharing the caller's arena would seal the very
    // compilation that is still building the relation these rows feed. The
    // rows cross as values, not as identities, so nothing is lost by the
    // separation: the unresolved chain carries no occurrence, and the armed
    // budgets come from the compilation executing now.
    let mut pipeline = Pipeline::new_from_unresolved_query(
        query,
        system,
        crate::relation::Planning::open(crate::names::Registry::new(&[])),
    );
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

    let (col_names, value_rows) = conn.query_all_rows(&sql, &[]).map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to execute pipe source query: {}", e),
            "Pipe source execution",
        )
    })?;

    // Every materialized cell becomes a string literal, NULL included: what
    // a pipe source's values are — and whether a null may be one — is a
    // domain-expression question, not a carrier question.
    let rows: Vec<Vec<DomainExpression>> = value_rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|val| {
                    DomainExpression::Application(FunctionApplication::Ground(
                        LiteralValue::String(pipe_source_cell_text(val)),
                    ))
                })
                .collect()
        })
        .collect();

    Ok((col_names, rows))
}

/// The text a materialized pipe-source cell becomes.
///
/// The ONLY place a database value's nullability is spent on a string
/// rather than carried: a pipe source has no null literal to become, so a
/// null arrives here as the four characters `NULL` and is thereafter
/// indistinguishable from text that spells them. Giving it one is a
/// domain-expression ruling, not a carrier repair — every other road out
/// of a database keeps absence absent.
fn pipe_source_cell_text(value: delightql_types::DbValue) -> String {
    use delightql_types::DbValue;
    match value {
        DbValue::Null => "NULL".to_string(),
        DbValue::Integer(i) => i.to_string(),
        DbValue::Real(f) => f.to_string(),
        DbValue::Text(s) => s,
        DbValue::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}

/// Extract column headers and row values from an Anonymous relation
fn extract_anonymous_rows(expr: &Chain) -> Result<(Vec<String>, Vec<Vec<DomainExpression>>)> {
    match (expr.head().form(), expr.continuations().is_empty()) {
        (GroundForm::Literal(anon), true) => {
            let column_headers = &anon.table.body.header;
            let rows = &anon.table.body.rows;
            // Extract header names from domain expressions
            let headers: Vec<String> = match column_headers {
                Some(exprs) => exprs
                    .iter()
                    .map(|item| match item.term() {
                        Some(e) => match e {
                            DomainExpression::Reference(Reference::Named(NamedReference(
                                AuthoredColumn { name, .. },
                            ))) => name.to_string(),
                            DomainExpression::Application(FunctionApplication::Ground(
                                LiteralValue::String(s),
                            )) => s.clone(),
                            _ => format!("{:?}", e),
                        },
                        None => "_".to_string(),
                    })
                    .collect(),
                None => {
                    // No headers — generate positional names
                    (0..rows.first().len())
                        .map(|i| format!("col{}", i))
                        .collect()
                }
            };

            let row_values = rows
                .iter()
                .map(|row| row.iter().map(Datum::value).collect())
                .collect();

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
/// THE PIPE SUPPLIES THE PARAMETERS. A piped call authors no arguments —
/// `q |> f!(access)` is `q |> f!()(access)`, and the lone group is receipt
/// access — so an empty argument list means the source row supplies them, in
/// its own order. That is what the pipe IS; a row bound against nothing would
/// hand the entity a zero-column call.
///
/// When arguments ARE authored (`q |> f!(a, b)(access)`), each binds:
/// - Glob (*) → expand to all column values from the row (in header order)
/// - Lvar in an ordinary value position → look up that column in the row
/// - Lvar in a descriptor-declared target position → keep the target name
/// - Literal → pass through unchanged
/// One argument of a directive terminal, as the lifted binder reads it.
enum TerminalArg {
    Value(DomainExpression),
    /// `f!(*)` — the source row's values, in header order.
    WholeRow,
}

impl TerminalArg {
    /// The values a row supplies. The whole-row glob supplies none: it
    /// enumerates the source rather than naming a parameter.
    fn values(arguments: &[Self]) -> Vec<DomainExpression> {
        arguments
            .iter()
            .filter_map(|argument| match argument {
                Self::Value(value) => Some(value.clone()),
                Self::WholeRow => None,
            })
            .collect()
    }
}

fn bind_directive_args(
    descriptor: Option<&crate::pipeline::asts::effects::DirectiveDescriptor>,
    headers: &[String],
    row_values: &[DomainExpression],
    terminal_args: &[TerminalArg],
) -> Result<Vec<DomainExpression>> {
    if terminal_args.is_empty() {
        return Ok(row_values.to_vec());
    }
    let mut bound = Vec::new();

    for (position, arg) in terminal_args.iter().enumerate() {
        match arg {
            TerminalArg::WholeRow => bound.extend(row_values.iter().cloned()),
            TerminalArg::Value(DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    namespace_path,
                    ..
                },
            )))) if namespace_path.is_empty()
                && !descriptor
                    .and_then(|descriptor| descriptor.params.get(position))
                    .is_some_and(|param| {
                        param.kind
                            == crate::pipeline::asts::effects::DirectiveParamKind::RelationTarget
                    }) =>
            {
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
            TerminalArg::Value(value) => bound.push(value.clone()),
        }
    }

    Ok(bound)
}

/// Execute a specific pseudo-predicate and return its result as an inline table
///
/// Resolution: identity is
/// (namespace, name). Qualified invocations resolve their spelled path
/// exactly; unqualified invocations reach only universal-visibility
/// namespaces. Contextual absences refuse by DESCRIPTOR POLICY with a
/// teaching diagnostic, never with a bare "unknown" error, and the
/// descriptor's arity is authoritative before the entity runs.
fn execute_pseudo_predicate(
    name: &str,
    namespace: &[String],
    arguments: &[DomainExpression],
    access: &crate::pipeline::asts::core::Access,
    alias: Option<String>,
    system: &mut DelightQLSystem,
) -> Result<GroundForm> {
    use crate::pipeline::asts::effects::{DirectiveKind, DirectiveRealization};

    let ns_strs: Vec<&str> = namespace.iter().map(|s| s.as_str()).collect();
    let qualifier = (!namespace.is_empty()).then(|| namespace.join("::"));
    let descriptor =
        DirectiveKind::select_identity(name, qualifier.as_deref()).map(|kind| kind.descriptor());
    let local_descriptor = DirectiveKind::from_name(name).map(|kind| kind.descriptor());
    let entity = if ns_strs.is_empty() {
        system.bin_registry().lookup_entity(name)
    } else {
        system
            .bin_registry()
            .lookup_qualified_entity(&ns_strs, name)
    };

    let entity = entity.ok_or_else(|| {
        let bare = name.strip_suffix('!').unwrap_or(name);
        // Descriptor policy: an intentional contextual absence refuses with
        // a rule-citing diagnostic, never a registration accident.
        if let Some(desc) = descriptor {
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
        if let Some(desc) = local_descriptor
            .filter(|descriptor| descriptor.realization == DirectiveRealization::Entity)
        {
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
        // Do not point end users at "register it in a bin cartridge" —
        // that is a compiler-internal mechanism, and naming it here reads
        // as "USER effect rules are unimplemented" (a false negative about
        // a shipped, load-bearing feature).
        //
        // A user effect rule IS demandable directly — THE IMPLICIT RUN: a
        // prompt statement is an implicit run. Reaching this arm therefore
        // means the name resolved to nothing at all, so the refusal must not
        // suggest that direct demand is the problem.
        DelightQLError::validation_error_categorized(
            "directive/unknown",
            format!(
                "Unknown directive '{}'. If this is YOUR effect rule, it is \
                 not in scope here: consult! the file that defines it, and demand \
                 it under the namespace it was consulted into. Otherwise, check \
                 the spelling against the built-in directives (EFFECT-ALGEBRA §3).",
                if namespace.is_empty() {
                    format!("{bare}!")
                } else {
                    format!("{}.{bare}!", namespace.join("::"))
                }
            ),
            "unknown directive",
        )
    })?;
    // Registry borrow ends here, but Arc keeps entity alive

    // EFFECT DISCIPLINE: a rejected program must not leave behind the
    // effect whose result it failed to bind. The receipt access is read
    // against the entity's DECLARED output schema BEFORE execution — never
    // discovered by executing first.
    let binding = read_receipt_access(access, descriptor, &entity.signature().output_schema, name)?;

    // Descriptor arity is AUTHORITATIVE for binding. Receipt access is
    // judged first because the grammar assigns a lone group to that role:
    // its teaching must name the group the author can see before reporting
    // the consequently empty ordinary-argument list.
    if let Some(desc) = descriptor {
        desc.judge_argument_arity(arguments.len())?;
    }

    // Downcast to EffectExecutable
    let executable = entity.as_effect_executable().ok_or_else(|| {
        DelightQLError::database_error(
            format!(
                "Entity '{}' is not executable in the effect executor. \
                 Only entities implementing EffectExecutable can be executed here.",
                name
            ),
            "Not an effect-executable entity",
        )
    })?;

    // Now we can execute with a mutable borrow of system
    system.note_effect_executed();
    let result = executable.execute(arguments, alias, system)?;

    // Convert EntityResult to Relation, then bind the receipt access read
    // above (the SECOND parentheses — the canonical invocation:
    // parameters first, receipt access second).
    let crate::bin_cartridge::EntityResult::Relation(relation) = result;
    bind_receipt(relation, binding, name, &entity.signature().output_schema)
}

/// What a receipt access asks of a directive's declared receipt.
enum ReceiptBinding {
    /// `(*)` / `()` — the receipt exactly as the directive declares it.
    Whole,
    /// An exact-arity binding list: the declared heading, renamed position
    /// by position.
    Rename(Vec<delightql_types::SqlIdentifier>),
}

/// Read a receipt access against the heading the entity DECLARES.
///
/// A receipt is not a directive-only dialect: `Access::is_whole` and
/// `Access::binders` are the same questions an ordinary call asks of its
/// access, and they are asked here exactly once. Reading happens BEFORE
/// the effect runs, so a malformed access can never consult/enlist/mount
/// first and fail second.
fn read_receipt_access(
    access: &crate::pipeline::asts::core::Access,
    descriptor: Option<&crate::pipeline::asts::effects::DirectiveDescriptor>,
    schema: &crate::bin_cartridge::OutputSchema,
    directive_name: &str,
) -> Result<ReceiptBinding> {
    if let Some(descriptor) = descriptor {
        descriptor.judge_receipt_access(access)?;
        return Ok(match access.binders() {
            Some(binders) => ReceiptBinding::Rename(
                binders
                    .into_iter()
                    .map(|binder| binder.name.clone())
                    .collect(),
            ),
            None => ReceiptBinding::Whole,
        });
    }
    if access.is_whole() {
        return Ok(ReceiptBinding::Whole);
    }
    let Some(binders) = access.binders() else {
        return Err(DelightQLError::validation_error_categorized(
            "directive/invocation/access",
            format!(
                "receipt access on {directive_name} must be a positional \
                 binding list of plain names, got {access:?}"
            ),
            "receipt access",
        ));
    };
    match schema {
        crate::bin_cartridge::OutputSchema::Relation(cols) => {
            if binders.len() != cols.len() {
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
                        binders.len(),
                        cols.len()
                    ),
                    "receipt access",
                ));
            }
            Ok(ReceiptBinding::Rename(
                binders
                    .into_iter()
                    .map(|binder| binder.name.clone())
                    .collect(),
            ))
        }
        crate::bin_cartridge::OutputSchema::Void => {
            Err(DelightQLError::validation_error_categorized(
                "directive/invocation/access",
                format!(
                    "receipt access on {directive_name}: this entity declares no \
                     receipt columns — use (*)"
                ),
                "receipt access",
            ))
        }
    }
}

/// A binding list reached a Void-declaring entity: the reader refuses
/// that pairing, so arriving here means the reader and the binder saw
/// different schemas.
fn internal_receipt_error(directive_name: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "directive/invocation/access",
        format!(
            "receipt access on {directive_name}: this entity declares no \
             receipt columns — use (*)"
        ),
        "receipt access",
    )
}

/// Bind a read receipt access to the relation the directive returned.
///
/// The reading already happened, against the DECLARED heading; what is
/// left is the renaming, plus the one question the declaration cannot
/// answer — whether the entity returned the receipt it promised.
fn bind_receipt(
    relation: GroundForm,
    binding: ReceiptBinding,
    directive_name: &str,
    declared_schema: &crate::bin_cartridge::OutputSchema,
) -> Result<GroundForm> {
    let binder_names = match binding {
        ReceiptBinding::Whole => return Ok(relation),
        ReceiptBinding::Rename(names) => names,
    };

    // An interior-bearing receipt — the construction is an inner relation,
    // not an inline table — binds positionally by RENAMING through an
    // ordinary projection over the DECLARED heading — the reading already
    // matched the arity.
    if let GroundForm::Reference(Relation::InnerRelation { .. }) = &relation {
        let crate::bin_cartridge::OutputSchema::Relation(cols) = declared_schema else {
            return Err(internal_receipt_error(directive_name));
        };
        use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
        let renamed = Chain::authored(relation).then(Step::authored(Continuation::Pipe {
            operator: PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                    cols.iter()
                        .zip(binder_names.iter())
                        .map(|((declared, _), bound)| {
                            let expr = DomainExpression::lvar_builder(declared.clone()).build();
                            // The binder renames the receipt's column only when it
                            // differs; the same name republishes itself.
                            crate::pipeline::asts::core::OutItem::One(
                                crate::pipeline::asts::core::OneOut::authored(
                                    expr,
                                    (bound.as_str() != declared).then(|| bound.as_str().into()),
                                ),
                            )
                        })
                        .collect(),
                )
                .expect("a receipt's declared heading has at least one column"),
            ),
            named: None,
        }));
        return Ok(GroundForm::Reference(Relation::InnerRelation {
            pattern: InnerRelationPattern::Indeterminate {
                identifier: crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                    namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
                    name: directive_name.trim_end_matches('!').into(),
                },
                subquery: Box::new(renamed),
            },
            alias: None,
            outer: false,
        }));
    }

    let GroundForm::Literal(AnonRelation {
        table: AnonTable {
            body: TabularBody { header, rows },
            ..
        },
        alias,
        outer,
    }) = relation
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

    let receipt_width = header
        .as_ref()
        .map(|h| h.len())
        .unwrap_or_else(|| rows.first().len());
    if binder_names.len() != receipt_width {
        let heading = header
            .as_ref()
            .map(|hs| {
                hs.iter()
                    .filter_map(|h| match h.term() {
                        Some(DomainExpression::Reference(Reference::Named(NamedReference(
                            AuthoredColumn { name, .. },
                        )))) => Some(name.to_string()),
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
        .map(|n| HeaderItem {
            slot: Slot::classify(DomainExpression::lvar_builder(n).build()),
            sparse: false,
        })
        .collect::<Vec<_>>();
    let bound_headers = crate::pipeline::asts::core::TabularRow(Box::new(
        crate::pipeline::asts::vocabulary::Vec1::try_from_vec(bound_headers)
            .expect("a receipt access binds at least one column"),
    ));
    Ok(GroundForm::Literal(AnonRelation {
        table: AnonTable {
            body: TabularBody {
                header: Some(bound_headers),
                rows,
            },
        },
        alias,
        outer,
    }))
}
