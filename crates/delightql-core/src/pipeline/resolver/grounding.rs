// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Grounding support: function inlining and view expansion
//!
//! When a query uses the grounding operator (^), consulted definitions from
//! grounded namespaces are applied at the unresolved AST level before normal
//! resolution proceeds.
//!
//! **Function inlining**: `double:(x) :- x * 2` in namespace `lib::math` causes
//! `data::test^lib::math.users(*) |> (first_name, double:(balance) as doubled)` to become
//! `... |> (first_name, (balance * 2) as doubled)` before resolution.
//!
//! **View expansion**: `high_balance(*) :- users(*), balance > 1000` causes
//! `data::test^lib::views.high_balance(*)` to expand into the view body with
//! unqualified table references patched to use the data namespace.

use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::{
    walk_transform_domain, walk_transform_inner_relation, walk_transform_relation, AstTransform,
};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::metadata::GroundedPath;
use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::{
    CfeDefinition, DomainExpression, FunctionApplication, Relation, Unresolved,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::ddl::{Clause, DefKind, HeadItem, HeadItems, HoParam};
use crate::pipeline::asts::vocabulary::Vec1;
use crate::resolution::registry::ConsultRegistry;
use std::collections::HashMap;

/// Look up a function entity by name: if namespace is specified, look in that
/// namespace; otherwise search across all borrowed namespaces.
pub(super) fn lookup_borrowed_function(
    name: &delightql_types::SqlIdentifier,
    namespace: Option<&str>,
    consult: &ConsultRegistry,
    scope: Option<&str>,
) -> Result<Option<crate::resolution::registry::ConsultedEntity>> {
    if let Some(ns) = namespace {
        let fq = ns.to_string();
        // Blueprint inertness, loud door for the function route
        // (companion_linear--74): a qualified call into an archived blueprint
        // gets the badged refusal, not a confusing "no such function".
        consult.refuse_if_blueprint_fq(&fq)?;
        Ok(consult
            .lookup_entity(name.as_str(), name.is_stropped(), &fq, scope)
            .filter(|e| e.entity_type == EntityType::DqlFunctionExpression))
    } else {
        consult.lookup_enlisted_function(name.as_str(), name.is_stropped(), scope)
    }
}

/// Look up a context-aware function entity (type=3) by name.
pub(super) fn lookup_borrowed_context_aware_function(
    name: &delightql_types::SqlIdentifier,
    namespace: Option<&str>,
    consult: &ConsultRegistry,
    scope: Option<&str>,
) -> Result<Option<crate::resolution::registry::ConsultedEntity>> {
    if let Some(ns) = namespace {
        let fq = ns.to_string();
        // Blueprint inertness, loud door (--74) — see lookup_borrowed_function.
        consult.refuse_if_blueprint_fq(&fq)?;
        Ok(consult
            .lookup_entity(name.as_str(), name.is_stropped(), &fq, scope)
            .filter(|e| e.entity_type == EntityType::DqlContextAwareFunctionExpression))
    } else {
        consult.lookup_enlisted_context_aware_function(name.as_str(), name.is_stropped(), scope)
    }
}

/// The data world a pre-grounded namespace binds (default_data_ns, set
/// by ground!), as a path — the fallback when no query-level grounding
/// supplies one, so an entity enlisted or borrowed FROM a pre-grounded
/// namespace still resolves its table holes against its bound world.
pub(super) fn pre_grounded_data_ns_path(
    consult: &ConsultRegistry,
    namespace_fq: &str,
) -> Option<ast_unresolved::NamespacePath> {
    consult
        .get_namespace_default_data_ns(namespace_fq)
        .and_then(|fq| ast_unresolved::NamespacePath::from_fq_string(&fq).ok())
}

/// Convert a consulted entity (type=1 or type=3) into the CfeDefinition the
/// instantiation road spends at its call sites.
///
/// Re-parses the stored definition text to extract the context_mode and body.
/// For multi-clause definitions (disjunctive functions), the clauses assemble
/// into one selection whose arms carry what each clause computes.
pub(crate) fn consulted_entity_to_cfe_definition(
    entity: &crate::resolution::registry::ConsultedEntity,
) -> Result<CfeDefinition> {
    let group = crate::ddl::reconstruct::group(&entity.definition).map_err(|e| {
        DelightQLError::parse_error(format!(
            "No definition found for function '{}': {e}",
            entity.name
        ))
    })?;

    // The declared parameters, in BINDING order: a call site supplies code
    // first, so the callable formals stand before the scalar ones whatever
    // order the declaration interleaved them in. The carrier's group door
    // makes a misordering unwritable.
    use crate::pipeline::asts::core::CfeFormals;
    let formals: CfeFormals = if group.kind() == DefKind::Function {
        let mut callable: Vec<delightql_types::SqlIdentifier> = Vec::new();
        let mut scalar: Vec<delightql_types::SqlIdentifier> = Vec::new();
        for param in group.params() {
            let HoParam::Scalar {
                name,
                callable: is_code,
                ..
            } = param
            else {
                continue;
            };
            if *is_code {
                callable.push(name.clone());
            } else {
                scalar.push(name.clone());
            }
        }
        CfeFormals::from_role_groups(callable, scalar)
    } else {
        CfeFormals::from_role_groups([], entity.params.iter().map(|p| p.name().clone()))
    };
    let context_mode = group.context().clone();
    let mut clauses = group.into_clauses();

    if clauses.len() == 1 {
        let clause = clauses.pop().expect("length checked above");

        let body = clause.into_out_value().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Expected scalar body for function '{}', got relational",
                entity.name
            ))
        })?;

        Ok(CfeDefinition {
            name: entity.name.clone(),
            formals,
            context_mode,
            body,
            source_namespace: Some(entity.namespace.clone()),
        })
    } else {
        // Multi-clause: synthesize CASE expression with parameter Lvars intact
        let body = crate::pipeline::asts::core::OutValue::Domain(build_case_body_from_clauses(
            &entity.name,
            clauses,
        )?);

        Ok(CfeDefinition {
            name: entity.name.clone(),
            formals,
            context_mode,
            body,
            source_namespace: Some(entity.namespace.clone()),
        })
    }
}

// ============================================================================
// Multi-clause selection synthesis
// ============================================================================

/// Assemble a `ClauseSelection` from multiple guarded function clauses,
/// leaving parameter Lvars intact (no substitution).
///
/// THE SYNTHESIZED SELECTION IS ITS OWN SHAPE: the arms carry clause BODIES,
/// and a value rule's body is one of the crossing's licensed positions. The
/// authored `CaseExpression` is a different carrier whose arm results are
/// plain domain expressions, so neither is spelled with the other's type.
///
/// Used when converting multi-clause DDL functions into CfeDefinitions; the
/// formals stand as ordinary named references until the frame answers them.
fn build_case_body_from_clauses(
    name: &str,
    clauses: Vec<Clause>,
) -> Result<ast_unresolved::DomainExpression> {
    let mut arms: Vec<crate::pipeline::asts::core::ClauseArm<Unresolved>> = Vec::new();

    for clause in &clauses {
        let params = clause.params();

        // A CLAUSE'S BODY IS WHAT IT COMPUTES, crossing included. The
        // synthesized selection's arms carry the same thing, so a lawful
        // crossed clause is not narrowed away on the way into one.
        let body = clause.as_out_value().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Expected scalar body for multi-clause function '{name}', got relational"
            ))
        })?;

        let guard = params.iter().find_map(|p| match p {
            HoParam::Scalar { guard, .. } => guard.as_ref(),
            _ => None,
        });
        arms.push(crate::pipeline::asts::core::ClauseArm {
            guard: guard.cloned(),
            result: body.clone(),
        });
    }

    Ok(ast_unresolved::DomainExpression::Application(
        ast_unresolved::FunctionApplication::ClauseSelection(
            crate::pipeline::asts::core::ClauseSelection { arms },
        ),
    ))
}

// ============================================================================
// Parameter substitution (used by sigma predicates)
// ============================================================================

/// Replaces `Lvar` nodes whose names appear in `param_map` with the
/// corresponding argument expression. All other nodes are structurally
/// descended by the default `walk_*` functions.
struct ParamSubstituter<'a> {
    param_map: &'a HashMap<&'a str, &'a ast_unresolved::DomainExpression>,
}

impl AstTransform<Unresolved, Unresolved> for ParamSubstituter<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

    // Stack-safe: one descent per nesting level, and the walk a
    // parenthesis ladder actually reaches.
    #[stacksafe::stacksafe]
    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Unresolved>> {
        match expr {
            DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
                ref name,
                ..
            }))) => {
                match self.param_map.get(name.as_str()) {
                    // The substituted value stands where the formal stood.
                    // What the position publishes it as is the position's
                    // question, and this is not one.
                    Some(&replacement) => Ok(replacement.clone()),
                    None => Ok(expr),
                }
            }
            other => walk_transform_domain(self, other),
        }
    }
}

/// Parameter substitution over a TRUTH body — a sigma rule's.
pub(crate) fn substitute_in_truth_expr(
    expr: ast_unresolved::TruthExpression,
    param_map: &HashMap<&str, &ast_unresolved::DomainExpression>,
) -> ast_unresolved::TruthExpression {
    ParamSubstituter { param_map }
        .transform_boolean(expr)
        .expect("substitution is infallible")
}

// ============================================================================
// View expansion
// ============================================================================

/// Expand a consulted view body into an unresolved Query.
///
/// Parses the view body source and patches all unqualified table references
/// to use the data namespace from the grounding context. Returns a full Query
/// (not just Chain) to preserve CTEs in view definitions.
///
/// For multi-clause (disjunctive) view definitions, synthesizes same-name CTEs
/// so the resolver's CTE merge infrastructure produces UNION ALL automatically.
pub(super) fn expand_consulted_view(
    body_source: &str,
    grounding: &GroundedPath,
) -> Result<ast_unresolved::Query> {
    let group = crate::ddl::reconstruct::group(body_source)?;
    let view_name = group.name();
    // The head declares a CLOSED schema; each clause satisfies it by
    // carrying the projection its own head declares. No text is
    // regenerated and nothing is re-parsed: the bodies that come out are
    // the ones that went in, with one continuation appended.
    let mut clauses = group.spend_heads()?;

    if clauses.len() == 1 {
        // Fast path: single clause (existing behavior)
        let clause = clauses.pop().expect("length checked above");
        let query = clause.into_query().ok_or_else(|| {
            DelightQLError::parse_error("Expected relational body for view, got scalar")
        })?;
        return Ok(patch_data_ns_query(
            query,
            &grounding.data_ns,
            &std::collections::HashSet::new(),
        ));
    }

    // Multi-clause: synthesize disjunctive CTEs
    expand_multi_clause_view(&view_name, clauses, Some(&grounding.data_ns))
}

/// Synthesize a disjunctive view from multiple clause definitions.
///
/// Creates same-name CTE bindings for each clause body, then wraps them
/// in a `Query::WithCtes` with a `view_name(*)` main query. The resolver's
/// CTE merge infrastructure groups same-name CTEs into UNION ALL.
pub(super) fn expand_multi_clause_view(
    view_name: &str,
    clauses: Vec<crate::pipeline::asts::ddl::Clause>,
    data_ns: Option<&ast_unresolved::NamespacePath>,
) -> Result<ast_unresolved::Query> {
    let view_name = view_name.to_string();
    let mut all_ctes = Vec::new();

    for def in clauses {
        let query = def.into_query().ok_or_else(|| {
            DelightQLError::parse_error(
                "Expected relational body for disjunctive view clause, got scalar",
            )
        })?;
        let patched = if let Some(ns) = data_ns {
            patch_data_ns_query(query, ns, &std::collections::HashSet::new())
        } else {
            query
        };

        let ast_unresolved::Query { cfes, ctes, body } = patched;
        if !cfes.is_empty() {
            return Err(DelightQLError::parse_error(
                "Unsupported query form in disjunctive view clause: a query-scoped \
                 function definition"
                    .to_string(),
            ));
        }
        // Clause body's own CTEs hoist into the outer list first, then the
        // body becomes the disjunctive CTE.
        for cte in ctes {
            all_ctes.push(cte);
        }
        all_ctes.push(ast_unresolved::CteBinding {
            subject: crate::pipeline::asts::core::CteSubject::Authored {
                name: delightql_types::SqlIdentifier::new(view_name.clone()),
                effect: crate::pipeline::asts::core::CteEffectDeclaration::Pure,
            },
            authority: crate::pipeline::asts::core::CteAuthority {
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                resolution_owner: crate::pipeline::asts::core::provenance::CteResolutionOwner::Entity,
            },
            expression: body,
            recursion: (),
        });
    }

    // Main query: view_name(*) — a ground relation referencing the CTE
    let main_query = ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                identifier: ast_unresolved::QualifiedName {
                    namespace_path: ast_unresolved::NamespacePath::empty(),
                    name: view_name.into(),
                },
                alias: None,
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
            cpr_schema: (),
        },
        ast_unresolved::Access::All,
        (),
    );

    Ok(ast_unresolved::Query {
        cfes: Vec::new(),
        ctes: all_ctes,
        body: main_query,
    })
}

/// How many leading argument positions of this callee take CODE — the
/// curried formals of a consulted higher-order function. Zero for
/// everything else, including names the catalog does not know.
pub(super) fn curried_code_positions(
    callee: &crate::pipeline::asts::vocabulary::Ref,
    registry: &crate::resolution::EntityRegistry,
    scope: Option<&str>,
) -> usize {
    let name = callee.name_identifier();
    let namespace = callee.namespace_fq();
    let Ok(Some(entity)) =
        lookup_borrowed_function(&name, namespace.as_deref(), &registry.consult, scope)
    else {
        return 0;
    };
    consulted_entity_to_cfe_definition(&entity)
        .map(|cfe| cfe.callable_formals().len())
        .unwrap_or(0)
}

/// THE INSTANTIATION ROAD: a consulted value definition is spent at its
/// call site, before ordinary closed resolution.
///
/// The definition environment identifies the formals and substitutes the
/// supplied values into a copy of the UNRESOLVED body; what resolution then
/// sees is ordinary closed code in the caller's position, so no parameter
/// survives as a deferred hole and no later phase re-substitutes. A crossed
/// body — a truth read as a value — resolves into the licensed
/// `ClauseSelection` carrier, whose arms are the one position that admits
/// it.
pub(super) fn inline_cfe_call(
    fold: &mut super::resolver_fold::ResolverFold,
    application: &ast_unresolved::StandardApplication,
) -> Result<Option<crate::pipeline::asts::resolved::DomainExpression>> {
    use crate::pipeline::asts::core::operators::ScalarArgument;
    if fold.cfe_code_suppression > 0 {
        // The call stands in a CODE position: it is handed to a curried
        // formal, not invoked here.
        return Ok(None);
    }
    let reference = &application.call().callee;
    let name = reference.name_text();
    let namespace = reference.namespace_fq();
    // A CODE FORMAL, INVOKED: the innermost open instantiation bound code
    // to this bare name, and the invocation spends that binding — before
    // any catalog is asked, because the formal is not a catalog name.
    if namespace.is_none() {
        let key = reference.name_identifier();
        let binding = fold
            .config
            .cfe_formal_frame
            .as_deref()
            .and_then(|frame| frame.callables.get(&key))
            .cloned();
        if let Some(binding) = binding {
            return instantiate_callable_site(fold, application, binding, &name).map(Some);
        }
    }
    if application.guard.is_some() || application.window.is_some() {
        // A guard filters the rows this application sees and a window
        // modifies an aggregate; neither belongs to a scalar definition's
        // instantiation.
        return Ok(None);
    }
    let Some(cfe) = lookup_cfe_definition(fold, reference, namespace.as_deref())? else {
        return Ok(None);
    };
    // Nesting is authored and finite (`double:(double:(id))`); only a
    // definition whose BODY reaches itself spins, and that exhausts the
    // compilation's ONE allowance instead of a name-based cycle guard that
    // would refuse lawful nesting.
    let _instantiation_frame = fold.config.instantiation_depth.enter(&name)?;
    use crate::pipeline::asts::core::ContextMode;
    let all_members = application.call().arguments.scalar_members();
    let is_marker = |member: &ScalarArgument<crate::pipeline::asts::core::Unresolved>| {
        matches!(member, ScalarArgument::Context(_))
    };
    let context_call = all_members.first().is_some_and(is_marker);
    match (&cfe.context_mode, context_call) {
        (ContextMode::None, true) => {
            return Err(DelightQLError::parse_error(format!(
                "'{name}' is not context-aware: it declares no `..`; supply values positionally"
            )));
        }
        (ContextMode::Implicit, false) => {
            return Err(DelightQLError::parse_error(format!(
                "CFE '{name}' uses implicit context and cannot be called positionally — use {name}:(.., args)"
            )));
        }
        _ => {}
    }
    let members = if context_call {
        &all_members[1..]
    } else {
        all_members
    };
    if members.iter().skip(1).any(|member| is_marker(member))
        || (!context_call && members.first().is_some_and(is_marker))
    {
        return Err(DelightQLError::parse_error(format!(
            "`..` stands first in a call or not at all; '{name}' received one elsewhere"
        )));
    }
    let (callable_formals, scalar_formals) = cfe.split_formals();
    let curried_count = callable_formals.len();
    // Explicit captures called positionally are leading positionals; a
    // context call binds them by name instead, so they are not counted.
    let positional_captures = match (&cfe.context_mode, context_call) {
        (ContextMode::Explicit(captures), false) => captures.len(),
        _ => 0,
    };
    let declared = curried_count + positional_captures + scalar_formals.len();
    if members.len() != declared {
        // A context call is lenient like the road it replaces: the capture
        // that cannot bind is what refuses, below, by name.
        if !context_call {
            if positional_captures > 0 {
                return Err(DelightQLError::parse_error(format!(
                    "'{name}' expects {declared} positional argument{} (captures first), got {}",
                    if declared == 1 { "" } else { "s" },
                    members.len()
                )));
            }
            return Err(DelightQLError::validation_error_categorized(
                "cfe/arity",
                format!(
                    "'{name}' expects {declared} argument{}, got {}",
                    if declared == 1 { "" } else { "s" },
                    members.len()
                ),
                "supply one argument per declared parameter, code first",
            ));
        }
    }
    let mut callables = std::collections::HashMap::new();
    for (formal, member) in callable_formals.iter().zip(members[..curried_count].iter()) {
        // The position declares CODE, so what stands in it is code however
        // spelled: `upper:()` arrives as an ordinary (nullary) application
        // and the position reads it as the mention it is.
        let binding = match member {
            ScalarArgument::Callable(callable) => callable_binding(fold, callable)?,
            ScalarArgument::Value(value)
                if matches!(
                    value.domain(),
                    Some(ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Standard(_)
                    ))
                ) =>
            {
                let Some(ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Standard(mention),
                )) = value.domain()
                else {
                    unreachable!("shape checked above");
                };
                callable_binding(
                    fold,
                    &crate::pipeline::asts::core::Callable::Functor(mention.clone()),
                )?
            }
            _ => {
                return Err(DelightQLError::validation_error_categorized(
                    "cfe/code_argument",
                    format!("'{name}' takes code in its position for '{}'", formal.name),
                    "write a mention `fn:()`, a lambda `:(…)`, or a template `:\"…\"`",
                ));
            }
        };
        callables.insert(formal.name.clone(), binding);
    }
    let mut values: Vec<ArgumentToBind> = Vec::new();
    for member in members[curried_count + positional_captures..].iter() {
        match member {
            ScalarArgument::Value(value) => match value {
                crate::pipeline::asts::core::ArgumentValue::Domain { value, .. } => {
                    values.push(ArgumentToBind::Domain(value.clone()))
                }
                // A truth standing in a value position is a value — 0 or 1 —
                // and binds like one.
                crate::pipeline::asts::core::ArgumentValue::Truth(truth) => {
                    values.push(ArgumentToBind::Truth(truth.clone().into_truth()))
                }
            },
            ScalarArgument::Callable(_)
            | ScalarArgument::Spread(_)
            | ScalarArgument::Star
            | ScalarArgument::Context(_) => return Ok(None),
        }
    }
    // Arguments are the CALLER's expressions: they resolve here, in the
    // caller's scope, before the body opens. The body then resolves with a
    // formal frame instead of textual substitution — a spliced name never
    // re-resolves inside the body's probes, so nothing the body opens can
    // capture it. The frame rides the config because nested resolutions
    // clone it: a formal reaches into the subqueries the body opens.
    let mut frame = super::FormalFrame {
        values: std::collections::HashMap::new(),
        callables,
    };
    if let ContextMode::Explicit(captures) = &cfe.context_mode {
        if context_call {
            // `..` binds each declared capture BY NAME, resolved at the
            // call site. A name the site cannot answer refuses here.
            for capture in captures {
                let reference = ast_unresolved::DomainExpression::Reference(
                    crate::pipeline::asts::core::Reference::Named(
                        crate::pipeline::asts::core::NamedReference(
                            crate::pipeline::asts::core::AuthoredColumn {
                                name: capture.clone(),
                                qualifier: None,
                                namespace_path: ast_unresolved::NamespacePath::empty(),
                            },
                        ),
                    ),
                );
                frame
                    .values
                    .insert(capture.clone(), fold.transform_domain(reference)?);
            }
        } else {
            // Positional: the captures are the leading positions.
            for (capture, value) in captures.iter().zip(
                members[curried_count..curried_count + positional_captures]
                    .iter()
                    .filter_map(|member| match member {
                        ScalarArgument::Value(value) => value.domain().cloned(),
                        _ => None,
                    }),
            ) {
                frame
                    .values
                    .insert(capture.clone(), fold.transform_domain(value)?);
            }
        }
    }
    for (formal, value) in scalar_formals.iter().zip(values) {
        frame
            .values
            .insert(formal.name.clone(), value.resolve(fold)?);
    }
    let prior = std::mem::replace(
        &mut fold.config.cfe_formal_frame,
        Some(std::sync::Arc::new(frame)),
    );
    // A consulted definition's body looks up ITS OWN siblings: nested calls
    // resolve against the owning namespace's edges, never the caller's
    // session set.
    let prior_scope = match &cfe.source_namespace {
        Some(namespace) => std::mem::replace(
            &mut fold.config.resolution_namespace,
            Some(namespace.clone()),
        ),
        None => fold.config.resolution_namespace.clone(),
    };
    // A definition's body is SEALED: only its formals — and, for a
    // context-aware definition, its declared captures — reach the call
    // site's row. Implicit context is the one deliberate unsealing: `..`
    // DECLARES that free names capture from the caller.
    let sealed = !matches!(cfe.context_mode, ContextMode::Implicit);
    let saved_scope = if sealed {
        Some((
            std::mem::take(&mut fold.available),
            std::mem::take(&mut fold.local_available),
            std::mem::take(&mut fold.qualifier_scope),
        ))
    } else {
        None
    };
    let outcome = match cfe.body {
        ast_unresolved::OutValue::Domain(body) => fold.transform_domain(body).map(Some),
        ast_unresolved::OutValue::Truth(crossing) => fold
            .transform_boolean(crossing.into_truth())
            .map(|resolved| {
                Some(
                    crate::pipeline::asts::resolved::DomainExpression::Application(
                        crate::pipeline::asts::resolved::FunctionApplication::ClauseSelection(
                            crate::pipeline::asts::core::ClauseSelection {
                                arms: vec![crate::pipeline::asts::core::ClauseArm {
                                    guard: None,
                                    result: crate::pipeline::asts::resolved::OutValue::Truth(
                                        crate::pipeline::asts::core::TruthAsValue(resolved),
                                    ),
                                }],
                            },
                        ),
                    ),
                )
            }),
    };
    if let Some((available, local, qualifiers)) = saved_scope {
        fold.available = available;
        fold.local_available = local;
        fold.qualifier_scope = qualifiers;
    }
    fold.config.resolution_namespace = prior_scope;
    fold.config.cfe_formal_frame = prior;
    outcome
}

/// An argument awaiting binding: the caller's value, or the caller's truth
/// read as one. Both resolve in the CALLER's scope; the truth resolves into
/// the licensed ClauseSelection carrier.
enum ArgumentToBind {
    Domain(ast_unresolved::DomainExpression),
    Truth(ast_unresolved::TruthExpression),
}

impl ArgumentToBind {
    fn resolve(
        self,
        fold: &mut super::resolver_fold::ResolverFold,
    ) -> Result<crate::pipeline::asts::resolved::DomainExpression> {
        match self {
            ArgumentToBind::Domain(value) => fold.transform_domain(value),
            ArgumentToBind::Truth(truth) => fold.transform_boolean(truth).map(|resolved| {
                crate::pipeline::asts::resolved::DomainExpression::Application(
                    crate::pipeline::asts::resolved::FunctionApplication::ClauseSelection(
                        crate::pipeline::asts::core::ClauseSelection {
                            arms: vec![crate::pipeline::asts::core::ClauseArm {
                                guard: None,
                                result: crate::pipeline::asts::resolved::OutValue::Truth(
                                    crate::pipeline::asts::core::TruthAsValue(resolved),
                                ),
                            }],
                        },
                    ),
                )
            }),
        }
    }
}

/// The window builtins' signature judgment — the ONE authority, consulted
/// from the ordinary Standard-application road for authored and rebuilt
/// invocations alike. The keyword "function" is the refusal's badge.
pub(super) fn judge_window_row(
    fold: &super::resolver_fold::ResolverFold,
    callee_name: &str,
    supplied: usize,
) -> Result<()> {
    let Some((min, max)) = fold.registry.built_in.window_signature(callee_name) else {
        return Ok(());
    };
    if supplied < min as usize || supplied > max as usize {
        return Err(DelightQLError::parse_error(format!(
            "the window function '{callee_name}' takes {} argument{}; the invocation hands it {supplied}",
            if min == max {
                min.to_string()
            } else {
                format!("{min} to {max}")
            },
            if max == 1 { "" } else { "s" },
        )));
    }
    Ok(())
}

/// The ONE bound on open instantiations, whichever road opens them: the
/// ordinary value position and the pattern slot answer identically.
pub(crate) const INSTANTIATION_DEPTH_LIMIT: usize = 128;

/// The one refusal for exhausting it.
pub(crate) fn instantiation_depth_refusal(name: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "cfe/recursion",
        format!(
            "instantiating '{name}' opened {INSTANTIATION_DEPTH_LIMIT} nested definitions: \
             a value definition cannot recurse"
        ),
        "a scalar definition computes from its inputs; write recursion as a relational rule",
    )
}

/// The definition a callee names, if any: query-scoped first, then
/// consulted (with its data namespace patched in).
pub(super) fn lookup_cfe_definition(
    fold: &mut super::resolver_fold::ResolverFold,
    callee: &crate::pipeline::asts::vocabulary::Ref,
    namespace: Option<&str>,
) -> Result<Option<CfeDefinition>> {
    if namespace.is_none() {
        let key = callee.name_identifier();
        if let Some(cfe) = fold.registry.query_local.scoped_cfes.get(&key).cloned() {
            return Ok(Some(cfe));
        }
    }
    let scope = fold.config.resolution_namespace.clone();
    let callee_ident = callee.name_identifier();
    let entity = match lookup_borrowed_function(
        &callee_ident,
        namespace,
        &fold.registry.consult,
        scope.as_deref(),
    )? {
        Some(entity) => entity,
        None => {
            let Some(entity) = lookup_borrowed_context_aware_function(
                &callee_ident,
                namespace,
                &fold.registry.consult,
                scope.as_deref(),
            )?
            else {
                return Ok(None);
            };
            entity
        }
    };
    let mut cfe = consulted_entity_to_cfe_definition(&entity)?;
    if let Some(ns) = pre_grounded_data_ns_path(&fold.registry.consult, &entity.namespace) {
        cfe.body = patch_data_ns_in_body(cfe.body, &ns);
    }
    Ok(Some(cfe))
}

/// A COVER'S CALLABLE THAT NAMES A DEFINITION, instantiated to the open
/// lambda it denotes: the FIRST formal is the covered cell — it stays a
/// slot — and the mention's own arguments fill the rest. What the cover
/// machinery then spends per cell is ordinary resolved code; no carrier
/// survives for a later phase to expand.
pub(super) fn cover_functor_apply_cell(
    fold: &mut super::resolver_fold::ResolverFold,
    application: &ast_unresolved::StandardApplication,
    cell: crate::pipeline::asts::resolved::DomainExpression,
) -> Result<Option<crate::pipeline::asts::resolved::DomainExpression>> {
    use crate::pipeline::asts::core::ContextMode;
    let name = application.call().callee.name_text();
    let namespace = application.call().callee.namespace_fq();
    let Some(cfe) = lookup_cfe_definition(fold, &application.call().callee, namespace.as_deref())?
    else {
        return Ok(None);
    };
    if cfe.context_mode != ContextMode::None || !cfe.callable_formals().is_empty() {
        // A context or higher-order definition has no one-cell reading;
        // the ordinary callable road keeps whatever meaning it had.
        return Ok(None);
    }
    let Some((cell_formal, partial_formals)) = cfe.scalar_formals().split_first() else {
        return Err(DelightQLError::validation_error_categorized(
            "cfe/cover_arity",
            format!("'{name}' takes no parameters, so a cover cannot land the cell in one"),
            "the covered callable's first parameter receives each cell",
        ));
    };
    let partials: Vec<ast_unresolved::DomainExpression> = application
        .call()
        .arguments
        .value_domains()
        .cloned()
        .collect();
    if partials.len() != partial_formals.len() {
        return Err(DelightQLError::validation_error_categorized(
            "cfe/cover_arity",
            format!(
                "'{name}' has {} parameter{} after the cell; the mention supplies {}",
                partial_formals.len(),
                if partial_formals.len() == 1 { "" } else { "s" },
                partials.len()
            ),
            "the cell lands first; supply one value per remaining parameter",
        ));
    }
    let mut frame = super::FormalFrame {
        values: std::collections::HashMap::new(),
        callables: std::collections::HashMap::new(),
    };
    // THE CELL LANDS IN THE FIRST PARAMETER: the cover is the applying
    // position, so the formal is the cell itself and the instantiated body
    // resolves CLOSED — no leaf survives into it.
    frame.values.insert(cell_formal.name.clone(), cell);
    for (formal, value) in partial_formals.iter().zip(partials) {
        frame
            .values
            .insert(formal.name.clone(), fold.transform_domain(value)?);
    }
    let _instantiation_frame = fold.config.instantiation_depth.enter(&name)?;
    let prior = std::mem::replace(
        &mut fold.config.cfe_formal_frame,
        Some(std::sync::Arc::new(frame)),
    );
    let prior_scope = match &cfe.source_namespace {
        Some(namespace) => std::mem::replace(
            &mut fold.config.resolution_namespace,
            Some(namespace.clone()),
        ),
        None => fold.config.resolution_namespace.clone(),
    };
    let saved_scope = (
        std::mem::take(&mut fold.available),
        std::mem::take(&mut fold.local_available),
        std::mem::take(&mut fold.qualifier_scope),
    );
    let outcome = match cfe.body {
        ast_unresolved::OutValue::Domain(body) => fold.transform_domain(body),
        ast_unresolved::OutValue::Truth(crossing) => fold
            .transform_boolean(crossing.into_truth())
            .map(|resolved| {
                crate::pipeline::asts::resolved::DomainExpression::Application(
                    crate::pipeline::asts::resolved::FunctionApplication::ClauseSelection(
                        crate::pipeline::asts::core::ClauseSelection {
                            arms: vec![crate::pipeline::asts::core::ClauseArm {
                                guard: None,
                                result: crate::pipeline::asts::resolved::OutValue::Truth(
                                    crate::pipeline::asts::core::TruthAsValue(resolved),
                                ),
                            }],
                        },
                    ),
                )
            }),
    };
    let (available, local, qualifiers) = saved_scope;
    fold.available = available;
    fold.local_available = local;
    fold.qualifier_scope = qualifiers;
    fold.config.resolution_namespace = prior_scope;
    fold.config.cfe_formal_frame = prior;
    Ok(Some(outcome?))
}

/// The code a caller supplies for a curried formal, made a binding.
///
/// A mention keeps its authored form — its arguments are replaced where the
/// formal is invoked. An open body (lambda, template) pre-resolves HERE, in
/// the caller's scope, with its slots left standing: the interior is the
/// caller's text, and resolving it later — inside the definition's frame —
/// would let the definition's formals capture the caller's names.
fn callable_binding(
    fold: &mut super::resolver_fold::ResolverFold,
    callable: &crate::pipeline::asts::core::Callable,
) -> Result<super::CallableBinding> {
    use crate::pipeline::asts::core::Callable;
    match callable {
        Callable::Functor(application) => {
            // A mention of an OUTER code formal hands the outer binding on.
            if application.call().callee.namespace_fq().is_none() {
                let key = application.call().callee.name_identifier();
                if let Some(outer) = fold
                    .config
                    .cfe_formal_frame
                    .as_deref()
                    .and_then(|frame| frame.callables.get(&key))
                {
                    return Ok(outer.clone());
                }
            }
            // The mention is judged HERE, where it is handed over: a window
            // function needs its window and refuses standing bare. The probe
            // resolves under suppression so a definition's mention is not an
            // invocation; its resolved form is discarded either way.
            fold.cfe_code_suppression += 1;
            let probe = fold.transform_domain(ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Standard(application.clone()),
            ));
            fold.cfe_code_suppression -= 1;
            probe?;
            Ok(super::CallableBinding::Named(Box::new(application.clone())))
        }
        Callable::Lambda(lambda) => open_binding(fold, (*lambda.body).clone()),
        Callable::String(template) => open_binding(
            fold,
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Template(template.clone()),
            ),
        ),
    }
}

/// An invocation of a curried FORMAL, spent against what the caller bound.
fn instantiate_callable_site(
    fold: &mut super::resolver_fold::ResolverFold,
    application: &ast_unresolved::StandardApplication,
    binding: super::CallableBinding,
    name: &str,
) -> Result<crate::pipeline::asts::resolved::DomainExpression> {
    match binding {
        super::CallableBinding::Named(mention) => {
            // The invocation's own arguments replace the mention's, and its
            // guard wins; the window rides whichever side wrote one.
            let mut rebuilt = *mention;
            if !application.call().arguments.scalar_members().is_empty() {
                rebuilt.call_mut().arguments = application.call().arguments.clone();
            }
            rebuilt.guard = application.guard.clone().or(rebuilt.guard);
            rebuilt.window = application.window.clone().or(rebuilt.window);
            // The rebuilt invocation resolves through the ordinary road,
            // where the ONE window-signature authority judges it exactly as
            // it judges an authored spelling.
            fold.transform_domain(ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Standard(rebuilt),
            ))
        }
        super::CallableBinding::Open(binding) => {
            let mut domains = application.call().arguments.value_domains();
            let (Some(first), None) = (domains.next(), domains.next()) else {
                return Err(DelightQLError::validation_error_categorized(
                    "cfe/lambda_arity",
                    format!("'{name}' is bound to an open body, which has ONE slot"),
                    "supply exactly one value",
                ));
            };
            let value = fold.transform_domain(first.clone())?;
            // THE INTERIOR IS THE CALLER'S TEXT: it resolves in the caller's
            // captured scope, outside the definition's formal frame, with
            // the supplied value standing in its slots — the applying
            // position spends the leaf here, and nothing open survives.
            let saved = (
                std::mem::replace(&mut fold.available, binding.available.clone()),
                std::mem::replace(&mut fold.local_available, binding.local_available.clone()),
                std::mem::replace(&mut fold.qualifier_scope, binding.qualifier_scope.clone()),
            );
            let prior_frame = std::mem::take(&mut fold.config.cfe_formal_frame);
            let prior_cell = fold.cover_cell.replace(value);
            let outcome = fold.transform_domain(binding.body.clone());
            fold.cover_cell = prior_cell;
            fold.config.cfe_formal_frame = prior_frame;
            fold.available = saved.0;
            fold.local_available = saved.1;
            fold.qualifier_scope = saved.2;
            outcome
        }
    }
}

/// An open body made a binding: judged HERE, in the caller's scope — a bad
/// reference refuses at the handover even if the formal is never invoked —
/// and carried authored for the invocation to apply.
fn open_binding(
    fold: &mut super::resolver_fold::ResolverFold,
    body: ast_unresolved::DomainExpression,
) -> Result<super::CallableBinding> {
    let probe_cell = crate::pipeline::asts::resolved::DomainExpression::Application(
        crate::pipeline::asts::resolved::FunctionApplication::Ground(
            crate::pipeline::asts::core::LiteralValue::Null,
        ),
    );
    let prior = fold.cover_cell.replace(probe_cell);
    let probe = fold.transform_domain(body.clone());
    fold.cover_cell = prior;
    probe?;
    Ok(super::CallableBinding::Open(Box::new(super::OpenBinding {
        body,
        available: fold.available.clone(),
        local_available: fold.local_available.clone(),
        qualifier_scope: fold.qualifier_scope.clone(),
    })))
}

// ============================================================================
// Ground scalar expansion for HO views
// ============================================================================

use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode, HoPositionInfo};

/// Compute cross-clause unified position analysis for all HO parameter positions.
///
/// For each position 0..max_params across all clauses:
/// - Determines column_kind: Glob/Argumentative/Scalar
/// - Determines ground_mode from the Scalar/GroundScalar distribution
/// - Collects ground_values: Vec<(ordinal, value)>
/// - Determines column_name: from free-variable clauses (must agree)
///
/// This replaces `extract_ground_scalar_info()` + `validate_mixed_ground_params()`
/// with a single, complete analysis computed at consult time.
pub(crate) fn build_ho_position_analysis(
    group: &crate::pipeline::asts::ddl::DefinitionGroup,
) -> Vec<HoPositionInfo> {
    if group.kind() != DefKind::HoView {
        return Vec::new();
    }
    let heads: Vec<&[HoParam]> = group.clauses().iter().map(Clause::params).collect();

    build_ho_position_analysis_from_heads(&heads)
}

/// Build position analysis from a set of HO head param lists.
///
/// Accepts pre-extracted heads so callers that only have heads (not whole
/// clauses) can use this directly — e.g., the deferred-body HO view path in
/// system.rs where each clause's head is parsed individually.
pub(crate) fn build_ho_position_analysis_from_heads(heads: &[&[HoParam]]) -> Vec<HoPositionInfo> {
    if heads.is_empty() {
        return Vec::new();
    }

    let max_params = heads.iter().map(|h| h.len()).max().unwrap_or(0);
    let mut positions = Vec::with_capacity(max_params);

    for pos in 0..max_params {
        let mut has_glob = false;
        let mut has_argumentative = false;
        let mut arg_columns: Option<Vec<String>> = None;
        let mut has_scalar = false;
        let mut has_ground_scalar = false;
        let mut ground_values: Vec<(usize, String)> = Vec::new();
        let mut column_name: Option<String> = None;

        for (clause_ordinal, head) in heads.iter().enumerate() {
            if let Some(param) = head.get(pos) {
                match param {
                    HoParam::Relation {
                        name,
                        cols: HeadItems::Glob,
                    } => {
                        has_glob = true;
                        // Glob contributes canonical name (table parameter name, e.g., "T")
                        if column_name.is_none() {
                            column_name = Some(name.to_string());
                        }
                    }
                    HoParam::Relation {
                        name,
                        cols: HeadItems::Listed(cols),
                    } => {
                        has_argumentative = true;
                        if arg_columns.is_none() {
                            arg_columns = Some(cols.iter().map(|c| c.supply.spelling()).collect());
                        }
                        // Argumentative contributes canonical name (table parameter name)
                        if column_name.is_none() {
                            column_name = Some(name.to_string());
                        }
                    }
                    HoParam::Scalar { name, .. } => {
                        has_scalar = true;
                        // Free variable — contributes canonical name
                        if column_name.is_none() {
                            column_name = Some(name.to_string());
                        }
                    }
                    HoParam::Ground { text, .. } => {
                        has_ground_scalar = true;
                        ground_values.push((clause_ordinal, text.clone()));
                        // A ground position contributes no column NAME: its
                        // spelling is the literal. The canonical name comes
                        // from a sibling clause that binds the position.
                    }
                }
            }
        }

        let column_kind = if has_glob {
            HoColumnKind::TableGlob
        } else if has_argumentative {
            HoColumnKind::TableArgumentative(arg_columns.unwrap_or_default())
        } else {
            HoColumnKind::Scalar
        };

        let ground_mode = if has_glob || has_argumentative {
            HoGroundMode::InputOnly
        } else if has_ground_scalar && !has_scalar {
            HoGroundMode::PureGround
        } else if has_ground_scalar && has_scalar {
            HoGroundMode::MixedGround
        } else {
            HoGroundMode::PureUnbound
        };

        positions.push(HoPositionInfo {
            position: pos,
            column_kind,
            ground_mode,
            ground_values,
            column_name,
        });
    }

    positions
}

/// Inject ground scalar constants as real AST columns into a clause body.
///
/// For each position where this clause has GroundScalar, wraps the body's
/// main query expression with a General (embed) operator:
///   `body |> (*, "ground_value" as column_name)`
///
/// Column names come from cross-clause position analysis:
/// - MixedGround positions: canonical name from Scalar (free-variable) clauses
/// - PureGround positions: DDL param name
///
/// At a MixedGround position a FREE clause must export the position
/// column too, carrying the CALLER's literal (its own substituted
/// value) — otherwise the union pads the column NULL and the
/// call-site filter (`x = 'a' AND y = 'c'`) kills every clause: the
/// whole entity silently empties. A caller lvar is injected too: the
/// caller-owned carrier is already in the clause body, so the discriminator
/// can publish the row value that selected this clause.
///
/// If `output_head` is Some, also applies the argumentative output projection.
pub(super) fn inject_scalar_columns(
    query: ast_unresolved::Query,
    clause_params: &[HoParam],
    positions: &[HoPositionInfo],
    output_head: Option<&[HeadItem]>,
    caller_scalar_params: &std::collections::HashMap<String, ast_unresolved::DomainExpression>,
    carry_caller_lvars: bool,
) -> ast_unresolved::Query {
    use crate::pipeline::asts::core::PipeOp;

    // Collect ground scalar injections: (column_name, literal_value)
    let mut ground_injections: Vec<(String, String)> = Vec::new();
    // And free-position injections at MixedGround positions:
    // (column_name, the caller's expression).
    let mut free_injections: Vec<(String, ast_unresolved::DomainExpression)> = Vec::new();
    for pos_info in positions {
        if let Some(clause_param) = clause_params.get(pos_info.position) {
            // A glob already republishes an identically named caller lvar.
            // Ground clauses constrain that occurrence before UNION; free
            // clauses bind it directly. Neither needs a second scalar column.
            let glob_already_carries_position = output_head.is_none()
                && carry_caller_lvars
                && pos_info.column_name.as_ref().is_some_and(|column_name| {
                    caller_scalar_params
                        .get(column_name)
                        .is_some_and(|expression| {
                            matches!(
                                expression,
                                ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
                                    name: caller_name,
                                    ..
                                }))) if caller_name == column_name
                            )
                        })
                });
            match clause_param {
                HoParam::Ground { text, .. } => {
                    if !glob_already_carries_position {
                        if let Some(name) = pos_info.column_name.clone() {
                            ground_injections.push((name, text.clone()));
                        }
                    }
                }
                HoParam::Scalar {
                    name: param_name, ..
                } if pos_info.ground_mode
                    == crate::pipeline::asts::ddl::HoGroundMode::MixedGround =>
                {
                    if let (Some(name), Some(expression)) = (
                        pos_info.column_name.clone(),
                        caller_scalar_params.get(param_name.as_str()),
                    ) {
                        if matches!(
                            expression,
                            ast_unresolved::DomainExpression::Application(
                                ast_unresolved::FunctionApplication::Ground(_)
                            )
                        ) || (carry_caller_lvars
                            && matches!(
                                expression,
                                ast_unresolved::DomainExpression::Reference(Reference::Named(
                                    NamedReference(AuthoredColumn { .. })
                                ))
                            )
                            && !glob_already_carries_position)
                        {
                            free_injections.push((name, expression.clone()));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if ground_injections.is_empty() && free_injections.is_empty() && output_head.is_none() {
        return query;
    }

    // Build the embed expressions
    let mut embed_items: Vec<ast_unresolved::OutItem> = Vec::new();
    let named = |expr: ast_unresolved::DomainExpression, name: &String| {
        ast_unresolved::OutItem::One(ast_unresolved::OneOut {
            expr: crate::pipeline::asts::core::OutValue::Domain(expr),
            naming: Some(name.clone().into()),
            output: (),
        })
    };

    if output_head.is_some() {
        // When there's an output head, inject ground constants as part of the projection
        // First: ground scalar constants
        for (col_name, literal_val) in &ground_injections {
            let literal = crate::pipeline::asts::core::LiteralValue::from_stored_ground(literal_val);
            embed_items.push(named(
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(literal),
                ),
                col_name,
            ));
        }
        for (col_name, expression) in &free_injections {
            embed_items.push(named(expression.clone(), col_name));
        }
        // Then: output head items
        if let Some(items) = output_head {
            for item in items {
                // NOTE: HO output-head positions do NOT yet honor `as`-labels. The
                // label parses (view_head_item is shared with rule heads) and is carried
                // in the AST, but is ignored here — a labeled HO output item is refused
                // earlier, at DDL build time (`ddl/head/ho_label_unsupported`), so this
                // code never sees one. Head-`as` on HO output positions is future work.
                match &item.supply {
                    crate::pipeline::asts::ddl::Supply::Ref(name) => {
                        embed_items.push(ast_unresolved::OutItem::plain(
                            ast_unresolved::DomainExpression::lvar_builder(name.clone()).build(),
                            (),
                        ));
                    }
                    crate::pipeline::asts::ddl::Supply::Ground(value) => {
                        embed_items.push(ast_unresolved::OutItem::plain(
                            ast_unresolved::DomainExpression::Application(
                                ast_unresolved::FunctionApplication::Ground(value.clone()),
                            ),
                            (),
                        ));
                    }
                }
            }
        }
    } else {
        // No output head (glob) — use embed: (*, "value" as name, ...)
        embed_items.push(ast_unresolved::OutItem::Many(
            crate::pipeline::asts::core::Spread::Glob(crate::pipeline::asts::core::Glob::whole()),
        ));
        for (col_name, literal_val) in &ground_injections {
            let literal = crate::pipeline::asts::core::LiteralValue::from_stored_ground(literal_val);
            embed_items.push(named(
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(literal),
                ),
                col_name,
            ));
        }
        for (col_name, expression) in &free_injections {
            embed_items.push(named(expression.clone(), col_name));
        }
    }

    let operator = PipeOp::Project(
        crate::pipeline::asts::vocabulary::Vec1::try_from_vec(embed_items)
            .expect("the embed carries the glob it was built around"),
    );

    // Wrap the main query expression with the pipe operator
    wrap_query_with_pipe(query, operator)
}

/// Parse a literal value string (e.g., `"young"`, `42`, `::fast`, or
/// `` :`people(*)` ``) into a LiteralValue. Mention ground values
/// arrive already canonical (the DDL extractor canonicalizes at
/// consult time), so the wrapper is stripped, never re-parsed.
/// A provable miss is an error, not an empty relation: a knowable ground
/// argument — a literal written at the call site — at a position every clause grounds
/// (PureGround), matching no clause head, is emptiness by absent
/// DECLARATION. The catalog proves it, so refuse with the declared
/// spellings instead of emitting a provably-empty query. A free
/// clause at the position (MixedGround) makes every call satisfiable
/// — no refusal can fire there; a data-borne argument (lvar,
/// expression) keeps relational semantics and misses to empty.
pub(super) fn refuse_provable_ground_miss(
    function: &str,
    scalar_spec: &ast_unresolved::Access,
    positions: &[HoPositionInfo],
) -> Result<()> {
    use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode};

    let ast_unresolved::Access::Slots(exprs) = scalar_spec else {
        return Ok(());
    };
    let scalar_positions: Vec<&HoPositionInfo> = positions
        .iter()
        .filter(|p| matches!(p.column_kind, HoColumnKind::Scalar))
        .collect();
    for (idx, pos) in scalar_positions.iter().enumerate() {
        if pos.ground_mode != HoGroundMode::PureGround {
            continue;
        }
        let Some(value) = exprs.get(idx).and_then(ast_unresolved::Slot::ground) else {
            continue;
        };
        let any_match = pos
            .ground_values
            .iter()
            .any(|(_, clause_val)| ground_literals_equal(&crate::pipeline::asts::core::LiteralValue::from_stored_ground(clause_val), value));
        if !any_match {
            let mut spellings: Vec<&str> =
                pos.ground_values.iter().map(|(_, s)| s.as_str()).collect();
            spellings.dedup();
            return Err(DelightQLError::validation_error_categorized(
                "grounding/head/provable_miss",
                format!(
                    "no clause of '{function}' grounds on '{arg}' at parameter {n} — \
                     emptiness by absent declaration is an error, not a result. \
                     Declared spellings: {list}. A data-borne value (a column, not \
                     a literal) misses to empty instead.",
                    arg = value.stored_ground(),
                    n = pos.position + 1,
                    list = spellings.join(", "),
                ),
                "ground-head selection",
            ));
        }
    }
    Ok(())
}

/// Equality for ground-head selection at compile time. Same-variant
/// byte equality; numbers compare by EXACT decimal value (the SQL
/// comparison the selection lowers to treats 5 and 5.0 as equal, and
/// distinguishes adjacent integers above 2^53 that any f64 road would
/// merge — a merged pair here passes the provable-miss check and then
/// misses in SQL, the silent empty the law forbids); differing
/// variants never match (an untyped injected column compares TEXT vs
/// INTEGER by type ordering, never equal).
/// R8's strict-landing refusal: the pipe binds position 1, and
/// something else occupies it.
/// A relation landed at a formal that cannot receive one.
///
/// THE POSITION SAYS WHICH FORMAL, and the position is all that is known: an
/// implicit landing and an authored `@` written first name the same formal,
/// so one refusal serves both and neither is told apart by the glyph the
/// author used. The first position teaches toward `@`, because moving the
/// landing is the remedy there; a later one was reached by an `@` already and
/// teaches toward the table parameter it should have named.
fn er_r8_landing_refusal(entity: &str, param: &str, position: usize) -> DelightQLError {
    let message = if position == 0 {
        format!(
            "the pipe lands at the first parameter of '{entity}', and '{param}' \
             occupies it — a relation can land only at a table parameter (T(*) \
             or T(cols)). write @ at the parameter that receives the pipe: \
             {entity}(…, @)"
        )
    } else {
        format!(
            "the pipe lands at '{param}', parameter {position} of '{entity}', and \
             '{param}' is scalar — a relation can land only at a table parameter \
             (T(*) or T(cols)). Supply the scalar and write @ at a table parameter"
        )
    };
    DelightQLError::validation_error_categorized(
        "resolution/ho/pipe_landing",
        message,
        "R8, strict: a piped relation lands at the first formal parameter, or at \
         exactly one explicit @ — never search, never displace",
    )
}

fn ground_literals_equal(
    a: &crate::pipeline::asts::core::LiteralValue,
    b: &crate::pipeline::asts::core::LiteralValue,
) -> bool {
    use crate::pipeline::asts::core::LiteralValue::*;
    match (a, b) {
        (Number(x), Number(y)) => match (normalize_number(x), normalize_number(y)) {
            (Some(p), Some(q)) => p == q,
            _ => x == y,
        },
        _ => a == b,
    }
}

/// Exact decimal value of a numeric spelling, as (sign, significant
/// digits, exponent) with value = sign × 0.<digits> × 10^exp; zero is
/// (0, "", 0). Arbitrary precision — no float on the road — so every
/// distinct value normalizes distinctly and every equal value ("12",
/// "12.0", "1.2e1") normalizes identically. None for spellings that
/// are not plain decimal/exponent numbers.
fn normalize_number(s: &str) -> Option<(i8, String, i64)> {
    let s = s.trim();
    let (sign, rest) = match s.as_bytes().first()? {
        b'-' => (-1i8, &s[1..]),
        b'+' => (1, &s[1..]),
        _ => (1, s),
    };
    let (mantissa, exp10) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits: String = int_part.chars().chain(frac_part.chars()).collect();
    let mut exp = int_part.len() as i64 + exp10;
    let stripped = digits.trim_start_matches('0');
    exp -= (digits.len() - stripped.len()) as i64;
    let stripped = stripped.trim_end_matches('0');
    if stripped.is_empty() {
        return Some((0, String::new(), 0));
    }
    Some((sign, stripped.to_string(), exp))
}

/// Wrap a Query's main expression with a pipe operator.
///
/// Wrap the query's BODY with a pipe operator; the bindings ride along.
fn wrap_query_with_pipe(
    mut query: ast_unresolved::Query,
    operator: ast_unresolved::PipeOp,
) -> ast_unresolved::Query {
    query.body = query.body.then(ast_unresolved::Continuation::Pipe {
        operator,
        named: None,
        cpr_schema: (),
    });
    query
}

/// Split first-parens Access into table bindings and scalar Access.
///
/// For each position in `entity.params`:
/// - Table param (Glob/Argumentative): extract value from first_parens, put in HoParamBindings
/// - Scalar param (Scalar/GroundScalar): leave in the scalar Access for PatternResolver
/// - @ (PipeLanding): mark that position as pipe target
///
/// Returns: (table_bindings, scalar_access, pipe_target_index)
/// The terms of a call's FIRST parens, in written order — `None` when the
/// group supplies no argument to match against a formal (nothing written,
/// or the whole-operand glob).
///
/// A table argument matches by the name it mentions; that is what the
/// formal is bound to.
fn relation_term(relation: &ast_unresolved::Chain) -> Option<ast_unresolved::DomainExpression> {
    match relation.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named { identifier, .. },
            ..
        }) => Some(
            ast_unresolved::DomainExpression::lvar_builder(identifier.name.to_string()).build(),
        ),
        // A RELATION THAT IS NOT A NAME HAS NO TERM. Saying so is
        // the whole of what this position knows; inventing an lvar
        // for it would put a spelling nobody wrote into the body,
        // where it refuses as a missing column or captures a real
        // one that happens to share the name.
        _ => None,
    }
}

/// The relation a member supplies, when the group's member at `index` is one.
fn member_relation(
    arguments: &ast_unresolved::CallArguments,
    index: usize,
) -> Option<&ast_unresolved::Chain> {
    arguments.ho_members().nth(index)?.relation()
}

/// One first-parens member as the formal-matching loop reads it: a value
/// term, a relation that names nothing a formal could bind, or one of the
/// two structural row marks.
#[derive(Debug)]
enum HoTerm {
    Term(ast_unresolved::DomainExpression),
    Opaque,
    Landing,
    Skip,
}

impl HoTerm {
    fn term(&self) -> Option<&ast_unresolved::DomainExpression> {
        match self {
            Self::Term(term) => Some(term),
            Self::Opaque | Self::Landing | Self::Skip => None,
        }
    }
}

fn first_parens_terms(arguments: &ast_unresolved::CallArguments) -> Option<Vec<HoTerm>> {
    use crate::pipeline::asts::core::operators::{CallArguments, HoArgument, ScalarArgument};
    let as_term = |term: Option<ast_unresolved::DomainExpression>| match term {
        Some(term) => HoTerm::Term(term),
        None => HoTerm::Opaque,
    };
    match arguments {
        CallArguments::None => None,
        CallArguments::HigherOrder(part) => Some(
            part.members
                .iter()
                .map(|argument| match argument {
                    HoArgument::Relation(relation) => as_term(relation_term(relation)),
                    HoArgument::Value(value) => as_term(value.domain().cloned()),
                    HoArgument::Landing(_) => HoTerm::Landing,
                    HoArgument::Skip => HoTerm::Skip,
                })
                .collect(),
        ),
        CallArguments::Scalar(members) => {
            if members.is_empty() {
                return None;
            }
            // AN ENUMERATION IS NOT A TERM. A lone whole-operand glob is
            // the group asking for everything, not an argument to match.
            if matches!(
                members.as_slice(),
                [ScalarArgument::Spread(
                    crate::pipeline::asts::core::Spread::Glob(_)
                )]
            ) {
                return None;
            }
            Some(
                members
                    .iter()
                    .map(|member| match member {
                        ScalarArgument::Value(value) => as_term(value.domain().cloned()),
                        // A callable's BODY is the term the callee applies.
                        ScalarArgument::Callable(ast_unresolved::Callable::Lambda(lambda)) => {
                            as_term(Some((*lambda.body).clone()))
                        }
                        ScalarArgument::Callable(_) => HoTerm::Opaque,
                        ScalarArgument::Spread(_) | ScalarArgument::Star => HoTerm::Opaque,
                        ScalarArgument::Context(_) => HoTerm::Opaque,
                    })
                    .collect(),
            )
        }
    }
}

/// The value a lifted group carries, when the group is one row of one column.
///
/// `f(t(*) & 3)` is `f(t(*), _(3))` — the lift's own equivalence — so a
/// scalar formal standing after `&` is supplied by a relation. Only the
/// single-cell shape answers: a wider or taller relation is a relation, and
/// a scalar slot that quietly took its first cell would be guessing.
fn lifted_scalar(
    relation: Option<&ast_unresolved::Chain>,
) -> Option<ast_unresolved::DomainExpression> {
    let relation = relation?;
    let ast_unresolved::Grelex::Literal(table) = &relation.head else {
        return None;
    };
    if !relation.continuations.is_empty() || table.table.body.header.is_some() {
        return None;
    }
    if table.table.body.rows.len() != 1 || table.table.body.rows.first().len() != 1 {
        return None;
    }
    Some(table.table.body.rows.first().0.first().value())
}

/// The term a formal needs, or the refusal for the argument that has none.
///
/// A relation argument that is not a name — an anonymous table, an inner
/// relation, a call — has no term, and a skip mark computes nothing. The
/// formals that read a NAME or a VALUE say so here rather than reading an
/// invented lvar, which would refuse downstream under a spelling nobody
/// wrote or, worse, capture a real column that happened to share it.
fn require_term<'a>(
    term: &'a HoTerm,
    param: &crate::pipeline::asts::ddl::HoParam,
    entity: &str,
    position: usize,
) -> Result<&'a ast_unresolved::DomainExpression> {
    term.term().ok_or_else(|| {
        DelightQLError::validation_error_categorized(
            "resolution/ho/relational_argument",
            format!(
                "parameter '{}' of '{entity}' is supplied at position {position} by a \
                 relation expression, which names nothing this position can bind",
                param.name()
            ),
            "pass a named relation, or write the argument the parameter declares",
        )
    })
}

pub(super) fn split_ho_first_parens(
    entity: &crate::resolution::registry::ConsultedEntity,
    pipe_source: Option<&ast_unresolved::Chain>,
    arguments: &ast_unresolved::CallArguments,
    direct_pipe_target: Option<usize>,
    registry: &mut crate::resolution::EntityRegistry,
    caller_scope: Option<&str>,
) -> Result<(HoParamBindings, ast_unresolved::Access, Option<usize>)> {
    use crate::pipeline::asts::ddl::HoParam as RegParam;

    // Compute position analysis for MixedGround detection
    let positions = if !entity.positions.is_empty() {
        entity.positions.clone()
    } else {
        crate::ddl::reconstruct::group(&entity.definition)
            .as_ref()
            .map(build_ho_position_analysis)
            .unwrap_or_default()
    };

    // The FIRST parens are arguments, not dimensions: the terms this
    // function matches against formals are the ho_arguments themselves.
    // A group that wrote nothing, or wrote only a glob, supplies no
    // argument to match.
    let exprs = first_parens_terms(arguments);
    let exprs = match exprs {
        Some(exprs) => exprs,
        None => {
            // No explicit args — but if piped, bind first table param to pipe source
            let mut bindings = HoParamBindings::default();
            let mut pipe_target_idx = None;
            if pipe_source.is_some() && !entity.params.is_empty() {
                // STRICT LANDING: the pipe lands at the FIRST formal
                // parameter — never a search for a table parameter
                // elsewhere. A scalar first parameter refuses toward the
                // explicit spelling.
                if !entity
                    .params
                    .iter()
                    .any(|p| matches!(p, RegParam::Relation { .. }))
                {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "Higher-order view '{}' has no table-value parameter to receive pipe input \
                             (all parameters are scalar)",
                            entity.name
                        ),
                        "A piped HO view must have at least one table-value parameter (e.g. T(*)) \
                         as the target for the pipe input",
                    ));
                }
                if !matches!(entity.params[0], RegParam::Relation { .. }) {
                    return Err(er_r8_landing_refusal(
                        &entity.name,
                        &entity.params[0].name(),
                        0,
                    ));
                }
                pipe_target_idx = Some(0);
                let first_param = &entity.params[0];
                bind_pipe_carrier(&mut bindings, first_param, &registry.identities);
            }
            return Ok((bindings, ast_unresolved::Access::All, pipe_target_idx));
        }
    };

    let mut bindings = HoParamBindings::default();
    let mut scalar_exprs = Vec::new();
    let mut pipe_target_idx = None;
    let mut expr_idx = 0;

    // Check if any expression is @. If piped with no @, the first table param
    // gets the pipe source implicitly — we must skip it in the expr-matching loop.
    let at_count = exprs
        .iter()
        .filter(|e| matches!(e, HoTerm::Landing))
        .count();
    if pipe_source.is_some() && at_count > 1 {
        return Err(DelightQLError::validation_error_categorized(
            "resolution/ho/pipe_landing",
            format!(
                "one pipe, one landing — the call to '{}' writes {} placeholders; \
                 exactly one @ names the parameter that receives the pipe",
                entity.name, at_count
            ),
            "R8: a piped relation lands at the first formal parameter, or at \
             exactly one explicit @",
        ));
    }
    let has_at = at_count > 0 || direct_pipe_target.is_some();
    let implicit_pipe_target = pipe_source.is_some() && !has_at;

    // NOWHERE TO LAND AT ALL comes first, and does not depend on WHERE the
    // relation landed: a callee with no table parameter cannot receive one
    // at any position, and saying which formal it reached instead would
    // teach toward moving an `@` that has nowhere to move to.
    if pipe_source.is_some()
        && !entity.params.is_empty()
        && !entity
            .params
            .iter()
            .any(|p| matches!(p, RegParam::Relation { .. }))
    {
        return Err(DelightQLError::validation_error(
            format!(
                "Higher-order view '{}' has no table-value parameter to receive pipe input \
                 (all parameters are scalar)",
                entity.name
            ),
            "A piped HO view must have at least one table-value parameter (e.g. T(*)) \
             as the target for the pipe input",
        ));
    }

    // STRICT LANDING: the implicit landing is the FIRST formal parameter,
    // period — a supplied/scalar first parameter refuses toward the
    // explicit @ spelling; the landing never searches and never
    // displaces.
    if implicit_pipe_target
        && !entity.params.is_empty()
        && !matches!(entity.params[0], RegParam::Relation { .. })
    {
        return Err(er_r8_landing_refusal(
            &entity.name,
            &entity.params[0].name(),
            0,
        ));
    }

    for (param_idx, param) in entity.params.iter().enumerate() {
        // A direct substituted call records an explicit @ as metadata while
        // omitting the placeholder from its authored argument vector. The
        // formal still consumes no authored expression; validate its category
        // at the same typed boundary as the CST placeholder path.
        if direct_pipe_target == Some(param_idx) {
            if !matches!(param, RegParam::Relation { .. }) {
                return Err(er_r8_landing_refusal(
                    &entity.name,
                    &param.name(),
                    param_idx,
                ));
            }
            pipe_target_idx = Some(param_idx);
            bind_pipe_carrier(&mut bindings, param, &registry.identities);
            continue;
        }

        // Implicit pipe target (R8): the first parameter, no other.
        if implicit_pipe_target
            && pipe_target_idx.is_none()
            && param_idx == 0
            && matches!(param, RegParam::Relation { .. })
        {
            pipe_target_idx = Some(param_idx);
            bind_pipe_carrier(&mut bindings, param, &registry.identities);
            continue; // Don't consume an expr for this param
        }

        if expr_idx >= exprs.len() {
            break;
        }

        // A RELATION WITH NO TERM IS STILL AN ARGUMENT. It reaches the
        // formal through `ho_arguments`, which holds what the author wrote;
        // only the roads that need a NAME or a VALUE ask for the term, and
        // they refuse when there is none rather than reading an invention.
        let expr = &exprs[expr_idx];

        // Check for @ (explicit pipe target)
        if matches!(expr, HoTerm::Landing) {
            // R8 names the LANDING; only a table-valued parameter can
            // receive a relation. An @ at a scalar parameter would bind
            // the pipe nowhere — the carrier CTE emits but nothing
            // references it, the relation silently vanishes — so the
            // shape refuses instead.
            if !matches!(param, RegParam::Relation { .. }) {
                return Err(er_r8_landing_refusal(
                    &entity.name,
                    &param.name(),
                    param_idx,
                ));
            }
            if pipe_source.is_none() {
                return Err(DelightQLError::validation_error_categorized(
                    "resolution/ho/pipe_landing",
                    format!(
                        "the call to '{}' writes @ but nothing is piped into it — \
                         @ names the landing of a piped relation; supply the \
                         argument directly, or pipe a relation in with |>",
                        entity.name
                    ),
                    "R8: @ has meaning only when a relation is piped into the call",
                ));
            }
            pipe_target_idx = Some(param_idx);
            bind_pipe_carrier(&mut bindings, param, &registry.identities);
            expr_idx += 1;
            continue;
        }

        match param {
            RegParam::Relation {
                cols: crate::pipeline::asts::ddl::HeadItems::Glob,
                ..
            } => {
                // EVERY caller-authored Glob argument rides a carrier CTE —
                // bare references included: a fast path that name-substitutes
                // the reference INLINE into the entity's clause bodies would
                // resolve an enlisted rule or aliased reference in the
                // ENTITY's scope instead of the caller's, and would drop a
                // bare reference's namespace qualifier.
                // The carrier is Caller-owned, so the caller's scope serves
                // its own names. Carrier names are counter-uniquified: two
                // invocations in one query must not UNION-merge through the
                // same-name CTE machinery.
                if let Some(rel_expr) = member_relation(arguments, expr_idx) {
                    let plain_inline_name = match (
                        rel_expr.as_read_relation(),
                        rel_expr.head_access(),
                    ) {
                        (
                            Some(ast_unresolved::Relation::Ground {
                                mention: ast_unresolved::GroundMention::Named { identifier, .. },
                                ..
                            }),
                            Some(ast_unresolved::Access::All | ast_unresolved::Access::Unasked),
                        ) if identifier.namespace_path.is_empty() => {
                            let name = identifier.name.clone();
                            plain_arg_may_inline(&name, registry, caller_scope)?
                                .then_some(name.as_str().to_string())
                        }
                        _ => None,
                    };
                    if let Some(name) = plain_inline_name {
                        bindings.table_params.insert(param.name().to_string(), name);
                    } else {
                        // Normalize: unwrap InnerRelation, patch Bare→Glob so
                        // columns are visible for filter/projection resolution.
                        let normalized =
                            normalize_interior_for_cte(patch_bare_to_glob(rel_expr.clone()));
                        bind_glob_carrier(
                            &mut bindings,
                            param.name().as_str(),
                            normalized,
                            &registry.identities,
                        );
                    }
                } else {
                    // The piped invocation forms supply no ho_arguments, so the
                    // argument arrives as a DomainExpression — same inline
                    // decision; a carried name becomes the reference it
                    // denotes, resolved in the caller's scope.
                    match require_term(expr, param, &entity.name, param_idx)? {
                        ast_unresolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(AuthoredColumn { name, .. }),
                        )) => {
                            if plain_arg_may_inline(name, registry, caller_scope)? {
                                bindings
                                    .table_params
                                    .insert(param.name().to_string(), name.to_string());
                            } else {
                                let rel = bare_glob_reference(name.as_str());
                                bind_glob_carrier(
                                    &mut bindings,
                                    param.name().as_str(),
                                    rel,
                                    &registry.identities,
                                );
                            }
                        }
                        ast_unresolved::DomainExpression::Application(
                            ast_unresolved::FunctionApplication::Ground(
                                crate::pipeline::asts::core::LiteralValue::String(s),
                            ),
                        ) => {
                            if plain_arg_may_inline(
                                &delightql_types::SqlIdentifier::new(s.clone()),
                                registry,
                                caller_scope,
                            )? {
                                bindings
                                    .table_params
                                    .insert(param.name().to_string(), s.clone());
                            } else {
                                let rel = bare_glob_reference(s);
                                bind_glob_carrier(
                                    &mut bindings,
                                    param.name().as_str(),
                                    rel,
                                    &registry.identities,
                                );
                            }
                        }
                        _ => {
                            return Err(DelightQLError::validation_error(
                                format!(
                                    "Expected table name at position {} for param '{}', got {:?}",
                                    param_idx,
                                    param.name(),
                                    expr
                                ),
                                "Glob table parameter must be a table name or variable",
                            ));
                        }
                    }
                }
                expr_idx += 1;
            }
            RegParam::Relation {
                cols: crate::pipeline::asts::ddl::HeadItems::Listed(cols),
                ..
            } => {
                let columns: Vec<String> = cols.iter().map(|c| c.supply.spelling()).collect();
                let columns = &columns;
                if let Some(rel_expr) = member_relation(arguments, expr_idx) {
                    // THE LIFT'S ROWS ARE THE ARGUMENT. `f("a"; "b")` is
                    // `f(_("a"; "b"))` — the lift's own equivalence — and a
                    // declared-width parameter NAMES that relation's columns,
                    // so the headerless literal binds inline under the declared
                    // names. Behind a carrier CTE the rows become a reference,
                    // and every reader that needs the VALUES — the pivot's IN
                    // among them — sees a relation it cannot look inside.
                    if let Some(named) = lifted_rows_under_declared_names(rel_expr, columns) {
                        bindings
                            .table_expr_params
                            .insert(param.name().to_string(), named);
                        expr_idx += 1;
                        continue;
                    }
                    let normalized =
                        normalize_interior_for_cte(patch_bare_to_glob(rel_expr.clone()));
                    bind_glob_carrier(
                        &mut bindings,
                        param.name().as_str(),
                        normalized,
                        &registry.identities,
                    );
                    bindings
                        .argumentative_patterns
                        .insert(param.name().to_string(), columns.clone());
                    expr_idx += 1;
                    continue;
                }
                // Argumentative table param: either a table ref (Lvar) or scalar lift
                match require_term(expr, param, &entity.name, param_idx)? {
                    ast_unresolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(AuthoredColumn { name, .. }),
                    )) => {
                        // Table reference. Same inline-or-carrier decision as
                        // the Glob kind: a query-local CTE or physical table
                        // binds by name (the remap machinery reads its schema
                        // directly); anything else — an enlisted rule, an
                        // aliased or scoped reference — rides a Caller
                        // carrier, where the body's own positional access
                        // supplies the arity check and column binding that
                        // the by-name remap cannot (it looks the name up as
                        // CTE-or-table and a consulted rule misses both).
                        if plain_arg_may_inline(name, registry, caller_scope)? {
                            bindings
                                .table_params
                                .insert(param.name().to_string(), name.to_string());
                            bindings.argumentative_table_refs.push((
                                param.name().to_string(),
                                name.clone(),
                                columns.len(),
                                columns.clone(),
                            ));
                        } else {
                            let rel = bare_glob_reference(name.as_str());
                            bind_glob_carrier(
                                &mut bindings,
                                param.name().as_str(),
                                rel,
                                &registry.identities,
                            );
                            bindings
                                .argumentative_patterns
                                .insert(param.name().to_string(), columns.clone());
                        }
                        expr_idx += 1;
                    }
                    _ => {
                        // Scalar lift: consume rows of N exprs each and build anon table.
                        // Multiple rows arise from `;` separator: pivot_by("Maths";"Music").
                        //
                        // Explicit always wins: when scalar parameters FOLLOW the
                        // lifted rows, the row/scalar split is genuinely ambiguous
                        // and must be marked with `&` — it is never guessed: guessing
                        // would silently take exactly one row and let the rest fall
                        // to the scalars.
                        let later_scalar = entity.params[param_idx + 1..]
                            .iter()
                            .any(|p| !matches!(p, RegParam::Relation { .. }));
                        if later_scalar {
                            return Err(DelightQLError::validation_error_categorized(
                                "resolution/ho/lifted_boundary",
                                format!(
                                    "ambiguous lifted-relation boundary in '{}': inline rows for parameter '{}' are followed by scalar parameter(s), and the split cannot be guessed",
                                    entity.name, param.name()
                                ),
                                "mark where the rows end with & — e.g. f(\"a\", 1; \"b\", 2 & \"x\") — or pass a named relation instead of inline rows",
                            ));
                        }
                        let n_cols = columns.len();
                        let mut all_rows = Vec::new();

                        loop {
                            if expr_idx >= exprs.len() {
                                break;
                            }
                            // Check if the next expr is a literal (part of this row)
                            // or an Lvar (next param / end of scalar lift)
                            let next = &exprs[expr_idx];
                            let is_literal = matches!(
                                next,
                                HoTerm::Term(ast_unresolved::DomainExpression::Application(
                                    ast_unresolved::FunctionApplication::Ground(_)
                                ))
                            );
                            if !is_literal && all_rows.is_empty() {
                                // First value is not a literal — error
                                return Err(DelightQLError::validation_error(
                                    format!(
                                        "Argumentative param '{}' expects literal values for scalar lift, \
                                         got {:?}",
                                        param.name(), next
                                    ),
                                    "Scalar lift values must be literals",
                                ));
                            }
                            if !is_literal {
                                // Non-literal after at least one row → stop consuming
                                break;
                            }

                            let mut row_values = Vec::with_capacity(n_cols);
                            for col_idx in 0..n_cols {
                                if expr_idx + col_idx >= exprs.len() {
                                    return Err(DelightQLError::validation_error(
                                        format!(
                                            "Argumentative param '{}' expects {} values per row, \
                                             but only {} remain at position {}",
                                            param.name(),
                                            n_cols,
                                            exprs.len() - expr_idx,
                                            param_idx
                                        ),
                                        "Not enough values for argumentative scalar lift row",
                                    ));
                                }
                                let val_expr = &exprs[expr_idx + col_idx];
                                let value = match require_term(
                                    val_expr,
                                    param,
                                    &entity.name,
                                    param_idx,
                                )? {
                                    ast_unresolved::DomainExpression::Application(ast_unresolved::FunctionApplication::Ground(value @ (crate::pipeline::asts::core::LiteralValue::String(
                                                _,
                                            )
                                            | crate::pipeline::asts::core::LiteralValue::Number(
                                                _,
                                            )))) => value.clone(),
                                    other => {
                                        return Err(DelightQLError::validation_error(
                                            format!(
                                                "Unsupported expression in scalar lift for param '{}' column {}: {:?}",
                                                param.name(), col_idx, other
                                            ),
                                            "Scalar lift values must be literals",
                                        ));
                                    }
                                };
                                row_values.push(value);
                            }
                            expr_idx += n_cols;
                            all_rows.push(row_values);
                        }

                        if all_rows.is_empty() {
                            return Err(DelightQLError::validation_error(
                                format!(
                                    "Argumentative param '{}' got no values for scalar lift",
                                    param.name(),
                                ),
                                "No values for argumentative scalar lift",
                            ));
                        }

                        let anon_table = lift_scalars_to_anonymous_table(columns, &all_rows)?;
                        bindings
                            .table_expr_params
                            .insert(param.name().to_string(), anon_table);
                    }
                }
            }
            RegParam::Scalar { .. } => {
                // Check if this position is MixedGround — needs BOTH text substitution
                // AND PatternResolver filtering
                let is_mixed_ground = positions.iter().any(|pi| {
                    pi.position == param_idx
                        && pi.ground_mode == crate::pipeline::asts::ddl::HoGroundMode::MixedGround
                });

                // THE DESCRIPTOR DECIDES WHAT A LIFTED GROUP IS. `&` bounds
                // arguments and dissolves into an anonymous relation, so a
                // scalar written after it arrives here as a one-row, one-column
                // relation. The formal says it is a scalar, and that is the
                // set-at-a-time reading: the row IS the value. Left as the
                // relation, the parameter would carry a placeholder spelling
                // into the body and refuse there under a name nobody wrote.
                let expr = match lifted_scalar(member_relation(arguments, expr_idx)) {
                    Some(value) => value,
                    None => require_term(expr, param, &entity.name, param_idx)?.clone(),
                };

                // Text substitution for free-variable clauses
                bindings
                    .scalar_params
                    .insert(param.name().to_string(), expr.clone());

                if is_mixed_ground {
                    // MixedGround: also add to scalar_exprs for PatternResolver
                    scalar_exprs.push(expr.clone());
                }
                expr_idx += 1;
            }
            RegParam::Ground { .. } => {
                // A ground position goes to PatternResolver via scalar_exprs
                scalar_exprs.push(require_term(expr, param, &entity.name, param_idx)?.clone());
                expr_idx += 1;
            }
        }
    }

    // If piped but no table-value parameter was found, error out
    if pipe_source.is_some() && pipe_target_idx.is_none() {
        return Err(DelightQLError::validation_error(
            format!(
                "Higher-order view '{}' has no table-value parameter to receive pipe input \
                 (all parameters are scalar)",
                entity.name
            ),
            "A piped HO view must have at least one table-value parameter (e.g. T(*)) \
             as the target for the pipe input",
        ));
    }

    let scalar_spec = match Vec1::try_from_vec(
        scalar_exprs
            .into_iter()
            .map(ast_unresolved::Slot::classify)
            .collect(),
    ) {
        Some(slots) => ast_unresolved::Access::Slots(slots),
        None => ast_unresolved::Access::All,
    };

    Ok((bindings, scalar_spec, pipe_target_idx))
}

fn bind_relation_carrier(
    bindings: &mut crate::pipeline::query_features::HoParamBindings,
    param_name: &str,
    expr: ast_unresolved::Chain,
    role: crate::names::HoRole,
    identities: &crate::names::Registry,
) -> crate::names::ScopeId {
    let scope = identities.mint_derived_scope(
        crate::names::ScopeOrigin::HoCarrier { role },
        crate::names::Hint::Prefix("ho"),
    );
    bindings
        .table_scope_params
        .insert(param_name.to_string(), scope);
    bindings
        .interior_ctes
        .push((param_name.to_string(), scope, expr));
    scope
}

/// Bind a table argument to a fresh caller-owned structural CTE.
fn bind_glob_carrier(
    bindings: &mut crate::pipeline::query_features::HoParamBindings,
    param_name: &str,
    expr: ast_unresolved::Chain,
    identities: &crate::names::Registry,
) {
    bind_relation_carrier(
        bindings,
        param_name,
        expr,
        crate::names::HoRole::Argument,
        identities,
    );
}

/// Bind a piped source without creating a query-local character key.
fn bind_pipe_carrier(
    bindings: &mut crate::pipeline::query_features::HoParamBindings,
    param: &crate::resolution::registry::HoParamInfo,
    identities: &crate::names::Registry,
) {
    let scope = identities.mint_derived_scope(
        crate::names::ScopeOrigin::HoCarrier {
            role: crate::names::HoRole::PipeSource,
        },
        crate::names::Hint::Prefix("ho"),
    );
    bindings
        .table_scope_params
        .insert(param.name().to_string(), scope);
    if let crate::pipeline::asts::ddl::HoParam::Relation {
        cols: crate::pipeline::asts::ddl::HeadItems::Listed(cols),
        ..
    } = param
    {
        bindings.argumentative_patterns.insert(
            param.name().to_string(),
            cols.iter().map(|c| c.supply.spelling()).collect(),
        );
    }
    bindings.pipe_carrier = Some((param.name().to_string(), scope));
}

/// May a plain HO argument NAME be inlined into the entity's clause bodies,
/// or must it ride a Caller carrier? Inline keeps the caller's spelling in
/// name-reifying positions (`^T` meta-izes the TABLE NAME as data), so it is
/// preserved exactly where it has always been correct: at the PROMPT, for a
/// name that is a query-local CTE (scope-free) or a physical table (served
/// identically in the entity's scope by the ambient-data fallback) — or an
/// unknown (its refusal should keep the caller's spelling too). A consulted
/// RULE, an alias, any qualified reference, or ANY name authored inside a
/// definition resolves scope-DEPENDENTLY and must carry the caller's scope.
fn plain_arg_may_inline(
    name: &delightql_types::SqlIdentifier,
    registry: &mut crate::resolution::EntityRegistry,
    caller_scope: Option<&str>,
) -> Result<bool> {
    if caller_scope.is_some() {
        return Ok(false);
    }
    use crate::resolution::{resolve_entity_with_alias, ResolutionResult};
    Ok(matches!(
        resolve_entity_with_alias(name, None, registry, None)?,
        ResolutionResult::CTE(_)
            | ResolutionResult::MaterializedRelation(_)
            | ResolutionResult::DatabaseEntity(_)
            | ResolutionResult::BuiltInFunction { .. }
            // Both of these REFUSE where they land, and a refusal reads
            // better under the name the caller wrote than under an internal
            // carrier's.
            | ResolutionResult::DefinedNonRelation { .. }
            | ResolutionResult::RuntimeServedRelation { .. }
            | ResolutionResult::Unknown(_)
    ))
}

/// A `name(*)` reference for a name-only HO argument. The name is the
/// caller's text, so the carrier resolves it in the caller's scope.
fn bare_glob_reference(name: &str) -> ast_unresolved::Chain {
    ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                identifier: ast_unresolved::QualifiedName {
                    namespace_path: ast_unresolved::NamespacePath::empty(),
                    name: name.into(),
                },
                alias: None,
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
            cpr_schema: (),
        },
        ast_unresolved::Access::All,
        (),
    )
}

/// Normalize an interior table expression for use as a CTE source.
///
/// Handles three cases:
/// 1. InnerRelation(Indeterminate { subquery }) → the subquery directly,
///    patching the innermost Ground from Bare to Glob.
/// 2. Ground with Positional access → Ground(Glob) piped through
///    a projection (SELECT col1, col2 FROM table).
/// 3. Everything else → pass through unchanged.
fn normalize_interior_for_cte(mut expr: ast_unresolved::Chain) -> ast_unresolved::Chain {
    if expr.has_steps() {
        return expr;
    }
    if let ast_unresolved::Grelex::Reference(ast_unresolved::Relation::InnerRelation {
        pattern: ast_unresolved::InnerRelationPattern::Indeterminate { subquery, .. },
        ..
    }) = expr.head
    {
        // Unwrap the InnerRelation and patch the base to Glob
        return patch_bare_to_glob(*subquery);
    }
    // Convert a caller pattern to Glob + Projection pipe:
    // users(first_name, age) → users(*) |> (first_name, age)
    if !matches!(
        expr.head,
        ast_unresolved::Grelex::Reference(ast_unresolved::Relation::Ground { .. })
    ) {
        return expr;
    }
    let Some(ast_unresolved::Access::Slots(slots)) = expr.head_access() else {
        return expr;
    };
    // A slot's term becomes a publication item that names nothing: the
    // access already said which columns, and it named none of them.
    let projection_items: Vec<_> = slots
        .iter()
        .filter_map(ast_unresolved::Slot::term)
        .map(|term| ast_unresolved::OutItem::plain(term, ()))
        .collect();
    let Some(ast_unresolved::Continuation::Access { access, .. }) = expr.continuations.first_mut()
    else {
        unreachable!("head_access answered with a leading access")
    };
    *access = ast_unresolved::Access::All;
    expr.then(ast_unresolved::Continuation::Pipe {
        operator: ast_unresolved::PipeOp::Project(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(projection_items)
                .expect("a receipt projection carries the terms it was built from"),
        ),
        named: None,
        cpr_schema: (),
    })
}

/// Patch a read's `Access::Unasked` to `Access::All`. The steps above it are
/// untouched: the access is the head's own.
fn patch_bare_to_glob(mut expr: ast_unresolved::Chain) -> ast_unresolved::Chain {
    if matches!(expr.head_access(), Some(ast_unresolved::Access::Unasked)) {
        if let Some(ast_unresolved::Continuation::Access { access, .. }) =
            expr.continuations.first_mut()
        {
            *access = ast_unresolved::Access::All;
        }
    }
    expr
}

/// Ensure all HO position infos have column names.
/// For Scalar (free-variable) positions, use the DDL param variable name.
/// For PureGround (all-literal) positions, generate `_label_N`.
pub(super) fn ensure_position_column_names(
    positions: Vec<HoPositionInfo>,
    clauses: &[Clause],
) -> Vec<HoPositionInfo> {
    positions
        .into_iter()
        .map(|mut pi| {
            if pi.column_name.is_none() {
                for clause in clauses {
                    if let Some(HoParam::Scalar { name, .. }) = clause.params().get(pi.position) {
                        pi.column_name = Some(name.to_string());
                        break;
                    }
                }
                if pi.column_name.is_none() {
                    pi.column_name = Some(format!("_label_{}", pi.position));
                }
            }
            pi
        })
        .collect()
}

/// Extract CTE bindings from a clause query: the clause's own bindings
/// hoist into the outer list, its definitions collect, and its body
/// becomes the squished CTE.
fn extract_clause_ctes(
    clause_query: ast_unresolved::Query,
    function: &str,
    all_ctes: &mut Vec<ast_unresolved::CteBinding>,
    collected_cfes: &mut Vec<ast_unresolved::CfeDefinition>,
) {
    let ast_unresolved::Query { cfes, ctes, body } = clause_query;
    collected_cfes.extend(cfes);
    all_ctes.extend(ctes);
    all_ctes.push(ast_unresolved::CteBinding {
        subject: crate::pipeline::asts::core::CteSubject::Authored {
            name: delightql_types::SqlIdentifier::new(function),
            effect: crate::pipeline::asts::core::CteEffectDeclaration::Pure,
        },
        authority: crate::pipeline::asts::core::CteAuthority {
            head: crate::pipeline::asts::core::definitions::Head::glob(),
            origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
            // The ENTITY's own clause body: its terms are the entity
            // file's text, so its file-local aliases must resolve in
            // the entity's scope.
            resolution_owner: crate::pipeline::asts::core::provenance::CteResolutionOwner::Entity,
        },
        expression: body,
        recursion: (),
    });
}

/// Inject a cross-join with the input table into a clause body's FROM clause.
/// When an invocation supplies a caller lvar, every clause receives the caller
/// row: free heads bind it and ground heads filter it before clauses merge.
///
/// Wraps the body with a direct read of the caller input occurrence.
fn inject_input_table_into_query(
    clause_query: ast_unresolved::Query,
    input_scope: crate::names::ScopeId,
    input_condition: Option<ast_unresolved::TruthExpression>,
) -> ast_unresolved::Query {
    let input_table = ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Plan {
                scope: input_scope,
                authored_name: None,
                alias: None,
            },
            outer: false,
            cpr_schema: (),
        },
        ast_unresolved::Access::All,
        (),
    );
    let input_table = if let Some(condition) = input_condition {
        input_table.then(ast_unresolved::Continuation::Restrict {
            condition: condition,
            origin: crate::pipeline::asts::core::FilterOrigin::HoGroundScalar,
            cpr_schema: (),
        })
    } else {
        input_table
    };

    let mut clause_query = clause_query;
    clause_query.body = input_table.then(ast_unresolved::Continuation::Member {
        rhs: clause_query.body,
        correlation: None,
        join_type: Some(crate::pipeline::asts::core::JoinType::Inner),
        cpr_schema: (),
    });
    clause_query
}

/// Build the constraint for a ground clause head against a caller lvar.
///
/// After UNION, a mixed position has one column identity even though one arm
/// bound the caller value and another arm supplied a ground discriminator.
/// Applying this predicate per arm preserves that distinction.
fn ground_scalar_correlation_condition(
    clause_params: &[HoParam],
    positions: &[HoPositionInfo],
    caller_scalar_params: &std::collections::HashMap<String, ast_unresolved::DomainExpression>,
) -> Option<ast_unresolved::TruthExpression> {
    let conditions: Vec<_> = positions
        .iter()
        .filter_map(|position| {
            let clause_param = clause_params.get(position.position)?;
            let HoParam::Ground { text: value, .. } = clause_param else {
                return None;
            };
            let caller = position
                .column_name
                .as_ref()
                .and_then(|name| caller_scalar_params.get(name))?;
            if !matches!(
                caller,
                ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                    AuthoredColumn { .. }
                )))
            ) {
                return None;
            }
            Some(ast_unresolved::TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left: Box::new(caller.clone()),
                right: Box::new(ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(crate::pipeline::asts::core::LiteralValue::from_stored_ground(value)),
                )),
            }))
        })
        .collect();
    ast_unresolved::TruthExpression::all(conditions)
}

/// Build the SQUISHED relation: ALL clauses as a UNION ALL, with scalar
/// positions injected as columns. Ground-head constraints against caller
/// lvars are applied within their clause; call-site literal filtering remains
/// a PatternResolver concern after the clauses merge.
///
/// Returns an unresolved Query with CTEs: one per clause (named `function`),
/// plus an optional pipe source CTE. The main query is `function(*)`.
pub(super) fn build_squished_relation(
    function: &str,
    entity: &crate::resolution::registry::ConsultedEntity,
    table_bindings: crate::pipeline::query_features::HoParamBindings,
    pipe_source_cte: Option<(String, crate::names::ScopeId, ast_unresolved::Chain)>,
    join_input_cte: Option<(crate::names::ScopeId, ast_unresolved::Chain)>,
    data_ns: Option<&ast_unresolved::NamespacePath>,
    caller_resolution_namespace: Option<String>,
) -> Result<ast_unresolved::Query> {
    let group = crate::ddl::reconstruct::group(&entity.definition).ok();

    let positions = if !entity.positions.is_empty() {
        entity.positions.clone()
    } else {
        group
            .as_ref()
            .map(build_ho_position_analysis)
            .unwrap_or_default()
    };

    let defs: &[Clause] = group.as_ref().map_or(&[], |g| g.clauses());
    let positions = ensure_position_column_names(positions, defs);

    let mut all_ctes = Vec::new();

    // Structural carrier reads carry a plan mention, so no compiler spelling
    // needs an exemption from data-namespace patching.
    let local_cte_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Prepend pipe source CTE if present
    if let Some((formal_name, scope, source_expr)) = pipe_source_cte {
        // The formal's spelling stays diagnostic vocabulary in
        // `interior_ctes`; the binding itself stands on the carrier scope.
        let _ = formal_name;
        all_ctes.push(ast_unresolved::CteBinding {
            expression: source_expr,
            subject: crate::pipeline::asts::core::CteSubject::Structural(scope),
            authority: crate::pipeline::asts::core::CteAuthority {
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                // Caller-authored terms (the piped source): resolve under the
                // CALLER's scope.
                resolution_owner:
                    crate::pipeline::asts::core::provenance::CteResolutionOwner::Caller {
                        resolution_namespace: caller_resolution_namespace.clone(),
                    },
            },
            recursion: (),
        });
    }

    // Prepend interior CTEs for table arguments with interior conditions.
    // Apply data namespace patching so table refs inside resolve correctly.
    for (formal_name, scope, source_expr) in &table_bindings.interior_ctes {
        let patched_expr = if let Some(dns) = data_ns {
            patch_data_ns_in_relational_expr(source_expr.clone(), dns, &local_cte_names)
        } else {
            source_expr.clone()
        };
        let _ = formal_name;
        all_ctes.push(ast_unresolved::CteBinding {
            expression: patched_expr,
            subject: crate::pipeline::asts::core::CteSubject::Structural(*scope),
            authority: crate::pipeline::asts::core::CteAuthority {
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                // Caller-authored HO argument (interior condition at the
                // call site): the caller's scope owns its names.
                resolution_owner:
                    crate::pipeline::asts::core::provenance::CteResolutionOwner::Caller {
                        resolution_namespace: caller_resolution_namespace.clone(),
                    },
            },
            recursion: (),
        });
    }

    // Prepend the caller-input carrier for free scalar params.
    // The scope is injected directly into correlated clause bodies.
    let join_input_scope = if let Some((scope, source_expr)) = join_input_cte {
        all_ctes.push(ast_unresolved::CteBinding {
            expression: source_expr,
            subject: crate::pipeline::asts::core::CteSubject::Structural(scope),
            authority: crate::pipeline::asts::core::CteAuthority {
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                // The caller's scope owns names in the caller-authored input.
                resolution_owner:
                    crate::pipeline::asts::core::provenance::CteResolutionOwner::Caller {
                        resolution_namespace: caller_resolution_namespace.clone(),
                    },
            },
            recursion: (),
        });
        Some(scope)
    } else {
        None
    };

    let mut collected_cfes: Vec<ast_unresolved::CfeDefinition> = Vec::new();

    if defs.len() > 1 {
        // Multi-clause: each clause becomes a CTE
        for def in defs {
            let clause_params = def.params().to_vec();
            let output_head = def.head_items();

            // Create per-clause bindings: for GroundScalar positions that are Scalar
            // in this clause, bind the ground value as a scalar param.
            let clause_bindings = table_bindings.clone();
            let clause_query = {
                let q = crate::ddl::reconstruct::bound_body(&def.full_source, clause_bindings)?;
                // Inject the caller input before inject_scalar_columns,
                // so the embed pipe wraps the join (not vice versa). This ensures
                // anonymous tables with column refs are on the right side of a join
                // where the MeltTable/json_each strategy can handle them.
                let q = if let Some(input_scope) = join_input_scope {
                    // Every clause participates in row-by-row dispatch. A
                    // ground-only clause still needs the caller row so its
                    // discriminator can be compared without joining the
                    // carrier a second time above the union.
                    let condition = ground_scalar_correlation_condition(
                        &clause_params,
                        &positions,
                        &table_bindings.scalar_params,
                    );
                    inject_input_table_into_query(q, input_scope, condition)
                } else {
                    q
                };
                inject_scalar_columns(
                    q,
                    &clause_params,
                    &positions,
                    output_head,
                    &table_bindings.scalar_params,
                    join_input_scope.is_some(),
                )
            };
            let clause_query = if let Some(dns) = data_ns {
                patch_data_ns_query(clause_query, dns, &local_cte_names)
            } else {
                clause_query
            };

            extract_clause_ctes(clause_query, function, &mut all_ctes, &mut collected_cfes);
        }
    } else {
        // Single clause — cannot be MixedGround (mixing needs two
        // clauses), so the caller-scalar snapshot is only signature
        // parity.
        let caller_scalar_params = table_bindings.scalar_params.clone();
        let clause_query = {
            let q = crate::ddl::reconstruct::bound_body(&entity.definition, table_bindings)?;
            if let Some(def) = defs.first() {
                let clause_params = def.params().to_vec();
                let output_head = def.head_items();
                // Inject the caller input before inject_scalar_columns,
                // so the embed pipe wraps the join (not vice versa). This ensures
                // anonymous tables with column refs are on the right side of a join
                // where the MeltTable/json_each strategy can handle them.
                let q = if let Some(input_scope) = join_input_scope {
                    let condition = ground_scalar_correlation_condition(
                        &clause_params,
                        &positions,
                        &caller_scalar_params,
                    );
                    inject_input_table_into_query(q, input_scope, condition)
                } else {
                    q
                };
                inject_scalar_columns(
                    q,
                    &clause_params,
                    &positions,
                    output_head,
                    &caller_scalar_params,
                    join_input_scope.is_some(),
                )
            } else {
                q
            }
        };
        let clause_query = if let Some(dns) = data_ns {
            patch_data_ns_query(clause_query, dns, &local_cte_names)
        } else {
            clause_query
        };

        extract_clause_ctes(clause_query, function, &mut all_ctes, &mut collected_cfes);
    }

    // Main query: function(*) referencing the CTE
    let main_query = ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                identifier: ast_unresolved::QualifiedName {
                    namespace_path: ast_unresolved::NamespacePath::empty(),
                    name: function.into(),
                },
                alias: None,
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
            cpr_schema: (),
        },
        ast_unresolved::Access::All,
        (),
    );

    Ok(ast_unresolved::Query {
        cfes: collected_cfes,
        ctes: all_ctes,
        body: main_query,
    })
}

/// Result of binding call-site arguments to HO view parameters using kind metadata.
pub(crate) use crate::pipeline::query_features::HoParamBindings;

fn bind_proffer_scope(
    bindings: &mut HoParamBindings,
    param_name: &str,
    identities: &crate::names::Registry,
) {
    let scope = identities.mint_derived_scope(
        crate::names::ScopeOrigin::HoCarrier {
            role: crate::names::HoRole::Proffer,
        },
        crate::names::Hint::Prefix("ho"),
    );
    bindings
        .table_scope_params
        .insert(param_name.to_string(), scope);
}

/// Create structural proffer bindings for an HO view's parameters.
///
/// Used at consult time to parse the view body with placeholder values,
/// enabling early validation of syntax and structure without real call-site args.
pub(crate) fn create_proffer_bindings(
    head: &crate::pipeline::asts::ddl::Head,
    identities: &crate::names::Registry,
) -> HoParamBindings {
    let mut bindings = HoParamBindings::default();
    for param in head.ho_params.as_deref().unwrap_or_default() {
        match param {
            HoParam::Relation {
                name,
                cols: HeadItems::Glob,
            } => {
                bind_proffer_scope(&mut bindings, name.as_str(), identities);
            }
            HoParam::Relation {
                name,
                cols: HeadItems::Listed(items),
            } => {
                let columns: Vec<String> = items.iter().map(|i| i.supply.spelling()).collect();
                let null_row: Vec<crate::pipeline::asts::core::LiteralValue> = columns
                    .iter()
                    .map(|_| crate::pipeline::asts::core::LiteralValue::Null)
                    .collect();
                match lift_scalars_to_anonymous_table(&columns, &[null_row]) {
                    Ok(anon) => {
                        bindings.table_expr_params.insert(name.to_string(), anon);
                    }
                    Err(_) => {
                        bind_proffer_scope(&mut bindings, name.as_str(), identities);
                    }
                }
            }
            HoParam::Scalar { name, .. } => {
                bindings.scalar_params.insert(
                    name.to_string(),
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Ground(
                            crate::pipeline::asts::core::LiteralValue::Null,
                        ),
                    ),
                );
                bind_proffer_scope(&mut bindings, name.as_str(), identities);
            }
            HoParam::Ground { name, text } => {
                // A ground position is a constant, not a parameter.
                bindings.scalar_params.insert(
                    name.to_string(),
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Ground(
                            crate::pipeline::asts::core::LiteralValue::Null,
                        ),
                    ),
                );
                bindings.table_params.insert(name.to_string(), text.clone());
            }
        }
    }
    bindings
}

/// Synthesize an anonymous table `_(col1, col2 ---- v1, v2; v3, v4)` from column names and rows.
///
/// Routes through the DQL body parser — no mini-pipeline.
/// The lift's rows, headed by the names the parameter declares.
///
/// `None` for anything that is not a bare headerless literal: a relation the
/// author named, an interior, a membership form, or a table that already
/// carries its own header row. Those bind through the carrier, where a
/// reference has a scope to resolve in; only a self-contained literal can
/// stand in the body under a heading the DECLARATION supplies.
///
/// Widths that disagree are left alone, so the arity check reports the
/// mismatch against the relation the author wrote rather than against a
/// silently repaired one.
fn lifted_rows_under_declared_names(
    relation: &ast_unresolved::Chain,
    columns: &[String],
) -> Option<ast_unresolved::Chain> {
    if !relation.continuations.is_empty() {
        return None;
    }
    let ast_unresolved::Grelex::Literal(table) = &relation.head else {
        return None;
    };
    if table.table.body.header.is_some() || table.alias.is_some() || table.outer {
        return None;
    }
    if table
        .table
        .body
        .rows
        .iter()
        .any(|row| row.len() != columns.len())
    {
        return None;
    }
    let headers: Vec<ast_unresolved::DomainExpression> = columns
        .iter()
        .map(|name| ast_unresolved::DomainExpression::lvar_builder(name.clone()).build())
        .collect();
    let mut headed = table.clone();
    headed.table.body.header = Some(crate::pipeline::asts::core::TabularRow(Box::new(
        crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
            headers
                .into_iter()
                .map(|term| crate::pipeline::asts::core::HeaderItem {
                    slot: crate::pipeline::asts::core::Slot::classify(term),
                    sparse: false,
                })
                .collect(),
        )
        .expect("a declared heading is nonempty"),
    )));
    Some(ast_unresolved::Chain::ground(
        ast_unresolved::Grelex::Literal(headed),
    ))
}

/// The anonymous table a lifted argument becomes: named columns and one row
/// per supplied tuple.
///
/// BUILT, NOT SPELLED. The values arrive as literals and the table is a
/// carrier; rendering them into `_(col ---- val)` text and parsing that back
/// would put a round trip through the grammar in the middle of a construction
/// that already has everything it needs — and would have to re-quote every
/// value correctly to survive it.
pub(crate) fn lift_scalars_to_anonymous_table(
    column_names: &[String],
    rows: &[Vec<crate::pipeline::asts::core::LiteralValue>],
) -> Result<ast_unresolved::Chain> {
    if let Some(row) = rows.iter().find(|row| row.len() != column_names.len()) {
        return Err(DelightQLError::parse_error(format!(
            "a lifted row carries {} value(s); the heading names {}",
            row.len(),
            column_names.len()
        )));
    }
    let column_headers = Some(
        column_names
            .iter()
            .map(|name| ast_unresolved::DomainExpression::lvar_builder(name.clone()).build())
            .collect(),
    );
    let rows = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| {
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Ground(value.clone()),
                    )
                })
                .collect()
        })
        .collect::<Vec<_>>();
    let table = crate::pipeline::asts::core::AnonTable::from_values(column_headers, rows, ())
        .ok_or_else(|| {
            DelightQLError::parse_error("a lifted table has a nonempty heading and body")
        })?;
    Ok(ast_unresolved::Chain::ground(
        ast_unresolved::Grelex::Literal(crate::pipeline::asts::core::AnonRelation::plain(table)),
    ))
}

/// Validate arity for argumentative params that received table references.
///
/// Argumentative params declare exact width: `V(k, l)` means the passed table
/// must have exactly 2 columns. This checks pending arity constraints against
/// the registry (CTEs, ground tables).
pub(super) fn validate_argumentative_arity(
    bindings: &HoParamBindings,
    registry: &crate::resolution::EntityRegistry,
) -> Result<()> {
    for (param_name, table_name, expected_cols, col_names) in &bindings.argumentative_table_refs {
        // Try CTE first, then ground table
        let actual_cols = if let Some(scope) = registry.query_local.lookup_cte(table_name) {
            Some(registry.identities.known_heading(*scope)?.len())
        } else if let Some(scope) = registry.database.lookup_table(table_name.as_str())? {
            Some(registry.identities.known_heading(scope)?.len())
        } else {
            // Table not found here — will fail during resolution with a "table not found" error
            None
        };

        if let Some(actual) = actual_cols {
            if actual != *expected_cols {
                return Err(DelightQLError::validation_error_categorized(
                    "constraint/ho_param/argumentative_functor/arity",
                    format!(
                        "Argumentative parameter '{}({})' expects {} column{} but table '{}' has {}",
                        param_name,
                        col_names.join(", "),
                        expected_cols,
                        if *expected_cols == 1 { "" } else { "s" },
                        table_name,
                        actual,
                    ),
                    "HO parameter arity mismatch",
                ));
            }
        }
    }
    Ok(())
}

// ============================================================================
// Namespace patching — DataNsPatcher fold
// ============================================================================

/// Patches unqualified table references (Ground, TVF, InnerRelation identifiers,
/// a scalarized relation's identifier) to use the data namespace. The default walk
/// functions recurse into all children, so filter conditions, operator
/// expressions, and nested domain expressions also get patched.
struct DataNsPatcher<'a> {
    data_ns: &'a ast_unresolved::NamespacePath,
    /// Authored query-local names that must not be namespace-qualified.
    /// Compiler carrier reads are structural and never enter this set.
    local_ctes: &'a std::collections::HashSet<String>,
}

impl AstTransform<Unresolved, Unresolved> for DataNsPatcher<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

    fn transform_relation(&mut self, r: Relation<Unresolved>) -> Result<Relation<Unresolved>> {
        match r {
            Relation::Ground {
                mention:
                    ast_unresolved::GroundMention::Named {
                        mut identifier,
                        alias,
                        mutation_target,
                        passthrough,
                    },
                outer,
                cpr_schema,
            } => {
                // Skip namespace patching for the query-local carrier CTEs
                // (an explicit set — see `local_ctes`).
                if identifier.namespace_path.is_empty()
                    && !self.local_ctes.contains(identifier.name.as_str())
                {
                    identifier.namespace_path = self.data_ns.clone();
                }
                Ok(Relation::Ground {
                    mention: ast_unresolved::GroundMention::Named {
                        identifier,
                        alias,
                        mutation_target,
                        passthrough,
                    },
                    outer,
                    cpr_schema,
                })
            }
            // InnerRelation: delegate to transform_inner_relation for identifier patching
            other => walk_transform_relation(self, other),
        }
    }

    fn transform_inner_relation(
        &mut self,
        i: InnerRelationPattern<Unresolved>,
    ) -> Result<InnerRelationPattern<Unresolved>> {
        match i {
            InnerRelationPattern::Indeterminate {
                mut identifier,
                subquery,
            } => {
                if identifier.namespace_path.is_empty() {
                    identifier.namespace_path = self.data_ns.clone();
                }
                Ok(InnerRelationPattern::Indeterminate {
                    identifier,
                    subquery: Box::new(self.transform_relational(*subquery)?),
                })
            }
            InnerRelationPattern::UncorrelatedDerivedTable {
                mut identifier,
                subquery,
                is_consulted_view,
            } => {
                if identifier.namespace_path.is_empty() {
                    identifier.namespace_path = self.data_ns.clone();
                }
                Ok(InnerRelationPattern::UncorrelatedDerivedTable {
                    identifier,
                    subquery: Box::new(self.transform_relational(*subquery)?),
                    is_consulted_view,
                })
            }
            other => walk_transform_inner_relation(self, other),
        }
    }

    // Stack-safe: one descent per nesting level, and the walk a
    // parenthesis ladder actually reaches.
    #[stacksafe::stacksafe]
    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Unresolved>> {
        match expr {
            // A NAMED SCALARIZED RELATION carries the spelling the patch
            // qualifies; the body is patched as the relation it is.
            DomainExpression::Application(FunctionApplication::Scalarized(
                crate::pipeline::asts::core::ScalarRelation::Named {
                    mut identifier,
                    body,
                },
            )) => {
                if identifier.namespace_path.is_empty() {
                    identifier.namespace_path = self.data_ns.clone();
                }
                let output = body.output;
                let patched = self.transform_relational(body.attached())?;
                Ok(DomainExpression::Application(
                    FunctionApplication::Scalarized(
                        crate::pipeline::asts::core::ScalarRelation::Named {
                            identifier,
                            body: Box::new(
                                crate::pipeline::asts::core::ScalarizedRelation::detach(
                                    patched, output,
                                )?,
                            ),
                        },
                    ),
                ))
            }
            other => walk_transform_domain(self, other),
        }
    }
}

/// Patch data namespace on all table references in a Query.
/// `local_ctes`: query-local carrier CTE names to leave unqualified.
pub(super) fn patch_data_ns_query(
    query: ast_unresolved::Query,
    data_ns: &ast_unresolved::NamespacePath,
    local_ctes: &std::collections::HashSet<String>,
) -> ast_unresolved::Query {
    DataNsPatcher {
        data_ns,
        local_ctes,
    }
    .transform_query(query)
    .expect("namespace patching is infallible")
}

/// Patch data_ns on table references within a relational expression.
fn patch_data_ns_in_relational_expr(
    expr: ast_unresolved::Chain,
    data_ns: &ast_unresolved::NamespacePath,
    local_ctes: &std::collections::HashSet<String>,
) -> ast_unresolved::Chain {
    DataNsPatcher {
        data_ns,
        local_ctes,
    }
    .transform_relational(expr)
    .expect("namespace patching is infallible")
}

/// The same patch over a CFE body, entered at whichever category it is.
pub(super) fn patch_data_ns_in_body(
    body: ast_unresolved::OutValue,
    data_ns: &ast_unresolved::NamespacePath,
) -> ast_unresolved::OutValue {
    let mut patcher = DataNsPatcher {
        data_ns,
        local_ctes: &std::collections::HashSet::new(),
    };
    match body {
        ast_unresolved::OutValue::Domain(value) => ast_unresolved::OutValue::Domain(
            patcher
                .transform_domain(value)
                .expect("namespace patching is infallible"),
        ),
        ast_unresolved::OutValue::Truth(crossing) => {
            ast_unresolved::OutValue::Truth(crate::pipeline::asts::core::TruthAsValue(
                patcher
                    .transform_boolean(crossing.into_truth())
                    .expect("namespace patching is infallible"),
            ))
        }
    }
}

#[cfg(test)]
mod clause_selection_tests {
    //! THE SYNTHESIZED SELECTION IS ITS OWN SHAPE.
    //!
    //! A multi-clause value rule assembles into `ClauseSelection`, whose arms
    //! carry what a CLAUSE computes — and a clause's result is its body,
    //! which is one of the crossing's licensed positions. The authored CASE
    //! carrier is a different thing and is pinned separately: its result is a
    //! `domain_expression` by the grammar, so a crossing has no derivation
    //! there.

    use super::build_case_body_from_clauses;
    use crate::ddl::reconstruct;
    use crate::pipeline::asts::core::{DomainExpression, OutValue};

    /// The selection a source's clauses assemble into.
    fn selection(source: &str) -> crate::pipeline::asts::core::ClauseSelection {
        let group = reconstruct::group(source).expect("the group reconstructs");
        let body =
            build_case_body_from_clauses("f", group.into_clauses()).expect("the clauses assemble");
        match body {
            DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::ClauseSelection(selection),
            ) => selection,
            other => panic!("expected a clause selection, got {other:?}"),
        }
    }

    /// Whether an arm's result crossed.
    fn crossed(arm: &crate::pipeline::asts::core::ClauseArm) -> bool {
        matches!(arm.result, OutValue::Truth(_))
    }

    /// BOTH CLAUSES CROSSED. The pre-carved existence spelling is a lawful
    /// value-rule body, so two of them are a lawful group.
    #[test]
    fn every_clause_may_compute_a_crossing() {
        let selection = selection(concat!(
            "served:(uid | uid > 5) :- +orders(, user_id = uid)\n",
            "served:(uid) :- +reviews(, user_id = uid)"
        ));
        assert_eq!(selection.arms.len(), 2);
        assert!(selection.arms.iter().all(crossed));
        // The guardless clause is the group's default, and there is one.
        assert_eq!(
            selection.arms.iter().filter(|a| a.guard.is_none()).count(),
            1
        );
    }

    /// MIXED IS ADMITTED. A clause computes a value either way, and the
    /// value-rule law does not tell a crossing from an ordinary value.
    #[test]
    fn clauses_may_mix_crossed_and_domain_results() {
        let selection = selection(concat!(
            "mixed:(uid | uid > 5) :- +orders(, user_id = uid)\n",
            "mixed:(uid) :- false"
        ));
        assert_eq!(selection.arms.len(), 2);
        assert!(crossed(&selection.arms[0]));
        assert!(!crossed(&selection.arms[1]));
    }

    /// The control: neither clause crossed, and the same shape carries them.
    #[test]
    fn a_domain_valued_group_uses_the_same_selection() {
        let selection = selection(concat!(
            "plain:(uid | uid > 5) :- \"high\"\n",
            "plain:(uid) :- \"low\""
        ));
        assert_eq!(selection.arms.len(), 2);
        assert!(!selection.arms.iter().any(crossed));
    }
}

#[cfg(test)]
mod ground_number_equality_tests {
    use super::normalize_number;

    fn eq(a: &str, b: &str) -> bool {
        normalize_number(a) == normalize_number(b) && normalize_number(a).is_some()
    }

    #[test]
    fn equal_values_across_spellings() {
        assert!(eq("12", "12.0"));
        assert!(eq("12", "1.2e1"));
        assert!(eq("0.5", "5e-1"));
        assert!(eq("0", "-0"));
        assert!(eq("0", "0.000"));
        assert!(eq("-3.25", "-32.5e-1"));
        assert!(eq("042", "42"));
    }

    #[test]
    fn adjacent_integers_beyond_f64_stay_distinct() {
        // 2^53 and 2^53 + 1: identical as f64, distinct as integers.
        assert!(!eq("9007199254740992", "9007199254740993"));
        assert!(!eq("-9007199254740992", "-9007199254740993"));
        // And the honest positive control at the same magnitude.
        assert!(eq("9007199254740993", "9007199254740993.0"));
    }

    #[test]
    fn sign_and_magnitude_matter() {
        assert!(!eq("1", "-1"));
        assert!(!eq("10", "1"));
        assert!(!eq("0.1", "0.01"));
    }

    #[test]
    fn non_numeric_spellings_do_not_normalize() {
        assert!(normalize_number("abc").is_none());
        assert!(normalize_number("").is_none());
        assert!(normalize_number("1.2.3").is_none());
        assert!(normalize_number("1e").is_none());
    }

    /// THE ROW IS THE VALUE, and only when there is one row of one column.
    ///
    /// `f(t(*) & 3)` is `f(t(*), _(3))`, so a scalar formal after `&` is
    /// supplied by a relation. A wider or taller relation is a relation: a
    /// scalar slot that took its first cell would be guessing which one the
    /// author meant, and the placeholder it binds instead refuses in the body
    /// under a spelling nobody wrote.
    #[test]
    fn only_a_single_cell_lift_answers_a_scalar_formal() {
        use super::{ast_unresolved, lifted_scalar};
        use crate::pipeline::asts::core::{AnonRelation, AnonTable, Chain, Grelex, LiteralValue};

        let literal = |n: &str| {
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(LiteralValue::Number(n.into())),
            )
        };
        let lifted = |rows: Vec<Vec<ast_unresolved::DomainExpression>>| {
            Chain::ground(Grelex::Literal(AnonRelation::plain(
                AnonTable::from_values(None, rows, ()).unwrap(),
            )))
        };

        let one_cell = lifted(vec![vec![literal("3")]]);
        assert_eq!(lifted_scalar(Some(&one_cell)), Some(literal("3")));

        let two_columns = lifted(vec![vec![literal("3"), literal("4")]]);
        let two_rows = lifted(vec![vec![literal("3")], vec![literal("4")]]);
        for wider in [two_columns, two_rows] {
            assert_eq!(
                lifted_scalar(Some(&wider)),
                None,
                "a relation with more than one cell is a relation"
            );
        }
        assert_eq!(lifted_scalar(None), None);
    }

    /// A RELATION THAT IS NOT A NAME HAS NO TERM.
    ///
    /// The formals that read a name or a value must be told there is none,
    /// not handed an invented lvar: a fabricated spelling either refuses in
    /// the body under a name nobody wrote, or — worse — captures a real
    /// column that happens to share it.
    #[test]
    fn a_relation_that_is_not_a_name_yields_no_term() {
        use super::{ast_unresolved, first_parens_terms, HoTerm};
        use crate::pipeline::asts::core::operators::{CallArguments, HoArgument};
        use crate::pipeline::asts::core::{
            AnonRelation, AnonTable, Chain, Grelex, LiteralValue, NamedReference, Reference,
        };

        let anonymous = HoArgument::Relation(Chain::ground(Grelex::Literal(AnonRelation::plain(
            AnonTable::from_values(
                None,
                vec![vec![ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(LiteralValue::Number("3".into())),
                )]],
                (),
            )
            .unwrap(),
        ))));
        let named = HoArgument::Relation(Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier: ast_unresolved::QualifiedName {
                        namespace_path: ast_unresolved::NamespacePath::empty(),
                        name: delightql_types::SqlIdentifier::new("users"),
                    },
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
                cpr_schema: (),
            },
            ast_unresolved::Access::All,
            (),
        ));

        let terms = first_parens_terms(&CallArguments::higher_order(vec![named, anonymous]))
            .expect("two arguments are terms");
        assert!(
            matches!(
                &terms[0],
                HoTerm::Term(ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(column)))) if column.name.as_str() == "users"
            ),
            "a named relation IS its name: {:?}",
            terms[0]
        );
        assert!(
            matches!(&terms[1], HoTerm::Opaque),
            "an anonymous relation names nothing a formal can bind: {:?}",
            terms[1]
        );
    }
}
