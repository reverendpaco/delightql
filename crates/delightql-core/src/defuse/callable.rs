// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The CALLABLE (scalar value) use road of the definition-use authority:
//! a consulted value definition is spent at its call site through ONE
//! instantiation entrance — selection, the position's grade, the formal
//! frame, the body's own world, and the body's resolution are all
//! interior. The cover and code-formal roads are the same machinery
//! reached from their own positions. No production caller can look a
//! definition up and instantiate it by hand, and NO CODE ACTUAL CROSSES A
//! DEFINITION BOUNDARY AS UNRESOLVED CALLER SYNTAX: the caller CLOSES what
//! it hands over — a consulted family by its exact qualified identity, a
//! target callable by its name, an open body or query-scoped definition as
//! a caller-resolved body with formal holes.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::resolved as ast_resolved;
use crate::pipeline::resolver::resolver_fold::ResolverFold;

/// THE CLOSED CODE ACTUAL a caller hands a curried formal. Every variant
/// is closed in the caller's world: nothing here is looked up again by
/// spelling inside the callee.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CallableBinding {
    /// A CONSULTED callable family, closed to its exact qualified
    /// identity in the caller. The invocation applies the family in the
    /// family's OWN world, with the site's arguments.
    Family {
        namespace: String,
        name: delightql_types::SqlIdentifier,
        /// The mention's own arguments, resolved in the CALLER: the
        /// defaults an argumentless invocation applies.
        defaults: Vec<ast_resolved::DomainExpression>,
    },
    /// A TARGET/BUILTIN callable, CLOSED in the caller: the callee the
    /// caller judged a target (the provider is deliberately late, so the
    /// name needs no lexical lookup — but the judgment that it IS a target
    /// is made once, here), with its own caller-resolved arguments, guard,
    /// and window. The invocation applies it to the site's arguments
    /// through the one window-signature authority; no unresolved syntax
    /// crosses and no definition of the callee's world can read the
    /// spelling.
    Target(TargetCallable),
    /// A CLOSED BODY: a lambda, template, or query-scoped definition,
    /// resolved in the caller's world with one FORMAL HOLE per input. The
    /// invocation substitutes the site's resolved arguments by position.
    Closed(ClosedCallable),
}

/// One target callable's closed lexical meaning: the callee as the caller
/// wrote and judged it, its marks, and the mention's own arguments, guard,
/// and window — every part RESOLVED in the caller's world at handover.
#[derive(Clone)]
pub(crate) struct TargetCallable {
    callee: crate::pipeline::asts::vocabulary::Ref,
    marks: crate::pipeline::asts::vocabulary::FunctorMarks,
    defaults: ast_resolved::CallArguments,
    guard: Option<Box<ast_resolved::TruthExpression>>,
    window: Option<ast_resolved::WindowSpec>,
}

impl TargetCallable {
    fn name(&self) -> String {
        self.callee.name_text()
    }

    fn namespace_fq(&self) -> Option<String> {
        self.callee.namespace_fq()
    }
}

impl std::fmt::Debug for TargetCallable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TargetCallable")
            .field("callee", &self.name())
            .field("namespace", &self.namespace_fq())
            .field("defaults", &self.defaults)
            .field("guard", &self.guard)
            .field("window", &self.window)
            .finish()
    }
}

impl PartialEq for TargetCallable {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
            && self.namespace_fq() == other.namespace_fq()
            && self.marks == other.marks
            && self.defaults == other.defaults
            && self.guard == other.guard
            && self.window == other.window
    }
}

/// One caller-closed open body: `arity` ordered holes and the resolved
/// body that carries them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClosedCallable {
    arity: usize,
    body: ast_resolved::DomainExpression,
    /// The mention's own arguments where it wrote any, resolved in the
    /// caller — applied when the invocation supplies none.
    defaults: Vec<ast_resolved::DomainExpression>,
}

impl CallableBinding {
    /// The semantic-key serialization of this actual.
    pub(in crate::defuse) fn semantic_key(&self) -> String {
        use crate::lispy::ToLispy;
        match self {
            CallableBinding::Family {
                namespace, name, ..
            } => format!("code:{namespace}::{name}"),
            CallableBinding::Target(target) => format!("code:target:{}", target.name()),
            CallableBinding::Closed(closed) => format!("code:closed:{}", closed.body.to_lispy()),
        }
    }
}

/// Substitute a closed body's formal holes with the invocation's resolved
/// arguments, by position — a resolved-to-resolved rewrite; nothing is
/// looked up.
fn substitute_holes(
    body: ast_resolved::DomainExpression,
    args: &[ast_resolved::DomainExpression],
) -> Result<ast_resolved::DomainExpression> {
    use crate::pipeline::ast_transform::walk_transform_domain;
    use crate::pipeline::asts::core::Resolved;
    struct Substitute<'a> {
        args: &'a [ast_resolved::DomainExpression],
    }
    impl AstTransform<Resolved, Resolved> for Substitute<'_> {
        crate::pipeline::ast_transform::same_phase_payload_folds!(Resolved);
        #[stacksafe::stacksafe]
        fn transform_domain(
            &mut self,
            expr: ast_resolved::DomainExpression,
        ) -> Result<ast_resolved::DomainExpression> {
            if let ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Open(hole),
            ) = &expr
            {
                let Some(value) = self.args.get(hole.0 as usize) else {
                    return Err(DelightQLError::validation_error_categorized(
                        "cfe/lambda_arity",
                        "a closed callable's slot has no argument at its position",
                        "supply one value per slot",
                    ));
                };
                return Ok(value.clone());
            }
            walk_transform_domain(self, expr)
        }
    }
    Substitute { args }.transform_domain(body)
}

/// The resolved expression standing for one formal hole.
fn hole(index: u32) -> ast_resolved::DomainExpression {
    ast_resolved::DomainExpression::Application(ast_resolved::FunctionApplication::Open(
        crate::pipeline::asts::core::FormalHole(index),
    ))
}

/// How many leading argument positions of this callee take CODE — the
/// curried formals of a consulted higher-order function. Zero for
/// everything else, including names the catalog does not know.
pub(crate) fn curried_code_positions(
    callee: &crate::pipeline::asts::vocabulary::Ref,
    core: &crate::resolution::ResolverCore,
    env: &super::environment::Environment,
) -> usize {
    let name = callee.name_identifier();
    let namespace = callee.namespace_fq();
    let Ok(Some(family)) =
        select_callable_family(&core.consult, &name, namespace.as_deref(), env.reach())
    else {
        return 0;
    };
    // A catalog FACT: the code-parameter roles are stored at consult, so
    // this probe never opens a body.
    family
        .params()
        .iter()
        .filter(|param| {
            matches!(
                param,
                crate::pipeline::asts::ddl::HoParam::Scalar { callable: true, .. }
            )
        })
        .count()
}

/// THE INSTANTIATION ROAD: a consulted value definition is spent at its
/// call site through the MANDATORY transition — the caller's actuals
/// resolve FIRST (in the caller's world), the family instance is
/// ADMITTED under their semantic key, and only the admitted instance
/// opens and shapes the body, in the body's OWN world. A same-key
/// self-reference is the immediate `cfe/recursion` terminal (a value
/// definition has no fixpoint to re-enter); a changed-key self-reference
/// is the ruled `semantic/recursion/parameter-widening` terminal.
pub(crate) fn inline_cfe_call(
    fold: &mut ResolverFold<'_, '_>,
    application: &ast_unresolved::StandardApplication,
) -> Result<Option<ast_resolved::DomainExpression>> {
    use crate::pipeline::asts::core::operators::ScalarArgument;
    let reference = &application.call().callee;
    let name = reference.name_text();
    let namespace = reference.namespace_fq();
    // A CODE FORMAL, INVOKED: the innermost open instantiation bound code
    // to this bare name, and the invocation spends that binding — before
    // any catalog is asked, because the formal is not a catalog name.
    if namespace.is_none() {
        let key = reference.name_identifier();
        let binding = fold.env.formal_callable(&key);
        if let Some(binding) = binding {
            return instantiate_callable_site(fold, application, binding, &name).map(Some);
        }
    }
    if application.guard.is_some() {
        // A guard filters the rows this application sees; it does not
        // belong to a scalar definition's instantiation.
        return Ok(None);
    }
    // THE USE-POSITION GRADE ENTERS THE BOUND USE: a windowed call is a
    // Windowed use; otherwise the position's own grade stands (Reducing
    // inside a reduction slot, RowWise elsewhere), and the body resolves
    // under it so nested callables inherit the expectation.
    let grade = if application.window.is_some() {
        super::bound_use::CallableGrade::Windowed
    } else {
        fold.position_grade
    };
    // SELECT, without opening: the query-scoped binding first, then the
    // catalog family. The code-parameter COUNT is a catalog fact, so the
    // member partition below never needs the body.
    let Some(selection) = select_callable(fold, reference, namespace.as_deref())? else {
        return Ok(None);
    };
    let code_count = match &selection {
        CallableSelection::Scoped(cfe) => cfe.callable_formals().len(),
        CallableSelection::Family(family) => family
            .params()
            .iter()
            .filter(|param| {
                matches!(
                    param,
                    crate::pipeline::asts::ddl::HoParam::Scalar { callable: true, .. }
                )
            })
            .count(),
    };

    let all_members = application.call().arguments.scalar_members();
    let is_marker = |member: &ScalarArgument<crate::pipeline::asts::core::Unresolved>| {
        matches!(member, ScalarArgument::Context(_))
    };
    let context_call = all_members.first().is_some_and(is_marker);
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
    if members.len() < code_count {
        return Err(DelightQLError::validation_error_categorized(
            "cfe/code_argument",
            format!(
                "'{name}' declares {code_count} code parameter{} and the call supplies \
                 only {} member{}",
                if code_count == 1 { "" } else { "s" },
                members.len(),
                if members.len() == 1 { "" } else { "s" },
            ),
            "supply one argument per declared parameter, code first",
        ));
    }

    // THE CALLER'S ACTUALS RESOLVE FIRST. Code members close into
    // caller-owned closed actuals; value members resolve in the caller's
    // world. A member shape the instantiation does not take falls through
    // to the ordinary roads before anything resolves.
    let mut unresolved_values: Vec<ast_unresolved::DomainExpression> = Vec::new();
    for member in members[code_count..].iter() {
        match member {
            ScalarArgument::Value(value) => unresolved_values.push(value.value.clone()),
            ScalarArgument::Callable(_)
            | ScalarArgument::Spread(_)
            | ScalarArgument::Star
            | ScalarArgument::Context(_) => return Ok(None),
        }
    }
    let mut code_bindings: Vec<CallableBinding> = Vec::new();
    for (index, member) in members[..code_count].iter().enumerate() {
        // The position declares CODE, so what stands in it is code however
        // spelled: `upper:()` arrives as an ordinary (nullary) application
        // and the position reads it as the mention it is.
        let binding = match member {
            ScalarArgument::Callable(callable) => callable_binding(fold, callable)?,
            ScalarArgument::Value(value)
                if matches!(
                    &value.value,
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Standard(_)
                    )
                ) =>
            {
                let ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Standard(mention),
                ) = &value.value
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
                    format!("'{name}' takes code in its position {}", index + 1),
                    "write a mention `fn:()`, a lambda `:(…)`, or a template `:\"…\"`",
                ));
            }
        };
        code_bindings.push(binding);
    }
    let resolved_values: Vec<ast_resolved::DomainExpression> = unresolved_values
        .into_iter()
        .map(|value| fold.transform_domain(value))
        .collect::<Result<Vec<_>>>()?;

    // BIND THE USE: the carrier owns family + declaration world + typed
    // actuals + semantic key + held admission as ONE construction, and it
    // is SPENT WHOLE by the consuming operation — this road never holds a
    // family, a world, an actuals value, or a raw body. A same-key
    // self-reference has no fixpoint to read — the immediate terminal; a
    // changed key is the ruled widening terminal, observed the moment it
    // is requested.
    let actuals = super::bound_use::ScalarActuals::of(context_call, code_bindings, resolved_values);
    let use_value = match selection {
        CallableSelection::Scoped(cfe) => {
            let scoped_name = reference.name_identifier();
            match super::bound_use::bind_scoped_use(
                &fold.config.instances,
                &scoped_name,
                cfe,
                actuals,
            ) {
                super::bound_use::ScopedBoundAdmission::Fresh(scoped) => {
                    super::bound_use::CallableUse::Scoped(scoped)
                }
                super::bound_use::ScopedBoundAdmission::Reenter => {
                    return Err(scalar_recursion_refusal(&name));
                }
                super::bound_use::ScopedBoundAdmission::Cycle { chain } => {
                    return Err(scalar_cycle_refusal(&name, chain));
                }
                super::bound_use::ScopedBoundAdmission::Widening {
                    building,
                    requested,
                } => {
                    return Err(scalar_widening_refusal(&name, &building, &requested));
                }
            }
        }
        CallableSelection::Family(family) => {
            match super::bound_use::bind_definition_use(&fold.config.instances, family, actuals)? {
                super::bound_use::BoundAdmission::Fresh(bound) => {
                    super::bound_use::CallableUse::Bound(bound)
                }
                super::bound_use::BoundAdmission::Reenter => {
                    return Err(scalar_recursion_refusal(&name));
                }
                super::bound_use::BoundAdmission::Cycle { chain } => {
                    return Err(scalar_cycle_refusal(&name, chain));
                }
                super::bound_use::BoundAdmission::Widening {
                    building,
                    requested,
                } => {
                    return Err(scalar_widening_refusal(&name, &building, &requested));
                }
            }
        }
    };
    // A WINDOWED USE OF A CONSULTED DEFINITION: the wrapper is
    // grade-polymorphic per use. The spec resolves HERE, in the caller's
    // world, and the obligation arms for exactly the body's resolution —
    // the first reducing absorber the body builds (an engine aggregate or
    // an unknown target callable) takes it, which is the position's grade
    // flowing INWARD through the wrapper.
    let armed_window = match (grade, application.window.clone()) {
        (super::bound_use::CallableGrade::Windowed, Some(window)) => {
            let spec = fold.resolve_window_spec(window)?;
            Some(std::mem::replace(
                &mut fold.window_obligation,
                Some(crate::pipeline::resolver::resolver_fold::WindowObligation {
                    spec,
                    taken: false,
                    extra: false,
                }),
            ))
        }
        _ => None,
    };
    let outcome = use_value.apply_call(fold, &name, members.len());
    if let Some(prior_obligation) = armed_window {
        let obligation = fold
            .window_obligation
            .take()
            .expect("the armed obligation outlives the body resolution");
        fold.window_obligation = prior_obligation;
        if outcome.is_ok() {
            if !obligation.taken {
                return Err(DelightQLError::validation_error_categorized(
                    "window/not_a_window",
                    format!(
                        "the window rides the window function itself, and the body \
                         of '{name}' opens no function that can carry it — a \
                         consulted value definition computes per row unless its \
                         body reaches an aggregate or a target callable"
                    ),
                    "spell the windowed call inside the argument, or window the \
                     body's own aggregate",
                ));
            }
            if obligation.extra {
                return Err(DelightQLError::validation_error_categorized(
                    "window/not_a_window",
                    format!(
                        "the window rides ONE function, and the body of '{name}' \
                         opens more than one that could carry it"
                    ),
                    "attach the window inside the definition, on the function it \
                     rides",
                ));
            }
        }
    }
    outcome
}

/// A COVER'S CALLABLE THAT NAMES A DEFINITION, instantiated to the open
/// lambda it denotes: the mention's own arguments bind a complete left
/// prefix and the covered cell takes the FINAL formal — it stays a slot. What the cover
/// machinery then spends per cell is ordinary resolved code; no carrier
/// survives for a later phase to expand.
pub(crate) fn cover_functor_apply_cell(
    fold: &mut ResolverFold<'_, '_>,
    application: &ast_unresolved::StandardApplication,
    cell: ast_resolved::DomainExpression,
) -> Result<Option<ast_resolved::DomainExpression>> {
    let name = application.call().callee.name_text();
    let namespace = application.call().callee.namespace_fq();
    let Some(selection) = select_callable(fold, &application.call().callee, namespace.as_deref())?
    else {
        return Ok(None);
    };
    if let CallableSelection::Family(family) = &selection {
        let has_code = family.params().iter().any(|param| {
            matches!(
                param,
                crate::pipeline::asts::ddl::HoParam::Scalar { callable: true, .. }
            )
        });
        if has_code {
            // A higher-order definition has no one-cell reading; the
            // ordinary callable road keeps whatever meaning it had.
            return Ok(None);
        }
    }
    // THE CELL AND THE PARTIALS ARE THE CALLER'S ACTUALS, resolved before
    // admission; the cell arrives already resolved from the cover.
    let partial_values: Vec<ast_resolved::DomainExpression> = application
        .call()
        .arguments
        .value_domains()
        .cloned()
        .map(|value| fold.transform_domain(value))
        .collect::<Result<Vec<_>>>()?;

    // THE CELL TAKES THE DEFAULT LANDING, the row's final place: the
    // written partials bind a complete left prefix exactly as they do for a
    // function pipe, so `$(f:(a))(x)` and `x /-> f:(a)` are one application.
    let actuals = super::bound_use::ValueActuals::of(crate::pipeline::normalize::land_final(
        cell,
        partial_values,
    ));
    let use_value = match selection {
        CallableSelection::Scoped(cfe) => {
            let scoped_name = application.call().callee.name_identifier();
            match super::bound_use::bind_scoped_use(
                &fold.config.instances,
                &scoped_name,
                cfe,
                actuals,
            ) {
                super::bound_use::ScopedBoundAdmission::Fresh(scoped) => {
                    super::bound_use::CallableUse::Scoped(scoped)
                }
                super::bound_use::ScopedBoundAdmission::Reenter => {
                    return Err(scalar_recursion_refusal(&name));
                }
                super::bound_use::ScopedBoundAdmission::Cycle { chain } => {
                    return Err(scalar_cycle_refusal(&name, chain));
                }
                super::bound_use::ScopedBoundAdmission::Widening {
                    building,
                    requested,
                } => {
                    return Err(scalar_widening_refusal(&name, &building, &requested));
                }
            }
        }
        CallableSelection::Family(family) => {
            match super::bound_use::bind_definition_use(&fold.config.instances, family, actuals)? {
                super::bound_use::BoundAdmission::Fresh(bound) => {
                    super::bound_use::CallableUse::Bound(bound)
                }
                super::bound_use::BoundAdmission::Reenter => {
                    return Err(scalar_recursion_refusal(&name));
                }
                super::bound_use::BoundAdmission::Cycle { chain } => {
                    return Err(scalar_cycle_refusal(&name, chain));
                }
                super::bound_use::BoundAdmission::Widening {
                    building,
                    requested,
                } => {
                    return Err(scalar_widening_refusal(&name, &building, &requested));
                }
            }
        }
    };
    use_value.apply_cover(fold, &name)
}

/// The code a caller supplies for a curried formal, CLOSED in the caller's
/// world:
///
/// - a mention of an OUTER code formal hands the outer closed actual on;
/// - a mention of a query-scoped definition closes into its caller-resolved
///   body with formal holes — the definition's meaning is fixed HERE, so no
///   same-spelled definition in the callee's world can capture it;
/// - a mention of a consulted callable closes to its exact qualified
///   identity;
/// - any other mention is a target/builtin callable: the name needs no
///   lexical lookup, and the mention is judged here (a window function
///   refuses standing bare);
/// - a lambda or template body resolves HERE, in the caller's world, with
///   its slot as a formal hole.
fn callable_binding(
    fold: &mut ResolverFold<'_, '_>,
    callable: &crate::pipeline::asts::core::Callable,
) -> Result<CallableBinding> {
    use crate::pipeline::asts::core::Callable;
    match callable {
        Callable::Functor(application) => {
            // A mention of an OUTER code formal hands the outer binding on.
            if application.call().callee.namespace_fq().is_none() {
                let key = application.call().callee.name_identifier();
                if let Some(outer) = fold.env.formal_callable(&key) {
                    return Ok(outer);
                }
                // A QUERY-SCOPED definition mentioned as code closes NOW:
                // its body resolves in this world with holes for its
                // formals.
                if let Some(super::environment::QueryLocalSelection::Value(cfe)) =
                    fold.env.select_query_local(
                        &key,
                        crate::pipeline::asts::core::QueryLocalDemand::Value,
                        None,
                    )?
                {
                    return close_scoped(fold, cfe, application);
                }
            }
            // A CONSULTED callable closes to its exact qualified identity.
            let namespace = application.call().callee.namespace_fq();
            let callee_ident = application.call().callee.name_identifier();
            if let Some(family) = select_callable_family(
                &fold.core.consult,
                &callee_ident,
                namespace.as_deref(),
                fold.env.reach(),
            )? {
                let defaults: Vec<ast_resolved::DomainExpression> = application
                    .call()
                    .arguments
                    .value_domains()
                    .cloned()
                    .map(|value| fold.transform_domain(value))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(CallableBinding::Family {
                    namespace: family.namespace().to_string(),
                    name: family.name().clone(),
                    defaults,
                });
            }
            // Any other mention is a TARGET callable, and it CLOSES here,
            // where it is handed over: the target-provider law judges the
            // callee in the caller's world (a qualified miss and a taken
            // name refuse), and the mention's own arguments, guard, and
            // window resolve in the caller's world. What crosses is
            // finished: the callee's world never reads the spelling.
            close_target(fold, application)
        }
        Callable::Lambda(lambda) => close_open_body(fold, (*lambda.body).clone()),
        Callable::String(template) => close_open_body(
            fold,
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Template(template.clone()),
            ),
        ),
    }
}

/// A target mention CLOSED in the caller: judged a target once, its own
/// arguments (as a row-wise call with no code position — no definition of
/// any world decides one), guard, and window resolved where they were
/// written.
fn close_target(
    fold: &mut ResolverFold<'_, '_>,
    application: &ast_unresolved::StandardApplication,
) -> Result<CallableBinding> {
    let callee = application.call().callee.clone();
    fold.judge_target_callee(&callee)?;
    let marks = application.call().marks.clone();
    let defaults = fold
        .resolve_target_call(ast_unresolved::FunctorCall {
            callee: callee.clone(),
            arguments: application.call().arguments.clone(),
            marks: marks.clone(),
        })?
        .arguments;
    let guard = application
        .guard
        .clone()
        .map(|condition| fold.transform_boolean(*condition).map(Box::new))
        .transpose()?;
    let window = application
        .window
        .clone()
        .map(|window| fold.resolve_window_spec(window))
        .transpose()?;
    Ok(CallableBinding::Target(TargetCallable {
        callee,
        marks,
        defaults,
        guard,
        window,
    }))
}

/// An open body CLOSED as a caller-resolved body with one formal hole:
/// the interior is the caller's text, so it resolves HERE — a bad
/// reference refuses at the handover even if the formal is never invoked
/// — and what crosses carries no spelling for any other world to answer.
fn close_open_body(
    fold: &mut ResolverFold<'_, '_>,
    body: ast_unresolved::DomainExpression,
) -> Result<CallableBinding> {
    let prior = fold.cover_cell.replace(hole(0));
    let resolved = fold.transform_domain(body);
    fold.cover_cell = prior;
    Ok(CallableBinding::Closed(ClosedCallable {
        arity: 1,
        body: resolved?,
        defaults: Vec::new(),
    }))
}

/// A query-scoped definition CLOSED as a code actual: its body resolves in
/// the caller's world — sealed, its formals standing as holes — so the
/// callee receives finished code, never a spelling to look up.
fn close_scoped(
    fold: &mut ResolverFold<'_, '_>,
    cfe: crate::pipeline::asts::core::CfeDefinition,
    application: &ast_unresolved::StandardApplication,
) -> Result<CallableBinding> {
    use crate::pipeline::asts::core::ContextMode;
    let name = cfe.name.clone();
    if cfe.context_mode != ContextMode::None || !cfe.callable_formals().is_empty() {
        return Err(DelightQLError::validation_error_categorized(
            "cfe/code_argument",
            format!(
                "'{name}' is context-aware or curried and cannot cross as a code \
                 actual"
            ),
            "pass a lambda `:(…)` closing over what it needs, or a consulted function",
        ));
    }
    let defaults: Vec<ast_resolved::DomainExpression> = application
        .call()
        .arguments
        .value_domains()
        .cloned()
        .map(|value| fold.transform_domain(value))
        .collect::<Result<Vec<_>>>()?;
    let (arity, scoped) = crate::defuse::admitted::scoped_curried(&cfe)?;
    fold.env.push_horizon(cfe.horizon());
    let resolved = scoped.resolve(fold);
    fold.env.pop_horizon();
    let resolved = resolved?;
    Ok(CallableBinding::Closed(ClosedCallable {
        arity,
        body: resolved,
        defaults,
    }))
}

/// An invocation of a curried FORMAL, spent against the CLOSED actual the
/// caller bound. The site's own arguments resolve HERE — they are this
/// world's values — and the actual is applied without any lookup of the
/// caller's spelling.
fn instantiate_callable_site(
    fold: &mut ResolverFold<'_, '_>,
    application: &ast_unresolved::StandardApplication,
    binding: CallableBinding,
    name: &str,
) -> Result<ast_resolved::DomainExpression> {
    match binding {
        CallableBinding::Family {
            namespace,
            name: family_name,
            defaults,
        } => {
            if application.guard.is_some() {
                return Err(DelightQLError::validation_error_categorized(
                    "cfe/code_argument",
                    format!(
                        "a guard on the invocation of code formal '{name}' has no \
                         reading"
                    ),
                    "filter the rows before the call",
                ));
            }
            // The site's arguments resolve in THIS world; an argumentless
            // invocation applies the mention's own caller-resolved
            // defaults.
            let site_args: Vec<ast_resolved::DomainExpression> = application
                .call()
                .arguments
                .value_domains()
                .cloned()
                .map(|value| fold.transform_domain(value))
                .collect::<Result<Vec<_>>>()?;
            let args = if site_args.is_empty() {
                defaults
            } else {
                site_args
            };
            let supplied = args.len();
            // The family applies in ITS OWN world, by exact qualified
            // identity — the same one road every scalar call takes.
            let Some(family) = select_callable_family(
                &fold.core.consult,
                &family_name,
                Some(namespace.as_str()),
                fold.env.reach(),
            )?
            else {
                return Err(DelightQLError::validation_error_categorized(
                    "cfe/code_argument",
                    format!(
                        "the code actual '{namespace}::{family_name}' no longer \
                         selects a callable"
                    ),
                    "code actuals apply the definition they closed over",
                ));
            };
            let actuals = super::bound_use::ScalarActuals::of(false, Vec::new(), args);
            let use_value = match super::bound_use::bind_definition_use(
                &fold.config.instances,
                family,
                actuals,
            )? {
                super::bound_use::BoundAdmission::Fresh(bound) => {
                    super::bound_use::CallableUse::Bound(bound)
                }
                super::bound_use::BoundAdmission::Reenter => {
                    return Err(scalar_recursion_refusal(name));
                }
                super::bound_use::BoundAdmission::Cycle { chain } => {
                    return Err(scalar_cycle_refusal(name, chain));
                }
                super::bound_use::BoundAdmission::Widening {
                    building,
                    requested,
                } => {
                    return Err(scalar_widening_refusal(name, &building, &requested));
                }
            };
            // The armed-window road belongs to the direct invocation; a
            // window on a formal invocation rides the applied family.
            let armed_window = match application.window.clone() {
                Some(window) => {
                    let spec = fold.resolve_window_spec(window)?;
                    Some(std::mem::replace(
                        &mut fold.window_obligation,
                        Some(crate::pipeline::resolver::resolver_fold::WindowObligation {
                            spec,
                            taken: false,
                            extra: false,
                        }),
                    ))
                }
                None => None,
            };
            let outcome = use_value.apply_call(fold, name, supplied);
            if let Some(prior) = armed_window {
                let obligation = fold
                    .window_obligation
                    .take()
                    .expect("the armed obligation outlives the body resolution");
                fold.window_obligation = prior;
                if outcome.is_ok() && !obligation.taken {
                    return Err(DelightQLError::validation_error_categorized(
                        "window/not_a_window",
                        format!(
                            "the window rides the window function itself, and the \
                             body of '{name}' opens no function that can carry it"
                        ),
                        "window the body's own aggregate",
                    ));
                }
            }
            outcome?.ok_or_else(|| {
                DelightQLError::validation_error_categorized(
                    "cfe/code_argument",
                    format!("the code actual behind '{name}' did not instantiate"),
                    "code actuals apply the definition they closed over",
                )
            })
        }
        CallableBinding::Target(target) => {
            // The invocation's own arguments replace the mention's, and its
            // guard wins; the window rides whichever side wrote one. The
            // site's arguments resolve HERE — they are this world's values,
            // with the formals they can lawfully reach — as a row-wise call
            // with no code position; the closed callee is never looked up
            // again, and the ONE window-signature authority judges the
            // finished application exactly as it judges an authored one.
            let TargetCallable {
                callee,
                marks,
                defaults,
                guard,
                window,
            } = target;
            let site_arguments = application.call().arguments.clone();
            let arguments = if site_arguments.scalar_members().is_empty() {
                defaults
            } else {
                fold.resolve_target_call(ast_unresolved::FunctorCall {
                    callee: callee.clone(),
                    arguments: site_arguments,
                    marks: marks.clone(),
                })?
                .arguments
            };
            let call = ast_resolved::FunctorCall {
                callee: callee.written_call_identity(&fold.core.identities),
                arguments,
                marks,
            };
            let guard = match application.guard.clone() {
                Some(condition) => Some(Box::new(fold.transform_boolean(*condition)?)),
                None => guard,
            };
            let windowed = application.window.is_some() || window.is_some();
            let window = match application.window.clone() {
                Some(spec) => Some(fold.resolve_window_spec(spec)?),
                None => window,
            };
            let resolved = fold.finish_application(
                &callee.name_text(),
                callee.namespace_fq().as_deref(),
                windowed,
                call,
                window,
                guard,
            )?;
            Ok(ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Standard(resolved),
            ))
        }
        CallableBinding::Closed(closed) => {
            if application.window.is_some() {
                return Err(DelightQLError::validation_error_categorized(
                    "window/not_a_window",
                    format!("a window cannot ride the closed-body formal '{name}'"),
                    "window the function inside the body instead",
                ));
            }
            let site_args: Vec<ast_resolved::DomainExpression> = application
                .call()
                .arguments
                .value_domains()
                .cloned()
                .map(|value| fold.transform_domain(value))
                .collect::<Result<Vec<_>>>()?;
            let args = if site_args.is_empty() && !closed.defaults.is_empty() {
                closed.defaults.clone()
            } else {
                site_args
            };
            if args.len() != closed.arity {
                return Err(DelightQLError::validation_error_categorized(
                    "cfe/lambda_arity",
                    format!(
                        "'{name}' is bound to a closed body with {} slot{}; the \
                         invocation supplies {}",
                        closed.arity,
                        if closed.arity == 1 { "" } else { "s" },
                        args.len()
                    ),
                    "supply one value per slot",
                ));
            }
            substitute_holes(closed.body, &args)
        }
    }
}

// ============================================================================
// Ground scalar expansion for HO views

/// A slot expression's invocation of a query-scoped value definition.
///
/// The definition is spent with the same laws as anywhere: arguments
/// resolve where the caller stands (this relation's heading, plus any
/// open frame), the body resolves SEALED — its formals and nothing
/// else, in its own world. A crossing body resolves into the licensed
/// ClauseSelection carrier.
pub(crate) fn instantiate_slot(
    converter: &mut crate::pipeline::resolver::StrictPhaseConverter<'_, '_>,
    application: &ast_unresolved::StandardApplication,
) -> Result<Option<ast_resolved::DomainExpression>> {
    use crate::pipeline::asts::core::operators::ScalarArgument;
    let instantiation = converter.instantiation();
    let callee = &application.call().callee;
    let name = callee.name_text();
    // A query-scoped name is bare; a consulted one may be qualified.
    let scoped = if callee.namespace_fq().is_none() {
        let key = callee.name_identifier();
        match instantiation
            .select_query_local(&key, crate::pipeline::asts::core::QueryLocalDemand::Value)?
        {
            Some(super::environment::QueryLocalSelection::Value(cfe)) => Some(cfe),
            Some(_) => unreachable!("value demand returns only a value manifestation"),
            None => None,
        }
    } else {
        None
    };
    let selection = match scoped {
        Some(cfe) => CallableSelection::Scoped(cfe),
        None => {
            let callee_ident = callee.name_identifier();
            let Some(family) = select_callable_family(
                &instantiation.core().consult,
                &callee_ident,
                callee.namespace_fq().as_deref(),
                instantiation.env().reach(),
            )?
            else {
                return Ok(None);
            };
            CallableSelection::Family(family)
        }
    };
    if let CallableSelection::Family(family) = &selection {
        let has_code = family.params().iter().any(|param| {
            matches!(
                param,
                crate::pipeline::asts::ddl::HoParam::Scalar { callable: true, .. }
            )
        });
        if has_code {
            // A curried definition has no slot reading; the ordinary
            // refusal for an unknown function stands.
            return Ok(None);
        }
    }
    // THE CALLER'S ACTUALS RESOLVE FIRST, in this relation's own scope.
    let mut values = Vec::new();
    for member in application.call().arguments.scalar_members() {
        match member {
            ScalarArgument::Value(value) => {
                values.push(converter.transform_domain(value.value.clone())?)
            }
            _ => return Ok(None),
        }
    }
    // BIND THE USE — the slot road crosses the same complete transition
    // as every other callable position, and spends it whole.
    let actuals = super::bound_use::ValueActuals::of(values);
    let use_value = match selection {
        CallableSelection::Scoped(cfe) => {
            let scoped_name = callee.name_identifier();
            match super::bound_use::bind_scoped_use(
                instantiation.instances(),
                &scoped_name,
                cfe,
                actuals,
            ) {
                super::bound_use::ScopedBoundAdmission::Fresh(scoped) => {
                    super::bound_use::CallableUse::Scoped(scoped)
                }
                super::bound_use::ScopedBoundAdmission::Reenter => {
                    return Err(scalar_recursion_refusal(&name));
                }
                super::bound_use::ScopedBoundAdmission::Cycle { chain } => {
                    return Err(scalar_cycle_refusal(&name, chain));
                }
                super::bound_use::ScopedBoundAdmission::Widening {
                    building,
                    requested,
                } => {
                    return Err(scalar_widening_refusal(&name, &building, &requested));
                }
            }
        }
        CallableSelection::Family(family) => {
            match super::bound_use::bind_definition_use(instantiation.instances(), family, actuals)?
            {
                super::bound_use::BoundAdmission::Fresh(bound) => {
                    super::bound_use::CallableUse::Bound(bound)
                }
                super::bound_use::BoundAdmission::Reenter => {
                    return Err(scalar_recursion_refusal(&name));
                }
                super::bound_use::BoundAdmission::Cycle { chain } => {
                    return Err(scalar_cycle_refusal(&name, chain));
                }
                super::bound_use::BoundAdmission::Widening {
                    building,
                    requested,
                } => {
                    return Err(scalar_widening_refusal(&name, &building, &requested));
                }
            }
        }
    };
    use_value.apply_slot(converter, instantiation, &name)
}

/// What a callable name selected: the query-scoped binding (already
/// shaped at registration — its admission is by binding name), or a
/// catalog family (admitted and opened by identity).
pub(in crate::defuse) enum CallableSelection<'s> {
    Scoped(crate::pipeline::asts::unresolved::CfeDefinition),
    Family(super::select::LinkedFamily<'s>),
}

/// SELECT a callable, without opening: query-scoped first, then the
/// catalog (function and context-aware capability in ONE judgment over
/// the complete candidate set — never probe order). `None` is a position
/// miss; the caller's fallback ladder stands.
pub(in crate::defuse) fn select_callable<'db>(
    fold: &mut ResolverFold<'_, 'db>,
    callee: &crate::pipeline::asts::vocabulary::Ref,
    namespace: Option<&str>,
) -> Result<Option<CallableSelection<'db>>> {
    if namespace.is_none() {
        let key = callee.name_identifier();
        match fold.env.select_query_local(
            &key,
            crate::pipeline::asts::core::QueryLocalDemand::Value,
            None,
        )? {
            Some(super::environment::QueryLocalSelection::Value(cfe)) => {
                return Ok(Some(CallableSelection::Scoped(cfe)));
            }
            Some(_) => unreachable!("value demand returns only a value manifestation"),
            None => {}
        }
    }
    let callee_ident = callee.name_identifier();
    let Some(family) = select_callable_family(
        &fold.core.consult,
        &callee_ident,
        namespace,
        fold.env.reach(),
    )?
    else {
        return Ok(None);
    };
    Ok(Some(CallableSelection::Family(family)))
}

/// The catalog half of callable selection, over one world's reach.
pub(in crate::defuse) fn select_callable_family<'s>(
    consult: &crate::resolution::registry::ConsultRegistry<'s>,
    callee: &delightql_types::SqlIdentifier,
    namespace: Option<&str>,
    reach: &super::environment::DeclarationReach,
) -> Result<Option<super::select::LinkedFamily<'s>>> {
    let capable = |k: crate::enums::EntityType| {
        k == crate::enums::EntityType::DqlFunctionExpression
            || k == crate::enums::EntityType::DqlContextAwareFunctionExpression
    };
    if let Some(ns) = namespace {
        // Blueprint inertness, loud door for the function route
        // (companion_linear--74): a qualified call into an archived
        // blueprint gets the badged refusal, not a confusing "no such
        // function".
        consult.refuse_if_blueprint_fq(ns)?;
        Ok(
            match consult
                .select_entity(callee.as_str(), callee.is_stropped(), ns, reach)?
                .unique_or_refuse(callee.as_str())?
            {
                Some(super::select::Selected::Authored(family)) if capable(family.kind()) => {
                    Some(family)
                }
                _ => None,
            },
        )
    } else {
        let candidates = consult.select_enlisted(callee.as_str(), callee.is_stropped(), reach)?;
        match super::select::judge_position(callee.as_str(), candidates, capable)? {
            super::select::PositionOutcome::Selected(super::select::Selected::Authored(family)) => {
                Ok(Some(family))
            }
            _ => Ok(None),
        }
    }
}

/// The immediate terminal for a SAME-KEY scalar self-reference: a value
/// definition has no fixpoint to re-enter.
fn scalar_recursion_refusal(name: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "cfe/recursion",
        format!(
            "the instantiation of '{name}' reaches itself with the same actuals: \
             a value definition cannot recurse"
        ),
        "a scalar definition computes from its inputs; write recursion as a relational rule",
    )
}

fn scalar_cycle_refusal(name: &str, chain: Vec<String>) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "cfe/recursion",
        format!(
            "the value instantiation of '{name}' enters the cycle {}: a value definition \
             has no relational fixpoint to re-enter",
            chain.join(" -> ")
        ),
        "break the value-definition cycle, or write recursion as a relational rule",
    )
}

/// The ruled terminal for a CHANGED-KEY scalar self-reference, observed
/// the moment the changed actual is requested.
fn scalar_widening_refusal(
    name: &str,
    building: &[String],
    requested: &[String],
) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::RECURSION_PARAMETER_WIDENING,
        format!(
            "'{name}' is recursive and its self-reference changes an actual \
             (building [{}], requested [{}]). A value definition's actuals select \
             ONE instantiation; state that changes between recursive steps belongs \
             in a relational rule.",
            building.join(", "),
            requested.join(", "),
        ),
        "recursive parameters never widen",
    )
}
