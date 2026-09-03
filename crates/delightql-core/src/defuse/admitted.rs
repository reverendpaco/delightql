// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE ADMITTED OPERATIONS — the construction-owned definition use.
//!
//! Every value here owns the selected declaration as the statement's
//! catalog read holds it, every caller-resolved actual, AND the body syntax
//! being resolved; each
//! has exactly one consuming operation producing the resolved artifact.
//! The fields are PRIVATE TO THIS FILE: the road code in `bound_use`
//! consumes these values but can never assemble one from loose pieces —
//! a declaration, a body, a formal map, and an instance admission come
//! together only in this module's transitions, each of which derives them
//! from one bound carrier's own parts.

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::ddl::HoParam;
use crate::pipeline::asts::unresolved as ast_unresolved;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::resolution::ResolverCore;

use crate::pipeline::ast_transform::AstTransform;

use super::bound_use::mutual_recursion_refusal;
use super::bound_use::{CallableGrade, CallableUse, ModeUse, SquishedExpansion};
use super::environment::{
    BodyEnvironment, DeclarationEnvironment, EnclosingRow, Environment, FormalInventory, FormalRole,
};
use super::instance::InstanceTable;
use super::select::LinkedFamily;
use crate::resolution::ConsultRegistry;

/// THE LICENSE TO OPEN A BODY WORLD. Its one field is private to this
/// file, so only this authority's own world derivation can mint it — and
/// [`BodyEnvironment::open`] requires one. A declaration, relation formals,
/// and scalar formals therefore meet in a body world only through
/// [`world_of`] (derived from one owned admission) or the compiler's own
/// synthesized road below; no other module can assemble one from parts.
pub(in crate::defuse) struct BodyOpening(());

/// THE POSITION'S FACTS a body resolution inherits: the replay-marked
/// config, the position's grade, the enclosing row where the body declared
/// `..`, and a windowed position's obligation. Captured from the caller
/// BEFORE the body opens — the body door never sees the caller's fold, and
/// no data world crosses from the caller: a body's holes are bound by its
/// own declaring namespace's publication alone.
pub(in crate::defuse) struct BodyPositionFacts<'p> {
    config: crate::pipeline::resolver::ResolutionConfig,
    grade: CallableGrade,
    enclosing_row: Option<super::environment::EnclosingRow<'p>>,
    window_obligation: Option<crate::pipeline::resolver::resolver_fold::WindowObligation>,
}

impl<'p> BodyPositionFacts<'p> {
    /// The plain position: no enclosing row, no window obligation. Borrows
    /// nothing of the caller: the row, when one rides in, is added after.
    fn of(caller: &ResolverFold<'_, '_>) -> BodyPositionFacts<'p> {
        BodyPositionFacts {
            config: crate::pipeline::resolver::ResolutionConfig {
                // Every body resolution is a REPLAY — an instantiated
                // definition — so the authored-environment judgments stay
                // with the submission that authored them.
                authored_environment: false,
                ..caller.config.clone()
            },
            grade: caller.position_grade,
            enclosing_row: None,
            window_obligation: None,
        }
    }

    /// A QUERY-SCOPED body's position: the body is the caller's own
    /// authored text, so the caller's judgments stay in force — nothing
    /// here is a replay.
    fn authored_of(caller: &ResolverFold<'_, '_>) -> Self {
        BodyPositionFacts {
            config: caller.config.clone(),
            grade: caller.position_grade,
            enclosing_row: None,
            window_obligation: None,
        }
    }

    fn with_enclosing_row(mut self, row: Option<super::environment::EnclosingRow<'p>>) -> Self {
        self.enclosing_row = row;
        self
    }

    fn with_window_obligation(
        mut self,
        obligation: Option<crate::pipeline::resolver::resolver_fold::WindowObligation>,
    ) -> Self {
        self.window_obligation = obligation;
        self
    }
}

/// A BODY WORLD from its parts — called ONLY by [`world_of`] (the owned
/// admission's world) and the compiler-synthesized road: a body world never
/// exists apart from a consumed admitted use or a synthesized body, and the
/// opening license minted here is the proof.
fn body_env(
    declaration: DeclarationEnvironment,
    carriers: &crate::defuse::carriers::CarrierRecord,
    formals: super::environment::FormalBindings,
) -> Environment {
    Environment::Body(BodyEnvironment::open(
        BodyOpening(()),
        declaration,
        carriers,
        formals,
    ))
}

/// A body fold over an opened world — the admitted operations' shared
/// private seam; nothing outside them reaches it.
fn body_fold_in<'a, 'db>(
    core: &'a mut ResolverCore<'db>,
    env: &'a mut Environment,
    facts: BodyPositionFacts<'a>,
) -> ResolverFold<'a, 'db> {
    let BodyPositionFacts {
        config,
        grade,
        enclosing_row,
        window_obligation,
    } = facts;
    // A body is SEALED against the caller's row unless `..` declared it
    // reads that row: then the body's position is enclosed by the
    // caller's, through a borrow, exactly as an interior expression is.
    let mut fold = match enclosing_row {
        Some(row) => ResolverFold::enclosed(core, env, config, row.0),
        None => ResolverFold::new(core, env, config),
    };
    fold.position_grade = grade;
    fold.window_obligation = window_obligation;
    fold
}

/// THE CLOSED ADMITTED-OPERATION INVENTORY. Each value owns the selected
/// declaration as the statement's read holds it, every caller-resolved
/// actual, AND the body syntax being resolved; each has exactly ONE consuming
/// operation producing the resolved artifact. No operation accepts a
/// body, query, truth, declaration, environment, or formal map as an
/// argument — what resolves is what was admitted.

/// THE ONE WORLD DERIVATION: a body world comes from the OWNED admission
/// and nowhere else. Every consuming operation below borrows its bound
/// use through here, so the admitted instance is alive BY TYPE for the
/// resolution's whole extent. No caller fact enters the world's data
/// binding: the declaration's own namespace publishes it.
fn world_of<A: ActualPayload>(
    bound: &BoundUse<A>,
    carriers: &crate::defuse::carriers::CarrierRecord,
    formals: super::environment::FormalBindings,
) -> Environment {
    body_env(bound.declaration.clone(), carriers, formals)
}

fn resolve_query_in<A: ActualPayload>(
    core: &mut ResolverCore<'_>,
    facts: BodyPositionFacts<'_>,
    bound: &BoundUse<A>,
    carriers: &crate::defuse::carriers::CarrierRecord,
    formals: super::environment::FormalBindings,
    query: ast_unresolved::Query,
) -> Result<crate::pipeline::resolver::ResolvedQuery> {
    let mut env = world_of(bound, carriers, formals);
    let mut fold = body_fold_in(core, &mut env, facts);
    crate::pipeline::resolver::resolve_query_with(&mut fold, query)
}

fn resolve_truth_in<A: ActualPayload>(
    core: &mut ResolverCore<'_>,
    facts: BodyPositionFacts<'_>,
    bound: &BoundUse<A>,
    formals: super::environment::FormalBindings,
    truth: ast_unresolved::TruthExpression,
) -> Result<crate::pipeline::asts::resolved::TruthExpression> {
    let mut env = world_of(bound, &crate::defuse::carriers::CarrierRecord::default(), formals);
    let mut fold = body_fold_in(core, &mut env, facts);
    fold.transform_boolean(truth)
}

/// A VALUE body in the owned admission's world. The window obligation the
/// position armed (if any) rides in through the facts and back out beside
/// the value — a windowed wrapper use reads whether the body took it.
fn resolve_value_in<A: ActualPayload>(
    core: &mut ResolverCore<'_>,
    facts: BodyPositionFacts<'_>,
    bound: &BoundUse<A>,
    formals: super::environment::FormalBindings,
    body: ast_unresolved::DomainExpression,
) -> (
    Result<crate::pipeline::asts::resolved::DomainExpression>,
    Option<crate::pipeline::resolver::resolver_fold::WindowObligation>,
) {
    let mut env = world_of(bound, &crate::defuse::carriers::CarrierRecord::default(), formals);
    let mut fold = body_fold_in(core, &mut env, facts);
    let outcome = fold.transform_domain(body);
    let obligation = fold.window_obligation.take();
    (outcome, obligation)
}

/// A consumed position's held world: the OWNED admission (alive until the
/// arm finishes resolving) or a scoped binding's held instance.
enum Held<'s, A: ActualPayload> {
    Bound(BoundUse<'s, A>),
    Scoped(super::instance::InstanceFrame),
}

impl<A: ActualPayload + Default> BoundUse<'_, A> {
    /// Spend the caller-resolved actuals ONCE; the admission — family,
    /// declaration, and the held instance — stays whole and alive.
    fn spend_actuals(&mut self) -> A {
        std::mem::take(&mut self.actuals)
    }
}

/// An admitted ER CHAIN: a NONEMPTY sequence of atomic admitted edges,
/// each carrying the context and the pair that selected it. Adjacency and
/// the shared endpoint are DERIVED from neighboring members as the chain
/// is linked — there is no second array to disagree with the members, and
/// a member cannot be paired with another edge's endpoints. The members'
/// common declaration is proven at each link, every admission stays alive
/// until resolution completes, and the composition is the authority's own
/// law over its own members.
pub(in crate::defuse) struct AdmittedErChain<'s> {
    members: Vec<ErEdgeUse<'s>>,
}

impl<'s> AdmittedErChain<'s> {
    /// The chain's first link.
    pub(in crate::defuse) fn link(first: ErEdgeUse<'s>) -> Self {
        AdmittedErChain {
            members: vec![first],
        }
    }

    /// Extend the chain to `right`: the next edge is SELECTED, ADMITTED, and
    /// OPENED here, from the chain's own context and from its last edge's
    /// own right term — the shared endpoint is one token the chain holds,
    /// never a second copy compared afterwards — and the new edge must
    /// share the chain's declaring namespace.
    pub(in crate::defuse) fn then(
        mut self,
        fold: &ResolverFold<'_, 's>,
        right: super::er::ErTerm,
    ) -> Result<Self> {
        let last = self
            .members
            .last()
            .expect("a chain is linked from its first edge");
        let next = super::er::use_er_edge(fold, &last.context, last.right.clone(), right)?;
        if next.bound.family.namespace() != last.bound.family.namespace() {
            return Err(DelightQLError::validation_error_categorized(
                "er/chain/declaration",
                format!(
                    "an ER chain's edges must share one declaring namespace; \
                     found '{}' and '{}'",
                    last.bound.family.namespace(),
                    next.bound.family.namespace(),
                ),
                "ER chain declaration identity",
            ));
        }
        self.members.push(next);
        Ok(self)
    }

    /// Compose and resolve in the chain's ONE proven declaration world.
    /// The composition is the authority's chain-merge law over the members
    /// themselves — each body beside the pair that selected it, the shared
    /// endpoint being the member's own left term — and EVERY member's
    /// admission is alive through the resolution.
    pub(in crate::defuse) fn resolve(
        self,
        caller: &mut ResolverFold<'_, '_>,
    ) -> Result<crate::pipeline::resolver::ResolvedQuery> {
        let AdmittedErChain { members } = self;
        let context = members
            .first()
            .expect("a chain is linked from its first edge")
            .context
            .clone();
        // THE SPEND: each member's opened body flows into the composition
        // here, at the one consuming act, beside the terms that selected
        // it; every member's ADMISSION stays alive (in `bounds`) until the
        // composed body has resolved.
        let mut bounds = Vec::with_capacity(members.len());
        let mut links = Vec::with_capacity(members.len());
        for ErEdgeUse {
            bound,
            query,
            context: _,
            left,
            right,
        } in members
        {
            bounds.push(bound);
            links.push(super::er::ErLink {
                body: query,
                left,
                right,
            });
        }
        let combined = super::er::compose_chain(links, &context)?;
        let facts = BodyPositionFacts::of(caller);
        let outcome = resolve_query_in(
            &mut *caller.core,
            facts,
            bounds
                .first()
                .expect("construction proved at least one edge"),
            &crate::defuse::carriers::CarrierRecord::default(),
            super::environment::FormalBindings::default(),
            combined,
        );
        drop(bounds);
        outcome
    }
}

/// A SCOPED definition's admitted body: its formal frame and its syntax,
/// ONE value — born where the binding's actuals resolved, consumed whole
/// by its own resolving operations below. The frame goes into the world
/// under a lease and comes out again when the resolution ends; the pair
/// can never be re-assembled from loose pieces at a use site, and no
/// caller holds a pushed frame open.
pub(crate) struct ScopedBody {
    formals: super::environment::FormalBindings,
    body: crate::pipeline::ast_unresolved::DomainExpression,
}

impl ScopedBody {
    fn of(
        formals: super::environment::FormalBindings,
        body: crate::pipeline::ast_unresolved::DomainExpression,
    ) -> Self {
        ScopedBody { formals, body }
    }

    /// RESOLVE the admitted scoped body in the world it was born in: its
    /// frame stands on that world for exactly this resolution's extent,
    /// answered through a child fold, and the lease's drop restores the
    /// world on every path.
    pub(crate) fn resolve(
        self,
        fold: &mut ResolverFold<'_, '_>,
    ) -> Result<crate::pipeline::ast_resolved::DomainExpression> {
        use crate::pipeline::ast_transform::AstTransform;
        let ScopedBody { formals, body } = self;
        let config = fold.config.clone();
        let grade = fold.position_grade;
        let mut lease = fold.env.instantiated(formals);
        let mut child = ResolverFold::new(&mut *fold.core, lease.world(), config);
        child.position_grade = grade;
        child.transform_domain(body)
    }

    /// The SCALAR twin of [`Self::resolve`]: the enclosing row a `..` body
    /// declared and the window obligation ride in beside the admitted
    /// value, and the obligation rides back out.
    pub(crate) fn resolve_scalar(
        self,
        core: &mut ResolverCore<'_>,
        env: &mut Environment,
        config: crate::pipeline::resolver::ResolutionConfig,
        grade: CallableGrade,
        row: Option<EnclosingRow<'_>>,
        obligation: Option<crate::pipeline::resolver::resolver_fold::WindowObligation>,
    ) -> (
        Result<Option<crate::pipeline::ast_resolved::DomainExpression>>,
        Option<crate::pipeline::resolver::resolver_fold::WindowObligation>,
    ) {
        use crate::pipeline::ast_transform::AstTransform;
        let ScopedBody { formals, body } = self;
        let mut lease = env.instantiated(formals);
        let mut child = match row {
            Some(row) => ResolverFold::enclosed(&mut *core, lease.world(), config, row.0),
            None => ResolverFold::new(&mut *core, lease.world(), config),
        };
        child.position_grade = grade;
        child.window_obligation = obligation;
        let out = child.transform_domain(body).map(Some);
        let obligation = child.window_obligation.take();
        (out, obligation)
    }
}

impl super::bound_use::BoundUse<'_, ValueActuals> {
    /// THE SIGMA CONSUMING OPERATION: opens the clauses, and resolves each
    /// clause in ITS OWN BODY WORLD — the rule's declaration and a
    /// params-only formal frame holding THIS use's resolved actuals (never
    /// the caller's frame beneath them: an undeclared name in the body has
    /// no lawful binding and refuses) — then ORs the alternatives.
    pub(in crate::defuse) fn resolve_sigma(
        self,
        caller: &mut ResolverFold<'_, '_>,
        functor: &str,
    ) -> Result<crate::pipeline::asts::resolved::TruthExpression> {
        use crate::pipeline::asts::ddl::DefKind;
        let group = self.reconstruct_group().map_err(|e| {
            DelightQLError::parse_error(format!(
                "No definitions found for sigma predicate '{functor}': {e}"
            ))
        })?;
        if group.kind() != DefKind::Sigma {
            return Err(DelightQLError::parse_error(format!(
                "Expected sigma predicate definition for '{}', got {:?}",
                functor,
                group.kind()
            )));
        }
        let ValueActuals(resolved_args) = &self.actuals;

        let mut clause_truths: Vec<crate::pipeline::asts::resolved::TruthExpression> = Vec::new();
        for clause in group.clauses() {
            let params = clause.params();
            if params.len() != resolved_args.len() {
                return Err(DelightQLError::validation_error(
                    format!(
                        "Sigma predicate '{}' expects {} arguments, got {}",
                        functor,
                        params.len(),
                        resolved_args.len()
                    ),
                    "Arity mismatch",
                ));
            }
            // A sigma rule's body is a TRUTH, and the parse-level
            // category says so: `p(x) :- users` never becomes one of
            // these.
            let truth = clause
                .as_truth_expr()
                .ok_or_else(|| {
                    DelightQLError::parse_error(format!(
                        "Sigma predicate '{}' clause has no truth body",
                        functor
                    ))
                })?
                .clone();
            let mut inventory = FormalInventory::declared(
                params
                    .iter()
                    .map(|param| (param.name().clone(), FormalRole::Value)),
            );
            inventory.bind_positional(FormalRole::Value, resolved_args.iter().cloned())?;
            let formals = inventory.sealed();
            // Each clause opens its own body world: its formals and its
            // declaration, nothing of the caller's row or bindings.
            let facts = BodyPositionFacts::of(caller);
            clause_truths.push(resolve_truth_in(
                &mut *caller.core,
                facts,
                &self,
                formals,
                truth,
            )?);
        }

        // Every clause is an alternative: the predicate holds if any does.
        Ok(
            crate::pipeline::asts::resolved::TruthExpression::any(clause_truths)
                .expect("a sigma definition group has at least one clause"),
        )
    }
}

/// One callable use OPENED: the definition's one callable form, the
/// caller's actuals spent once, and the admission held for the extent of
/// the consuming operation. The one prologue every callable position
/// shares — scalar call, cover cell, pattern slot — derived from the
/// carrier's own parts.
struct OpenedCallable<'s, A: ActualPayload> {
    cfe: crate::pipeline::asts::unresolved::CfeDefinition,
    actuals: A,
    held: Held<'s, A>,
}

impl<'s, A: ActualPayload + Default> CallableUse<'s, A> {
    fn open(self) -> Result<OpenedCallable<'s, A>> {
        Ok(match self {
            CallableUse::Bound(mut bound) => {
                let cfe = shape_callable(&bound)?;
                let actuals = bound.spend_actuals();
                OpenedCallable {
                    cfe,
                    actuals,
                    held: Held::Bound(bound),
                }
            }
            CallableUse::Scoped(ScopedBoundUse {
                cfe,
                actuals,
                _frame,
            }) => OpenedCallable {
                cfe,
                actuals,
                held: Held::Scoped(_frame),
            },
        })
    }
}

/// The categorized cardinality refusal of a callable's formal binding,
/// spelled with the callable's name.
fn named_binding_refusal(name: &str, error: DelightQLError) -> DelightQLError {
    match error {
        DelightQLError::ValidationError {
            message,
            context,
            subcategory,
        } => DelightQLError::ValidationError {
            message: format!("'{name}': {message}"),
            context,
            subcategory,
        },
        other => other,
    }
}

impl CallableUse<'_, ScalarActuals> {
    /// THE SCALAR-CALL CONSUMING OPERATION: shapes the callable, judges
    /// the context and arity laws on the admitted shape, builds the
    /// formal frame FROM THIS USE'S ACTUALS (code first, captures, then
    /// values — captures resolve at the call site, in the caller's
    /// world), opens the body's own world SEALED — `..` is the one
    /// deliberate unsealing, and it admits the caller's ROW, never its
    /// bindings — resolves the body there, and returns. Consumes self;
    /// the instance is released when the operation returns.
    pub(in crate::defuse) fn apply_call(
        self,
        fold: &mut ResolverFold<'_, '_>,
        name: &str,
        supplied_members: usize,
    ) -> Result<Option<crate::pipeline::asts::resolved::DomainExpression>> {
        use crate::pipeline::asts::core::ContextMode;
        let OpenedCallable { cfe, actuals, held } = self.open()?;
        let ScalarActuals {
            context: context_call,
            code: held_code,
            values: held_values,
        } = actuals;

        // The context and arity laws, judged on the ADMITTED, opened shape.
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
        let (callable_formals, scalar_formals) = cfe.split_formals();
        let curried_count = callable_formals.len();
        // Explicit captures called positionally are leading positionals; a
        // context call binds them by name instead, so they are not counted.
        let positional_captures = match (&cfe.context_mode, context_call) {
            (ContextMode::Explicit(captures), false) => captures.len(),
            _ => 0,
        };
        let declared = curried_count + positional_captures + scalar_formals.len();
        if supplied_members != declared {
            // A context call is lenient like the road it replaces: the
            // capture that cannot bind is what refuses, below, by name.
            if !context_call {
                if positional_captures > 0 {
                    return Err(DelightQLError::parse_error(format!(
                        "'{name}' expects {declared} positional argument{} (captures first), got {}",
                        if declared == 1 { "" } else { "s" },
                        supplied_members
                    )));
                }
                return Err(DelightQLError::validation_error_categorized(
                    "cfe/arity",
                    format!(
                        "'{name}' expects {declared} argument{}, got {}",
                        if declared == 1 { "" } else { "s" },
                        supplied_members
                    ),
                    "supply one argument per declared parameter, code first",
                ));
            }
        }

        // Arguments are the CALLER's expressions, already resolved before
        // the body opened. The body resolves with its formal bindings
        // instead of textual substitution — a spliced name never
        // re-resolves inside the body's probes, so nothing the body opens
        // can capture it.
        let inventory = {
            use crate::pipeline::asts::core::ContextMode;
            let mut declared: Vec<(delightql_types::SqlIdentifier, FormalRole)> = callable_formals
                .iter()
                .map(|formal| (formal.name.clone(), FormalRole::Callable))
                .collect();
            if let ContextMode::Explicit(captures) = &cfe.context_mode {
                declared.extend(
                    captures
                        .iter()
                        .map(|capture| (capture.clone(), FormalRole::Capture)),
                );
            }
            declared.extend(
                scalar_formals
                    .iter()
                    .map(|formal| (formal.name.clone(), FormalRole::Value)),
            );
            FormalInventory::declared(declared)
        };
        let mut inventory = inventory;
        // The catalog's code-parameter facts and the opened formals agree,
        // or the binding REFUSES — total in every build, never a truncating
        // `zip` behind a debug assertion.
        inventory
            .bind_callables_positional(held_code)
            .map_err(|error| named_binding_refusal(name, error))?;
        let mut resolved_values = held_values.into_iter();
        if let ContextMode::Explicit(captures) = &cfe.context_mode {
            if context_call {
                // `..` binds each declared capture BY NAME, resolved at
                // the call site, THROUGH the issued inventory — a name the
                // family never declared refuses.
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
                    let resolved = fold.transform_domain(reference)?;
                    inventory.bind_named(capture, resolved)?;
                }
            } else {
                // Positional: the captures are the leading value positions.
                let taken: Vec<_> = resolved_values.by_ref().take(captures.len()).collect();
                inventory
                    .bind_positional(FormalRole::Capture, taken)
                    .map_err(|error| named_binding_refusal(name, error))?;
            }
        }
        inventory
            .bind_positional(FormalRole::Value, resolved_values)
            .map_err(|error| named_binding_refusal(name, error))?;
        let frame = inventory.sealed();
        // THE BODY OPENS IN ITS OWN WORLD with exactly the frame this
        // use's actuals derive; a definition's body is SEALED — only its
        // formals, and for a context-aware definition its declared
        // captures, reach the call site's row. Implicit context is the one
        // deliberate unsealing: `..` DECLARES that free names capture from
        // the caller's ROW — the row, never the caller's bindings.
        let implicit = matches!(cfe.context_mode, ContextMode::Implicit);
        let horizon = cfe.horizon();
        let enclosing_row = implicit.then(|| EnclosingRow(&fold.lexical));
        let body = cfe.body;
        // A windowed use's obligation flows into the body's resolution and
        // back out: the position's grade is a contract, not lexical state.
        let obligation = fold.window_obligation.take();
        let (outcome, obligation) = match held {
            Held::Bound(bound) => {
                let facts = BodyPositionFacts::of(fold)
                    .with_enclosing_row(enclosing_row)
                    .with_window_obligation(obligation);
                let (outcome, obligation) =
                    resolve_value_in(&mut *fold.core, facts, &bound, frame, body);
                (outcome.map(Some), obligation)
            }
            Held::Scoped(_frame) => {
                // The scoped twin resolves in the SAME world it was born
                // in: its admitted frame-and-body value resolves itself,
                // sealed against the caller's row unless `..` declared
                // otherwise.
                fold.env.push_horizon(horizon);
                let config = fold.config.clone();
                let grade = fold.position_grade;
                let resolved = ScopedBody::of(frame, body).resolve_scalar(
                    &mut *fold.core,
                    &mut *fold.env,
                    config,
                    grade,
                    implicit.then_some(()).and(enclosing_row),
                    obligation,
                );
                fold.env.pop_horizon();
                let (outcome, obligation) = resolved;
                (outcome, obligation)
            }
        };
        fold.window_obligation = obligation;
        outcome
    }
}

impl CallableUse<'_, ValueActuals> {
    /// THE COVER-CELL CONSUMING OPERATION. The cover is the applying
    /// position, and the row it hands over is already COMPLETE: the
    /// mention's written partials bind a complete left prefix and the
    /// covered cell took the default landing after them, so this binds
    /// the whole row positionally like any other call and holds no idea
    /// of a distinguished cell position. The instantiated body resolves
    /// CLOSED, in its own world. A context or higher-order definition has
    /// no one-cell reading: Ok(None) and the ordinary callable road keeps
    /// whatever meaning it had.
    pub(in crate::defuse) fn apply_cover(
        self,
        fold: &mut ResolverFold<'_, '_>,
        name: &str,
    ) -> Result<Option<crate::pipeline::asts::resolved::DomainExpression>> {
        use crate::pipeline::asts::core::ContextMode;
        let OpenedCallable { cfe, actuals, held } = self.open()?;
        let ValueActuals(values) = actuals;
        if cfe.context_mode != ContextMode::None || !cfe.callable_formals().is_empty() {
            return Ok(None);
        }
        let formals = cfe.scalar_formals();
        if formals.is_empty() {
            return Err(DelightQLError::validation_error_categorized(
                "cfe/cover_arity",
                format!("'{name}' takes no parameters, so a cover cannot land the cell in one"),
                "the covered callable's final parameter receives each cell",
            ));
        }
        if values.len() != formals.len() {
            return Err(DelightQLError::validation_error_categorized(
                "cfe/cover_arity",
                format!(
                    "'{name}' has {} parameter{} before the cell; the mention supplies {}",
                    formals.len() - 1,
                    if formals.len() == 2 { "" } else { "s" },
                    values.len() - 1
                ),
                "the cell lands last; supply one value per preceding parameter",
            ));
        }
        let mut inventory = FormalInventory::declared(
            formals
                .iter()
                .map(|formal| (formal.name.clone(), FormalRole::Value)),
        );
        inventory
            .bind_positional(FormalRole::Value, values)
            .map_err(|error| named_binding_refusal(name, error))?;
        let frame = inventory.sealed();
        let horizon = cfe.horizon();
        let body = cfe.body;
        let outcome = match held {
            Held::Bound(bound) => {
                let facts = BodyPositionFacts::of(fold);
                resolve_value_in(&mut *fold.core, facts, &bound, frame, body).0
            }
            Held::Scoped(_frame) => {
                fold.env.push_horizon(horizon);
                let resolved = ScopedBody::of(frame, body).resolve(fold);
                fold.env.pop_horizon();
                resolved
            }
        };
        Ok(Some(outcome?))
    }

    /// THE SLOT CONSUMING OPERATION: values bind the declared formals,
    /// the body resolves SEALED through its own converter (the body sees
    /// its formals, never the pattern's heading), and sibling lookups
    /// answer in the definition's own world. A curried or context-aware
    /// definition has no slot reading: Ok(None).
    pub(in crate::defuse) fn apply_slot(
        self,
        converter: &mut crate::pipeline::resolver::StrictPhaseConverter<'_, '_>,
        instantiation: crate::pipeline::resolver::SlotInstantiation<'_, '_>,
        name: &str,
    ) -> Result<Option<crate::pipeline::asts::resolved::DomainExpression>> {
        let OpenedCallable { cfe, actuals, held } = self.open()?;
        let ValueActuals(values) = actuals;
        if !cfe.callable_formals().is_empty()
            || cfe.context_mode != crate::pipeline::asts::core::ContextMode::None
        {
            return Ok(None);
        }
        let scalar_formals = cfe.scalar_formals();
        if values.len() != scalar_formals.len() {
            return Err(DelightQLError::validation_error_categorized(
                "cfe/arity",
                format!(
                    "'{name}' expects {} argument{}, got {}",
                    scalar_formals.len(),
                    if scalar_formals.len() == 1 { "" } else { "s" },
                    values.len()
                ),
                "supply one value per declared parameter",
            ));
        }
        let mut inventory = FormalInventory::declared(
            scalar_formals
                .iter()
                .map(|formal| (formal.name.clone(), FormalRole::Value)),
        );
        inventory
            .bind_positional(FormalRole::Value, values)
            .map_err(|error| named_binding_refusal(name, error))?;
        let frame = inventory.sealed();
        // THE ADMITTED SLOT BODY: for a consulted family, the world opens
        // from the admitted parts and the body converts through the sealed
        // door — no environment leaves this arm; for a scoped one, the
        // frame-and-body value is consumed whole against the caller's own
        // allowance. Either way the world and the body cross as ONE closed
        // value, and the converter consumes the variant it is handed.
        let world = match held {
            Held::Bound(ref bound) => SealedSlotWorld::Opened(world_of(bound, &crate::defuse::carriers::CarrierRecord::default(), frame)),
            Held::Scoped(_) => SealedSlotWorld::Scoped(frame, cfe.horizon()),
        };
        let outcome = SealedSlot {
            world,
            body: cfe.body,
        }
        .convert(converter.registry(), instantiation);
        drop(held);
        outcome
    }
}

/// THE SEALED SLOT: the world (or the scoped frame) and the body as ONE
/// value, private to this file — constructed from the consumed use's own
/// parts and spent only by its own converting operation. No signature
/// anywhere accepts the world and the body independently.
struct SealedSlot {
    world: SealedSlotWorld,
    body: crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Unresolved>,
}

/// The world a sealed slot body resolves in — a closed sum: an OPENED
/// consulted world, or a scoped binding's frame standing on the caller's
/// own world. There is no third state and no combination of the two.
enum SealedSlotWorld {
    Opened(Environment),
    Scoped(
        super::environment::FormalBindings,
        crate::pipeline::asts::core::LexicalHorizon,
    ),
}

impl SealedSlot {
    /// THE CONSUMING OPERATION: convert the body in the world it was sealed
    /// with. The allowance stands in that world — an opened world on its
    /// own, a scoped frame on the caller's — and the strict converter
    /// realizes the body under it. The variant is consumed here; nothing
    /// receives the world beside the body.
    fn convert(
        self,
        registry: &crate::names::Registry,
        allowance: crate::pipeline::resolver::SlotInstantiation<'_, '_>,
    ) -> Result<Option<crate::pipeline::asts::resolved::DomainExpression>> {
        use crate::pipeline::resolver::StrictPhaseConverter;
        let SealedSlot { world, body } = self;
        let nested = match &world {
            SealedSlotWorld::Opened(env) => allowance.in_opened(env),
            SealedSlotWorld::Scoped(frame, horizon) => allowance.in_scoped(frame, *horizon),
        };
        let mut converter = StrictPhaseConverter::sealed(registry, nested);
        converter.transform_domain(body).map(Some)
    }
}

/// WHAT AN EFFECT DEMAND SELECTED: the query's own effect-mirror CHOE, or a
/// consulted effect rule. Both invoke through one entrance.
pub(crate) enum EffectSelection<'s> {
    Consulted(EffectUse<'s>),
    Scoped(ScopedEffectUse),
}

impl EffectSelection<'_> {
    /// The rule's name, `!` included.
    pub(crate) fn rule_name(&self) -> delightql_types::SqlIdentifier {
        match self {
            EffectSelection::Consulted(consulted) => consulted.rule_name().clone(),
            EffectSelection::Scoped(scoped) => scoped.rule_name(),
        }
    }

    /// The exact declared row, reconstructed from the same selected family
    /// for consulted effects and read directly from a scoped definition.
    pub(crate) fn declared_params(&self) -> Result<Vec<HoParam>> {
        match self {
            EffectSelection::Consulted(consulted) => Ok(crate::ddl::reconstruct::group(
                consulted.family.definition(),
            )?
            .params()
            .to_vec()),
            EffectSelection::Scoped(scoped) => Ok(scoped.definition.group().params().to_vec()),
        }
    }

    pub(crate) fn invoke(
        self,
        instances: &InstanceTable,
        resolved_arguments: Vec<crate::pipeline::asts::resolved::DomainExpression>,
        rule_arguments: std::collections::HashMap<
            delightql_types::SqlIdentifier,
            super::ho::RuleValueId,
        >,
        builder: &mut crate::pipeline::effect_transformer::PlanBuilder<'_>,
        piped: Option<crate::relation::ScratchRow>,
        ctx: &crate::pipeline::effect_transformer::WalkCtx<'_>,
        root: bool,
    ) -> Result<crate::pipeline::ast_unresolved::Chain> {
        match self {
            EffectSelection::Consulted(consulted) => consulted.invoke(
                instances,
                resolved_arguments,
                rule_arguments,
                builder,
                piped,
                ctx,
                root,
            ),
            EffectSelection::Scoped(scoped) => scoped.invoke(
                instances,
                resolved_arguments,
                rule_arguments,
                builder,
                piped,
                ctx,
            ),
        }
    }
}

/// The query's own EFFECT-MIRROR CHOE, selected by a demand and not yet
/// invoked. Its body is the query's own text: it compiles IN THE DEMAND
/// SITE'S WORLD, through a body frame holding the parameter frame and the
/// definition's lexical horizon, exactly as a pure CHOE resolves — the
/// piped input binds its relation formal as a consulted rule's does.
pub(crate) struct ScopedEffectUse {
    definition: crate::pipeline::asts::core::HoDefinition,
}

impl ScopedEffectUse {
    pub(crate) fn of(definition: crate::pipeline::asts::core::HoDefinition) -> Self {
        ScopedEffectUse { definition }
    }

    pub(crate) fn rule_name(&self) -> delightql_types::SqlIdentifier {
        delightql_types::SqlIdentifier::new(format!("{}!", self.definition.name().as_str()))
    }

    pub(crate) fn scalar_param_count(&self) -> usize {
        self.definition
            .group()
            .params()
            .iter()
            .filter(|param| matches!(param, crate::pipeline::asts::ddl::HoParam::Scalar { .. }))
            .count()
    }

    fn invoke(
        self,
        instances: &InstanceTable,
        resolved_arguments: Vec<crate::pipeline::asts::resolved::DomainExpression>,
        rule_arguments: std::collections::HashMap<
            delightql_types::SqlIdentifier,
            super::ho::RuleValueId,
        >,
        builder: &mut crate::pipeline::effect_transformer::PlanBuilder<'_>,
        piped: Option<crate::relation::ScratchRow>,
        ctx: &crate::pipeline::effect_transformer::WalkCtx<'_>,
    ) -> Result<crate::pipeline::ast_unresolved::Chain> {
        let display_name = self.rule_name().to_string();
        let declared = self.scalar_param_count();
        if resolved_arguments.len() != declared {
            return Err(DelightQLError::validation_error_categorized(
                "effect/rule/arity",
                format!(
                    "effect rule '{display_name}' declares {declared} scalar parameter(s) and is \
                     invoked with {} argument(s)",
                    resolved_arguments.len()
                ),
                "scalar argument count does not match the rule's parameters",
            ));
        }
        let actuals = EffectActuals::of(resolved_arguments, rule_arguments);
        let frame = match instances.admit_scoped(self.definition.name(), actuals.scoped_key()) {
            super::instance::ScopedAdmission::Fresh(frame) => frame,
            super::instance::ScopedAdmission::Reenter
            | super::instance::ScopedAdmission::Cycle { .. }
            | super::instance::ScopedAdmission::Widening { .. } => {
                return Err(choe_recursion_refusal(&display_name));
            }
        };
        let mut rule = crate::pipeline::asts::effects::EffectRule::from_definition_group(
            self.definition.group(),
        )?;
        rule.strip_bound_param_heads();
        let formals = {
            let EffectActuals { values, rules } = actuals;
            let mut inventory =
                FormalInventory::declared(self.definition.group().params().iter().filter_map(
                    |param| match param {
                        HoParam::Scalar { name, .. } => Some((name.clone(), FormalRole::Value)),
                        HoParam::Rule { name, .. } => Some((name.clone(), FormalRole::Rule)),
                        HoParam::Relation { .. } | HoParam::Ground { .. } => None,
                    },
                ));
            inventory
                .bind_positional(FormalRole::Value, values)
                .map_err(|error| named_binding_refusal(&display_name, error))?;
            for (name, value) in rules {
                inventory.bind_rule_named(&name, value)?;
            }
            inventory.sealed()
        };
        // THE DEMAND SITE'S OWN WORLD, entered through a body frame for
        // exactly this invocation's extent.
        let lease = super::environment::SharedInstantiated::body(
            ctx.world_cell(),
            formals,
            self.definition.horizon().clone(),
        );
        let invoked = InvokedRule {
            rule,
            world: InvokedWorld::Caller {
                _lease: lease,
                horizon: self.definition.horizon().clone(),
            },
            _held: HeldInvocation::Scoped { _frame: frame },
        };
        builder.compile_invoked(&invoked, piped, ctx)
    }
}

/// One selected effect rule, NOT yet opened: invocation is the use, and
/// [`Self::invoke`] is its one entrance — the complete bound-use
/// transition (resolved actuals -> semantic key -> admission -> a fresh
/// body world with the parameter frame) with restoration owned
/// STRUCTURALLY by the entrance, never by a consumer convention.
pub(crate) struct EffectUse<'s> {
    family: LinkedFamily<'s>,
}

impl<'s> EffectUse<'s> {
    /// The rule's name, `!` included — a family fact for receipts and
    /// diagnostics.
    pub(crate) fn rule_name(&self) -> &delightql_types::SqlIdentifier {
        self.family.name()
    }

    /// INVOKE the rule AS ONE CLOSED OPERATION: the caller-RESOLVED
    /// actuals bind the use — semantic key, admission (re-encountering the
    /// rule while invoked is the R6 refusal), body opening and shaping —
    /// then plan compilation runs INSIDE this operation, under a world
    /// this operation OWNS for exactly its extent; the resolved effect
    /// artifact is what returns. The plan builder compiles the rule's
    /// statements through the world's own named resolving operations: it
    /// never receives the environment, cannot retain the world past this
    /// call, and has no stack of its own to place it on.
    pub(crate) fn invoke(
        self,
        instances: &InstanceTable,
        resolved_arguments: Vec<crate::pipeline::asts::resolved::DomainExpression>,
        rule_arguments: std::collections::HashMap<
            delightql_types::SqlIdentifier,
            super::ho::RuleValueId,
        >,
        builder: &mut crate::pipeline::effect_transformer::PlanBuilder<'_>,
        piped: Option<crate::relation::ScratchRow>,
        ctx: &crate::pipeline::effect_transformer::WalkCtx<'_>,
        root: bool,
    ) -> Result<crate::pipeline::ast_unresolved::Chain> {
        let EffectUse { family } = self;
        let display_name = family.name().to_string();
        let declared = family
            .params()
            .iter()
            .filter(|param| matches!(param, crate::pipeline::asts::ddl::HoParam::Scalar { .. }))
            .count();
        if resolved_arguments.len() != declared {
            return Err(DelightQLError::validation_error_categorized(
                "effect/rule/arity",
                format!(
                    "effect rule '{display_name}' declares {declared} scalar parameter(s) and is \
                     invoked with {} argument(s)",
                    resolved_arguments.len()
                ),
                "scalar argument count does not match the rule's parameters",
            ));
        }
        let bound = require_fresh(
            bind_definition_use(
                instances,
                family,
                EffectActuals::of(resolved_arguments, rule_arguments),
            )?,
            || {
                DelightQLError::validation_error_categorized(
                    "effect/transform/unsupported",
                    format!("effect rule '{display_name}' recursed during plan expansion (R6)"),
                    "unsupported in the v0.1 effect transformer",
                )
            },
        )?;
        let group = bound.reconstruct_group()?;
        let mut rule = crate::pipeline::asts::effects::EffectRule::from_definition_group(&group)?;
        // A parameterized rule's HEADS drop their scalar formals for the
        // landing decisions (the bound parameters leave the head; what a
        // rule still declares is what it still wants) — the BODIES keep
        // their parameter references, bound below through the frame.
        rule.strip_bound_param_heads();
        // THE PARAMETER FRAME: the caller-resolved values bind the
        // declared parameters by name, through the same formal-frame
        // mechanism every value definition uses — never textual
        // substitution of unresolved caller expressions into callee text.
        let formals = {
            let EffectActuals { values, rules } = &bound.actuals;
            let mut inventory =
                FormalInventory::declared(group.params().iter().filter_map(|param| match param {
                    HoParam::Scalar { name, .. } => Some((name.clone(), FormalRole::Value)),
                    HoParam::Rule { name, .. } => Some((name.clone(), FormalRole::Rule)),
                    HoParam::Relation { .. } | HoParam::Ground { .. } => None,
                }));
            inventory
                .bind_positional(FormalRole::Value, values.iter().cloned())
                .map_err(|error| named_binding_refusal(&display_name, error))?;
            for (name, value) in rules {
                inventory.bind_rule_named(name, *value)?;
            }
            inventory.sealed()
        };
        // THE INVOKED WORLD, owned HERE for exactly the compilation's
        // extent: a demanded PROGRAM (root) is the plan's own use world,
        // rooted at its namespace — its statements are the plan and read
        // the plan's creations. A rule invoked FROM a body opens a
        // consulted body world: only its formals cross. The builder
        // compiles the rule's statements through the world's named
        // operations and cannot reach the enclosing world while it stands.
        // THE PROGRAM WORLD READS UNDER THE FAMILY'S OWN READ: the registry
        // it captures the session reach through is built from the read
        // that selected the rule, never supplied beside it.
        let program_registry = ConsultRegistry::new_with_system(bound.family.catalog().system());
        let world = EffectWorld::of(if root {
            Environment::Use(super::environment::UseEnvironment::session_with_formals(
                &program_registry,
                bound.declaration.namespace(),
                formals,
            )?)
        } else {
            // An invoked rule's world is its own declaration's.
            body_env(bound.declaration.clone(), &crate::defuse::carriers::CarrierRecord::default(), formals)
        });
        // THE INVOKED ATOM: the shaped rule, the world it resolves in, and
        // the held admission are ONE value the builder compiles through —
        // it reads the rule's syntax and obtains walk contexts standing in
        // the world, and can pair neither with anything else.
        let invoked = InvokedRule {
            rule,
            world: InvokedWorld::Owned(world),
            _held: HeldInvocation::Bound { _bound: bound },
        };
        builder.compile_invoked(&invoked, piped, ctx)
    }
}

/// THE WORLD AN INVOKED RULE COMPILES IN: a consulted rule's own, owned
/// by the invocation; or — for the query's effect-mirror CHOE — the demand
/// site's world itself, entered through a body frame the invocation holds
/// open for its extent.
enum InvokedWorld<'w> {
    Owned(EffectWorld),
    Caller {
        _lease: super::environment::SharedInstantiated<'w>,
        horizon: crate::pipeline::asts::core::LexicalHorizon,
    },
}

/// The admission an invocation holds while its clauses compile — held for
/// its extent, read by nothing.
enum HeldInvocation<'s> {
    Bound {
        _bound: BoundUse<'s, EffectActuals>,
    },
    Scoped {
        _frame: super::instance::InstanceFrame,
    },
}

/// ONE INVOKED EFFECT RULE — the shaped rule, the world its statements
/// resolve in, and the admission held while they compile, as one value
/// with private fields. The plan builder compiles it through the
/// operations below: it may read the rule's syntax, and it may obtain a
/// walk context whose facts it supplies standing in THIS rule's world;
/// the world itself never leaves, so no planner entrance can accept a rule
/// beside a world, and nothing can pair this world with another rule.
pub(crate) struct InvokedRule<'s, 'w> {
    rule: crate::pipeline::asts::effects::EffectRule,
    world: InvokedWorld<'w>,
    _held: HeldInvocation<'s>,
}

impl InvokedRule<'_, '_> {
    /// The shaped rule's syntax.
    pub(crate) fn rule(&self) -> &crate::pipeline::asts::effects::EffectRule {
        &self.rule
    }

    /// The given walk facts standing in this invocation's world — the one
    /// road the world reaches a walk, inside a context for this rule's own
    /// clauses. A consulted rule's clauses stand in its own world with its
    /// clause's own block; a CHOE's stand in the demand site's world, in a
    /// block where the clause's own bindings stand FIRST and the site's
    /// follow.
    ///
    /// That order is the nesting, spelled in the one carrier a flat ledger
    /// has: the clause's text is the nearer scope, so all of it is visible
    /// and a spelling it declares SHADOWS the site's — which leaves the
    /// site's block entirely, claim and binding together. The site's
    /// remaining declarations follow, and the definition's horizon — read
    /// at the distance those claims moved — still cuts them exactly where
    /// the definition was written, so a site name declared after the
    /// definition remains a refusal rather than becoming a local miss.
    pub(crate) fn context_for<'a, 'f: 'a>(
        &'a self,
        clause: &crate::pipeline::asts::effects::EffectClause,
        facts: crate::pipeline::effect_transformer::WalkCtx<'f>,
    ) -> Result<crate::pipeline::effect_transformer::WalkCtx<'a>> {
        match &self.world {
            InvokedWorld::Owned(world) => Ok(facts
                .with_locals(clause.body.locals.clone())
                .standing_in(world)),
            InvokedWorld::Caller { horizon, .. } => {
                let nearer = clause.body.locals.clone();
                let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
                let site = facts.locals().visible_at(*horizon).shadowed_by(&nearer);
                block.absorb(nearer)?;
                let moved = block.absorb(site)?;
                Ok(facts
                    .with_locals(block.seal()?)
                    .at_horizon(horizon.shifted(moved))
                    .standing_in_caller())
            }
        }
    }
}

/// THE WORLD AN EFFECT PLAN RESOLVES IN — owned by the invocation that
/// built it (or by the plan's own root compilation for the session world)
/// and reachable by the plan builder only through the named operations
/// below. The environment inside never leaves: nothing outside this
/// authority can retain it, stack it, or pair it with another plan.
pub(crate) struct EffectWorld {
    world: std::cell::RefCell<Environment>,
}

impl EffectWorld {
    fn of(world: Environment) -> Self {
        EffectWorld {
            world: std::cell::RefCell::new(world),
        }
    }

    /// The world's cell, for a body frame lease over it — reachable only
    /// through a walk context, whose world this is.
    pub(crate) fn cell(&self) -> &std::cell::RefCell<Environment> {
        &self.world
    }

    /// The plan's OWN session world — the scope of statements standing
    /// outside every rule body, rooted at the plan's namespace (`home` for
    /// an ad-hoc statement).
    pub(crate) fn program(consult: &ConsultRegistry, root_fq: &str) -> Result<Self> {
        Ok(Self::of(Environment::Use(
            super::environment::UseEnvironment::session(consult, root_fq)?,
        )))
    }

    /// Register a relation an earlier statement of the plan created. A
    /// PROGRAM world reads it as its own state; a consulted body world
    /// reads a plan creation only through an explicit actual, never
    /// ambiently, so the registration lands nowhere there.
    pub(crate) fn register_materialized(
        &self,
        name: delightql_types::SqlIdentifier,
        relation: crate::relation::SemanticRelation,
    ) {
        if let Environment::Use(world) = &mut *self.world.borrow_mut() {
            world.register_materialized(name, relation);
        }
    }

    /// Resolve one of the plan's statements in this world.
    pub(crate) fn resolve_query(
        &self,
        core: &mut ResolverCore<'_>,
        config: crate::pipeline::resolver::ResolutionConfig,
        query: ast_unresolved::Query,
    ) -> Result<crate::pipeline::resolver::ResolvedQuery> {
        let mut world = self.world.borrow_mut();
        let mut fold = ResolverFold::new(core, &mut world, config);
        crate::pipeline::resolver::resolve_query_with(&mut fold, query)
    }

    /// Resolve a compiler-built application of one already-closed rule value.
    /// The opaque id enters through the same formal inventory and residual
    /// spending road as an authored higher-order body; callers provide only
    /// the synthetic formal spelling used by their compiler-built query.
    pub(crate) fn resolve_query_with_rule_value(
        &self,
        core: &mut ResolverCore<'_>,
        config: crate::pipeline::resolver::ResolutionConfig,
        query: ast_unresolved::Query,
        formal: delightql_types::SqlIdentifier,
        value: super::ho::RuleValueId,
    ) -> Result<crate::pipeline::resolver::ResolvedQuery> {
        let mut inventory =
            FormalInventory::declared(std::iter::once((formal.clone(), FormalRole::Rule)));
        inventory.bind_rule_named(&formal, value)?;
        let mut world = self.world.borrow_mut();
        let mut lease = world.instantiated(inventory.sealed());
        let mut fold = ResolverFold::new(core, lease.world(), config);
        crate::pipeline::resolver::resolve_query_with(&mut fold, query)
    }

    /// Resolve a demand site's row-free argument values in this world —
    /// the CALLER's actuals, resolved before the callee is admitted.
    pub(crate) fn resolve_values(
        &self,
        core: &mut ResolverCore<'_>,
        config: crate::pipeline::resolver::ResolutionConfig,
        values: Vec<ast_unresolved::DomainExpression>,
    ) -> Result<Vec<crate::pipeline::asts::resolved::DomainExpression>> {
        let mut world = self.world.borrow_mut();
        let mut fold = ResolverFold::new(core, &mut world, config);
        values
            .into_iter()
            .map(|value| fold.transform_domain(value))
            .collect()
    }

    /// Construct a pure closed residual at an effect demand site through the
    /// ordinary higher-order constructor. The caller world and resolver core
    /// meet only inside this owned operation.
    pub(crate) fn close_rule_value(
        &self,
        core: &mut ResolverCore<'_>,
        config: crate::pipeline::resolver::ResolutionConfig,
        designator: &ast_unresolved::Chain,
        expected: &crate::pipeline::asts::core::definitions::ResidualSignature,
        evaluation_relation: Option<crate::relation::ScratchRow>,
    ) -> Result<super::ho::RuleValueId> {
        let mut world = self.world.borrow_mut();
        let mut fold = ResolverFold::new(core, &mut world, config);
        super::carriers::construct_effect_residual(
            designator,
            expected,
            &mut fold,
            evaluation_relation,
            Vec::new(),
        )
    }

    /// Close a rule value at a built-in demand that stands inside an authored
    /// query-local block. The block enters through the same name and
    /// manifestation registration used by ordinary query resolution, so a
    /// query-scoped designator and any configured local relation capture keep
    /// their lexical identities.
    pub(crate) fn close_rule_value_in_locals(
        &self,
        core: &mut ResolverCore<'_>,
        config: crate::pipeline::resolver::ResolutionConfig,
        locals: crate::pipeline::asts::core::QueryLocals<crate::pipeline::asts::core::Unresolved>,
        designator: &ast_unresolved::Chain,
        expected: &crate::pipeline::asts::core::definitions::ResidualSignature,
        evaluation_relation: Option<crate::relation::ScratchRow>,
    ) -> Result<super::ho::RuleValueId> {
        let mut world = self.world.borrow_mut();
        let (names, cfes, hos, ctes) = locals.spend();
        world.push_query_names(names);
        let result = (|| {
            for cfe in cfes {
                world.register_query_local(super::environment::QueryLocalRegistration::Value(cfe));
            }
            for ho in hos {
                world.register_query_local(
                    super::environment::QueryLocalRegistration::HigherOrder(ho),
                );
            }
            let mut fold = ResolverFold::new(core, &mut world, config);
            let leading_ctes = if ctes.is_empty() {
                Vec::new()
            } else {
                crate::pipeline::bindings::resolve_cte_bindings(ctes, &mut fold)?
            };
            super::carriers::construct_effect_residual(
                designator,
                expected,
                &mut fold,
                evaluation_relation,
                leading_ctes,
            )
        })();
        world.pop_query_names();
        result
    }

    /// Select one effect family from this world's captured reach. Qualified
    /// and enlisted demands share the catalog's exhaustive selection and
    /// kind judgment; the returned use carries the exact selected family.
    pub(crate) fn select_effect_rule<'s>(
        &self,
        system: &'s crate::system::DelightQLSystem,
        namespace: Option<&str>,
        name: &str,
        stropped: bool,
    ) -> Result<Option<EffectUse<'s>>> {
        let consult = ConsultRegistry::new_with_system(system);
        let world = self.world.borrow();
        let selected = match namespace {
            Some(namespace) => consult
                .select_entity(name, stropped, namespace, world.reach())?
                .unique_or_refuse(name)?,
            None => match super::select::judge_position(
                name,
                consult.select_enlisted(name, stropped, world.reach())?,
                |kind| kind == crate::enums::EntityType::DqlEffectRule,
            )? {
                super::select::PositionOutcome::Selected(selected) => Some(selected),
                _ => None,
            },
        };
        match selected {
            Some(super::select::Selected::Authored(family))
                if family.kind() == crate::enums::EntityType::DqlEffectRule =>
            {
                Ok(Some(EffectUse { family }))
            }
            _ => Ok(None),
        }
    }
}

/// Use a name in EFFECT-RULE position: select the family in the demanded
/// namespace and judge the kind. The body does NOT open here — invocation
/// is the use, and [`EffectUse::invoke`] is its one entrance.
/// `None` is a position miss (no such rule in the namespace).
pub(crate) fn use_effect_rule<'s>(
    system: &'s crate::system::DelightQLSystem,
    namespace: &str,
    rule_name: &str,
) -> Result<Option<EffectUse<'s>>> {
    let consult = crate::resolution::registry::ConsultRegistry::new_with_system(system);
    // The demanded namespace's own reach, captured for this selection.
    let reach = super::environment::reach::capture(
        super::CatalogRead::of(system),
        namespace,
        super::environment::reach::World::Session,
    )?;
    let Some(super::select::Selected::Authored(family)) = consult
        .select_entity(rule_name, false, namespace, &reach)?
        .unique_or_refuse(rule_name)?
    else {
        return Ok(None);
    };
    if family.kind() != crate::enums::EntityType::DqlEffectRule {
        return Ok(None);
    }
    Ok(Some(EffectUse { family }))
}

/// The declaring namespace of a selected family, FOR DIAGNOSTICS ONLY —
/// refusal texts spell where a definition came from. Environment
/// decisions never pass through this string.
pub(in crate::defuse) fn family_display_namespace<'a>(family: &'a LinkedFamily<'_>) -> &'a str {
    family.namespace()
}

/// One DECLARED-MODE use: the catalog's declared functional dependency,
/// the authored arms held UNRESOLVED inside the authority, and the used
/// definition's qualified identity (for the mode witness) — selected,
/// admitted, opened, and AGREED as one act. The arms leave only RESOLVED,
/// through [`Self::resolve_arms`], in the declaration's own world.

impl ModeUse<'_> {
    /// Resolve the declared arms IN THE DECLARATION'S OWN WORLD, against
    /// the declared inputs and nothing else: an output cell's only binders
    /// are the inputs the head declared — no enclosing row, no caller
    /// binding, in either face.
    pub(crate) fn resolve_arms(
        self,
        caller: &mut ResolverFold<'_, '_>,
        inputs: crate::pipeline::resolver::ResolvedRelation,
    ) -> Result<crate::pipeline::asts::core::FactFunctionMode<crate::pipeline::asts::core::Resolved>>
    {
        let ModeUse {
            authored, bound, ..
        } = self;
        let facts = BodyPositionFacts::of(caller);
        let resolved = resolve_mode_in(&mut *caller.core, facts, &bound, authored, inputs)?;
        drop(bound);
        Ok(resolved)
    }
}

/// One SELECTED-and-ADMITTED relation use: the definition's name, its
/// position kind, its opened query, and the held admission — paired ONLY
/// by [`relation_use`], from one bound carrier's own parts.
pub(crate) struct RelationUse<'s> {
    name: delightql_types::SqlIdentifier,
    definition_kind: crate::relation::form::DefinitionKind,
    query: ast_unresolved::Query,
    bound: BoundUse<'s, NoActuals>,
}

impl RelationUse<'_> {
    /// Resolve the opened body in its declaration world, holding the
    /// recursion instance for exactly the resolution's extent, and hand
    /// back the RESOLVED artifacts the access boundary consumes: the
    /// definition's name, its position kind, and its resolved query.
    pub(crate) fn resolve_body(
        self,
        caller: &mut ResolverFold<'_, '_>,
    ) -> Result<(
        delightql_types::SqlIdentifier,
        crate::relation::form::DefinitionKind,
        crate::pipeline::asts::resolved::Query,
    )> {
        let RelationUse {
            name,
            definition_kind,
            query,
            bound,
        } = self;
        let resolved = resolve_relation_body(caller, &name, query, &bound, &crate::defuse::carriers::CarrierRecord::default())?;
        drop(bound);
        Ok((name, definition_kind, resolved))
    }
}

/// THE RELATION-USE TRANSITION: the admitted family's body opens and the
/// carrier pairs it with the SAME bound use — nothing outside this
/// authority pairs a query with an admission.
pub(in crate::defuse) fn relation_use(
    bound: BoundUse<NoActuals>,
    name: delightql_types::SqlIdentifier,
    definition_kind: crate::relation::form::DefinitionKind,
) -> Result<RelationUse> {
    let opened = open_relation_body(bound)?;
    Ok(RelationUse {
        name,
        definition_kind,
        query: opened.query,
        bound: opened.bound,
    })
}

/// Resolve a COMPILER-SYNTHESIZED body in a namespace's world (the
/// liminal wrapper): synthesizing a body is not using a definition, but
/// its world is built the same one way, and nothing of the caller's
/// lexical state reaches it.
pub(crate) fn resolve_synthesized_body(
    caller: &mut ResolverFold<'_, '_>,
    namespace: &str,
    view_name: &delightql_types::SqlIdentifier,
    query: ast_unresolved::Query,
) -> Result<crate::pipeline::asts::resolved::Query> {
    let declaration = DeclarationEnvironment::of_namespace(&caller.core.consult, namespace)?;
    resolve_synthesized_in(caller, view_name, query, declaration)
}

/// THE BODY OPENS IN ITS DECLARATION WORLD — derived from the OWNED
/// admission, whose borrow keeps the instance alive through resolution —
/// and resolves HERE: no caller receives an unresolved body beside a
/// world to apply.
fn resolve_relation_body<A: ActualPayload>(
    caller: &mut ResolverFold<'_, '_>,
    view_name: &delightql_types::SqlIdentifier,
    query: ast_unresolved::Query,
    bound: &BoundUse<A>,
    carriers: &crate::defuse::carriers::CarrierRecord,
) -> Result<crate::pipeline::asts::resolved::Query> {
    let facts = BodyPositionFacts::of(caller);
    let resolve_result = resolve_query_in(
        &mut *caller.core,
        facts,
        bound,
        carriers,
        super::environment::FormalBindings::default(),
        query,
    );
    Ok(dress_view_error(view_name, resolve_result)?.into_query())
}

/// Preserve validation errors (e.g., the B5 expansion-cycle refusal) so
/// their subcategory URI survives to the user and to error assertions.
fn dress_view_error(
    view_name: &delightql_types::SqlIdentifier,
    resolved: Result<crate::pipeline::resolver::ResolvedQuery>,
) -> Result<crate::pipeline::resolver::ResolvedQuery> {
    resolved.map_err(|e| {
        if matches!(e, DelightQLError::ValidationError { .. }) {
            return e;
        }
        DelightQLError::database_error(
            format!("Error while resolving borrowed view '{}': {}", view_name, e),
            e.to_string(),
        )
    })
}

/// A COMPILER-SYNTHESIZED body has NO admission to own — synthesizing is
/// not using a definition — so its world derives from the namespace's
/// published state directly. This is the one worldbuilding road with no
/// bound use, and it is private to the authority.
fn resolve_synthesized_in(
    caller: &mut ResolverFold<'_, '_>,
    view_name: &delightql_types::SqlIdentifier,
    query: ast_unresolved::Query,
    declaration: DeclarationEnvironment,
) -> Result<crate::pipeline::asts::resolved::Query> {
    let facts = BodyPositionFacts::of(caller);
    let mut env = body_env(
        declaration,
        &crate::defuse::carriers::CarrierRecord::default(),
        super::environment::FormalBindings::default(),
    );
    let resolve_result = {
        let mut fold = body_fold_in(&mut *caller.core, &mut env, facts);
        crate::pipeline::resolver::resolve_query_with(&mut fold, query)
    };
    Ok(dress_view_error(view_name, resolve_result)?.into_query())
}

/// One ADMITTED, opened ER-edge use: the rule's instance is held, its
/// body opened to a query, and the CONTEXT and PAIR that selected it
/// retained — an edge never stands apart from the terms it was selected
/// for. The body may be COMPOSED (the ER position's own law: self-aliases,
/// endpoint marks, chain merges), but RESOLUTION happens here, in the
/// rule's own world, while the admitted instance holds. The world decision
/// never leaves.
pub(crate) struct ErEdgeUse<'s> {
    bound: BoundUse<'s, NoActuals>,
    query: ast_unresolved::Query,
    context: String,
    left: super::er::ErTerm,
    right: super::er::ErTerm,
}

impl ErEdgeUse<'_> {
    /// Compose the OPENED body — ER-specific shaping over opened
    /// material. The query never stands beside its world in a caller's
    /// hands; it flows back in for [`Self::resolve`].
    fn compose(
        mut self,
        f: impl FnOnce(ast_unresolved::Query) -> Result<ast_unresolved::Query>,
    ) -> Result<Self> {
        self.query = f(self.query)?;
        Ok(self)
    }

    /// The STANDARD single-edge composition — the position's finite law
    /// (self-aliases; the outer endpoint mark), never a caller-supplied
    /// replacement of the opened body.
    pub(crate) fn compose_standard(self, outer_endpoint: Option<&str>) -> Result<Self> {
        self.compose(|query| {
            let mut query = crate::pipeline::resolver::add_self_aliases_to_query(query);
            if let Some(endpoint) = outer_endpoint {
                query.body =
                    crate::pipeline::resolver::mark_er_endpoint_outer(query.body, endpoint)?;
            }
            Ok(query)
        })
    }

    /// Resolve the (possibly composed) body in the declaration's own
    /// world. The admitted instance holds for the resolution's extent.
    pub(crate) fn resolve(
        self,
        fold: &mut ResolverFold<'_, '_>,
        wrap: impl FnOnce(DelightQLError) -> DelightQLError,
    ) -> Result<crate::pipeline::resolver::ResolvedQuery> {
        // THE SINGLE-EDGE OPERATION: this edge's own admitted body
        // resolves in its own declaration world — no chain, no
        // composition choice, nothing discarded.
        let ErEdgeUse { bound, query, .. } = self;
        let facts = BodyPositionFacts::of(fold);
        let outcome = resolve_query_in(
            &mut *fold.core,
            facts,
            &bound,
            &crate::defuse::carriers::CarrierRecord::default(),
            super::environment::FormalBindings::default(),
            query,
        )
        .map_err(wrap);
        drop(bound);
        outcome
    }
}

/// THE ER-EDGE TRANSITION: one admitted rule opens to its body, and the
/// pairing — the admission, the opened body, and the context and terms
/// that selected the rule — is this authority's own act.
pub(in crate::defuse) fn er_edge<'s>(
    bound: BoundUse<'s, NoActuals>,
    context: &str,
    left: super::er::ErTerm,
    right: super::er::ErTerm,
) -> Result<ErEdgeUse<'s>> {
    let opened = super::bound_use::use_er_rule(bound)?;
    Ok(ErEdgeUse {
        bound: opened.bound,
        query: opened.query,
        context: context.to_string(),
        left,
        right,
    })
}

/// A CODE actual's scoped closing: the frame of formal holes and the body
/// derive from ONE definition value; a truth body refuses before anything
/// resolves.
pub(in crate::defuse) fn scoped_curried(
    cfe: &crate::pipeline::asts::core::CfeDefinition,
) -> Result<(usize, ScopedBody)> {
    let formals = cfe.scalar_formals();
    let arity = formals.len();
    let mut inventory = FormalInventory::declared(
        formals
            .iter()
            .map(|formal| (formal.name.clone(), FormalRole::Value)),
    );
    inventory
        .bind_positional(FormalRole::Value, (0..arity as u32).map(curried_hole))
        .map_err(|error| named_binding_refusal(cfe.name.as_str(), error))?;
    let frame = inventory.sealed();
    let body = cfe.body.clone();
    Ok((arity, ScopedBody::of(frame, body)))
}

fn curried_hole(index: u32) -> crate::pipeline::asts::resolved::DomainExpression {
    crate::pipeline::asts::resolved::DomainExpression::Application(
        crate::pipeline::asts::resolved::FunctionApplication::Open(
            crate::pipeline::asts::core::FormalHole(index),
        ),
    )
}

impl super::bound_use::HoUse {
    /// The analyzed positions, for the caller-side spec judgments that run
    /// between admission and consumption.
    pub(in crate::defuse) fn positions(&self) -> &[crate::pipeline::asts::ddl::HoPositionInfo] {
        &self.positions
    }

    /// THE PARAMETERIZED CONSUMING OPERATION — affine: it takes the use BY
    /// VALUE, so an admitted body resolves exactly once, in exactly one
    /// world. It judges the call-site spec against the declared positions,
    /// shapes every clause with THIS use's actuals, opens a fresh body
    /// world holding the caller-resolved carriers as relation formals and
    /// the caller-resolved scalar actuals as the formal frame, resolves
    /// the squished body there, and returns the ONE finished expansion
    /// artifact with the resolved carriers standing ahead of the body's
    /// own bindings. No caller spelling is looked up again inside the
    /// body.
    pub(crate) fn resolve_squished(
        self,
        function: &str,
        scalar_spec: &ast_unresolved::Access,
        crossing_carriers: &[crate::relation::PortId],
        caller: &mut ResolverFold<'_, '_>,
    ) -> Result<SquishedExpansion> {
        log::debug!(
            "Expanding HO view '{}' (unified) from namespace '{}'",
            function,
            self.declaration.namespace(),
        );
        // The call-site spec is judged against the declared positions here.
        // Scalar supply was already resolved in the caller before admission;
        // a literal at a fully-ground position can therefore be rejected as
        // a provable miss without creating a publication path.
        super::ho::refuse_provable_ground_miss(function, scalar_spec, &self.positions)?;
        let join_input_scope = self.actuals.carriers.join_input();
        // A multi-clause (or badged) parameterized definition establishes a
        // fixpoint: mint THIS instance's frontier so a same-key
        // self-reference re-enters it by identity, exactly as the
        // unparameterized road does.
        let frontier = (self.group.clauses().len() > 1 || self.group.fixpoint().is_badged())
            .then(|| self.frame.frontier(&self.group));
        let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
        for clause_query in self.shaped_clause_queries(join_input_scope)? {
            super::ho::extract_clause_ctes(clause_query, function, frontier.as_ref(), &mut block)?;
        }
        // Main query: function(*) referencing the clause CTEs.
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
            },
            ast_unresolved::Access::All,
        );
        let squished = ast_unresolved::Query::binding(block.seal()?, main_query);
        // THE BODY WORLD: the declaration's own reach, the resolved
        // carriers as relation formals, the caller-resolved scalar actuals
        // as the frame — and nothing of the caller's lexical state.
        let declared: Vec<(delightql_types::SqlIdentifier, FormalRole)> = self
            .positions
            .iter()
            .filter(|position| {
                position.column_kind == crate::pipeline::asts::ddl::HoColumnKind::Scalar
            })
            .filter_map(|position| {
                position
                    .column_name
                    .as_ref()
                    .map(|name| (delightql_types::SqlIdentifier::new(name), FormalRole::Value))
            })
            .chain(self.group.params().iter().filter_map(|param| match param {
                HoParam::Rule { name, .. } => Some((name.clone(), FormalRole::Rule)),
                HoParam::Relation { .. } | HoParam::Scalar { .. } | HoParam::Ground { .. } => None,
            }))
            .collect();
        let HoUse {
            declaration,
            mut actuals,
            frame,
            ..
        } = self;
        let carriers = std::mem::take(&mut actuals.carriers);
        let leading_ctes: Vec<_> = carriers.leading_ctes().cloned().collect();
        let mut inventory = FormalInventory::declared(declared);
        for (name, value) in &actuals.values {
            inventory.bind_named(name, value.clone())?;
        }
        for (name, value) in &actuals.rules {
            inventory.bind_rule_named(name, *value)?;
        }
        let formals = inventory.sealed();
        // THE OCCURRENCE EDGE IS CONSTRUCTION'S OWN RECORD: a caller-resolved
        // scalar actual is a port of a carrier the body reads BY IDENTITY,
        // and every carry of that port records the value class it carries.
        // The body spends a formal against the exact position of its own
        // heading that carries the actual's value class; nothing here
        // registers, scans, or reconstructs.
        let spent = std::mem::take(&mut actuals.values);
        let facts = BodyPositionFacts::of(caller);
        let resolved = {
            let mut env = body_env(declaration, &carriers, formals);
            let mut fold = body_fold_in(&mut *caller.core, &mut env, facts);
            fold.crossing_carriers = crossing_carriers.to_vec();
            crate::pipeline::resolver::resolve_query_with(&mut fold, squished)?
        };
        drop(frame);
        Ok(SquishedExpansion {
            resolved: resolved.with_leading_ctes(leading_ctes),
            actuals: spent,
        })
    }

    /// The parameterized body, one shaped query per clause, each bound
    /// and injected with THIS use's actuals — module-private: only the
    /// consuming operation above pairs them with a world.
    fn shaped_clause_queries(
        &self,
        join_input_scope: Option<crate::relation::StructuralRelation>,
    ) -> Result<Vec<ast_unresolved::Query>> {
        let actuals = &self.actuals;
        let bindings = &actuals.bindings;
        let defs = self.group.clauses();
        let mut shaped = Vec::with_capacity(defs.len());
        for def in defs {
            let q = if defs.len() == 1 {
                // Single clause: the whole stored definition is the clause.
                crate::ddl::reconstruct::bound_body(&self.definition, bindings.clone())?
            } else {
                crate::ddl::reconstruct::bound_body(&def.full_source, bindings.clone())?
            };
            shaped.push(shape_bound_clause(
                q,
                def,
                &self.positions,
                actuals,
                join_input_scope,
            ));
        }
        Ok(shaped)
    }
}

/// ONE BOUND CLAUSE, SHAPED: the caller input injected (before the scalar
/// columns, so the embed pipe wraps the join and not vice versa — an
/// anonymous table with column references stays on the right of a join
/// where the melt strategy handles it), then the scalar positions bound
/// and the head projected. The same act for a consulted clause and a
/// CHOE's; only how the bound query was obtained differs.
fn shape_bound_clause(
    q: ast_unresolved::Query,
    def: &crate::pipeline::asts::ddl::Clause,
    positions: &[crate::pipeline::asts::ddl::HoPositionInfo],
    actuals: &HoActuals,
    join_input_scope: Option<crate::relation::StructuralRelation>,
) -> ast_unresolved::Query {
    let clause_params = def.params().to_vec();
    let output_head = def.head_items();
    let q = if let Some(input_scope) = join_input_scope {
        // Every clause participates in row-by-row dispatch. A ground-only
        // clause still needs the caller row so its discriminator can be
        // compared without joining the carrier a second time above the
        // union.
        let condition =
            super::ho::ground_scalar_correlation_condition(&clause_params, positions, actuals);
        super::ho::inject_input_table_into_query(q, input_scope, condition)
    } else {
        q
    };
    super::ho::inject_scalar_columns(
        q,
        &clause_params,
        positions,
        output_head,
        actuals,
        join_input_scope.is_some(),
    )
}

/// The refusal of a common higher-order expression whose body reaches
/// itself: a query-scoped parameterized rule has no fixpoint to re-enter.
pub(in crate::defuse) fn choe_recursion_refusal(name: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::RESOLUTION_CHOE_RECURSION,
        format!(
            "the common higher-order expression '{name}' reaches itself: a query-scoped \
             parameterized rule has no fixpoint to re-enter"
        ),
        "write the recursion as a consulted rule, or bind the recursive relation \
         with a `%`-badged common table expression",
    )
}

/// ONE ADMITTED COMMON HIGHER-ORDER EXPRESSION USE — the CHOE twin of
/// [`HoUse`]: the definition the query declared, the caller-resolved
/// actuals, the analyzed positions, and the held query-local instance,
/// paired by construction and spent whole by [`Self::resolve_squished`].
pub(in crate::defuse) struct ScopedHoUse {
    definition: crate::pipeline::asts::core::HoDefinition,
    actuals: HoActuals,
    positions: Vec<crate::pipeline::asts::ddl::HoPositionInfo>,
    _frame: super::instance::InstanceFrame,
}

/// Use a CHOE in parameterized relation position: the instance is admitted
/// under the binding's query-local identity BEFORE the body opens, and a
/// self-reference — reached while the body resolves — is the v1 refusal:
/// there is no fixpoint to re-enter and no parameter to widen.
pub(in crate::defuse) fn use_scoped_ho(
    instances: &InstanceTable,
    definition: crate::pipeline::asts::core::HoDefinition,
    call_spelling: &str,
    actuals: HoActuals,
) -> Result<ScopedHoUse> {
    let key = actuals.scoped_key();
    match instances.admit_scoped(definition.name(), key) {
        super::instance::ScopedAdmission::Fresh(frame) => {
            let positions = super::ho::scoped_positions(&definition);
            Ok(ScopedHoUse {
                definition,
                actuals,
                positions,
                _frame: frame,
            })
        }
        super::instance::ScopedAdmission::Reenter
        | super::instance::ScopedAdmission::Cycle { .. }
        | super::instance::ScopedAdmission::Widening { .. } => {
            Err(choe_recursion_refusal(call_spelling))
        }
    }
}

impl ScopedHoUse {
    pub(in crate::defuse) fn positions(&self) -> &[crate::pipeline::asts::ddl::HoPositionInfo] {
        &self.positions
    }

    /// THE CHOE CONSUMING OPERATION — affine, like the consulted one. It
    /// judges the call-site spec, shapes every clause with THIS use's
    /// actuals — each clause body is the query's own authored text,
    /// normalized now with the bindings in hand — and resolves the squished
    /// body IN THE QUERY'S OWN WORLD: through a body frame that holds the
    /// caller-resolved carriers and the formal frame, answers the world's
    /// bindings only up to the definition's lexical horizon, and keeps the
    /// body's own registrations for exactly this resolution. Nothing of the
    /// caller's ROW crosses; nothing of the body's stays.
    pub(in crate::defuse) fn resolve_squished(
        self,
        function: &str,
        scalar_spec: &ast_unresolved::Access,
        scoped_world: Option<super::environment::ClosedLexicalWorld>,
        crossing_carriers: &[crate::relation::PortId],
        caller: &mut ResolverFold<'_, '_>,
    ) -> Result<SquishedExpansion> {
        super::ho::refuse_provable_ground_miss(function, scalar_spec, &self.positions)?;
        let join_input_scope = self.actuals.carriers.join_input();
        let group = self.definition.group();
        // A multi-clause CHOE binds its clauses under one frontier so they
        // accumulate as one definition; a self-reference never reaches the
        // frontier — admission refused it before the body opened.
        let frontier = (group.clauses().len() > 1).then(|| self._frame.frontier(group));
        let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
        for def in group.clauses() {
            let crate::pipeline::asts::ddl::DdlBody::Deferred { source } = &def.body else {
                return Err(DelightQLError::transformation_error(
                    "a common higher-order expression holds its body as authored text; \
                     a clause built any other way cannot be bound",
                    "choe",
                ));
            };
            let q = crate::ddl::reconstruct::bound_relex(source, self.actuals.bindings.clone())?;
            let q = shape_bound_clause(q, def, &self.positions, &self.actuals, join_input_scope);
            super::ho::extract_clause_ctes(q, function, frontier.as_ref(), &mut block)?;
        }
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
            },
            ast_unresolved::Access::All,
        );
        let squished = ast_unresolved::Query::binding(block.seal()?, main_query);
        let declared: Vec<(delightql_types::SqlIdentifier, FormalRole)> = self
            .positions
            .iter()
            .filter(|position| {
                position.column_kind == crate::pipeline::asts::ddl::HoColumnKind::Scalar
            })
            .filter_map(|position| {
                position
                    .column_name
                    .as_ref()
                    .map(|name| (delightql_types::SqlIdentifier::new(name), FormalRole::Value))
            })
            .chain(
                self.definition
                    .group()
                    .params()
                    .iter()
                    .filter_map(|param| match param {
                        HoParam::Rule { name, .. } => Some((name.clone(), FormalRole::Rule)),
                        HoParam::Relation { .. }
                        | HoParam::Scalar { .. }
                        | HoParam::Ground { .. } => None,
                    }),
            )
            .collect();
        let ScopedHoUse {
            definition,
            mut actuals,
            ..
        } = self;
        let carriers = std::mem::take(&mut actuals.carriers);
        let leading_ctes: Vec<_> = carriers.leading_ctes().cloned().collect();
        let mut inventory = FormalInventory::declared(declared);
        for (name, value) in &actuals.values {
            inventory.bind_named(name, value.clone())?;
        }
        for (name, value) in &actuals.rules {
            inventory.bind_rule_named(name, *value)?;
        }
        let formals = inventory.sealed();
        let spent = std::mem::take(&mut actuals.values);
        let facts = BodyPositionFacts::authored_of(caller);
        let resolved = match scoped_world {
            Some(world) => {
                let mut world = world.open();
                let mut lease =
                    world.opened_body(formals, &carriers, definition.horizon().clone());
                let mut fold = body_fold_in(&mut *caller.core, lease.world(), facts);
                fold.crossing_carriers = crossing_carriers.to_vec();
                crate::pipeline::resolver::resolve_query_with(&mut fold, squished)?
            }
            None => {
                let mut lease =
                    caller
                        .env
                        .opened_body(formals, &carriers, definition.horizon().clone());
                let mut fold = body_fold_in(&mut *caller.core, lease.world(), facts);
                fold.crossing_carriers = crossing_carriers.to_vec();
                crate::pipeline::resolver::resolve_query_with(&mut fold, squished)?
            }
        };
        Ok(SquishedExpansion {
            resolved: resolved.with_leading_ctes(leading_ctes),
            actuals: spent,
        })
    }
}

/// Admit only a FRESH binding: a cycle refuses with its chain, and a
/// re-entry or widening refuses with the position's own teaching — the
/// positions that lawfully re-enter (the HO fixpoint) match the admission
/// themselves instead of calling this.
pub(in crate::defuse) fn require_fresh<A: ActualPayload>(
    admission: BoundAdmission<A>,
    reenter: impl FnOnce() -> DelightQLError,
) -> Result<BoundUse<A>> {
    match admission {
        BoundAdmission::Fresh(bound) => Ok(bound),
        BoundAdmission::Cycle { chain } => Err(mutual_recursion_refusal(chain)),
        BoundAdmission::Reenter | BoundAdmission::Widening { .. } => Err(reenter()),
    }
}

/// The declared-mode recursion refusal: a value definition cannot recurse.

pub(in crate::defuse) struct HoUse {
    declaration: DeclarationEnvironment,
    actuals: HoActuals,
    frame: super::instance::InstanceFrame,
    definition: String,
    group: crate::pipeline::asts::ddl::DefinitionGroup,
    positions: Vec<crate::pipeline::asts::ddl::HoPositionInfo>,
}

/// The owned consulted-family half of a closed residual. Selection has
/// already happened: source, declaration reach, positions, and durable
/// instance identity travel together without retaining a borrow of the
/// resolver core that performed construction.
#[derive(Debug, Clone)]
pub(in crate::defuse) struct ClosedHoFamily {
    name: delightql_types::SqlIdentifier,
    identity: super::instance::ClosedFamilyIdentity,
    declaration: DeclarationEnvironment,
    definition: String,
    group: crate::pipeline::asts::ddl::DefinitionGroup,
    declared: Vec<HoParam>,
    positions: Vec<crate::pipeline::asts::ddl::HoPositionInfo>,
}

impl ClosedHoFamily {
    pub(in crate::defuse) fn close(family: LinkedFamily<'_>) -> Result<Self> {
        let identity = super::instance::ClosedFamilyIdentity::of(&family);
        let name = family.name().clone();
        let declaration = DeclarationEnvironment::of_family(&family)?;
        let definition = family.definition().to_string();
        let group = crate::ddl::reconstruct::group(&definition)?;
        let positions = crate::pipeline::resolver::grounding::build_ho_position_analysis(&group);
        let positions = super::ho::ensure_position_column_names(positions, group.clauses());
        let declared = closed_ho_signature(&group, &positions)?;
        Ok(ClosedHoFamily {
            name,
            identity,
            declaration,
            definition,
            group,
            declared,
            positions,
        })
    }

    pub(in crate::defuse) fn name(&self) -> &delightql_types::SqlIdentifier {
        &self.name
    }

    pub(in crate::defuse) fn params(&self) -> &[HoParam] {
        &self.declared
    }

    pub(in crate::defuse) fn positions(&self) -> &[crate::pipeline::asts::ddl::HoPositionInfo] {
        &self.positions
    }

    pub(in crate::defuse) fn output(&self) -> &crate::pipeline::asts::core::definitions::HeadItems {
        &self.group.first().head.items
    }
}

/// The family's call row is the exhaustive cross-clause position judgment,
/// not one clause's pattern row. Ground patterns and free binders may occupy
/// the same scalar position, so taking `group.params()` would make whichever
/// clause happens to stand first impersonate the declaration. This projection
/// turns each already-analyzed position into its one callable formal; clause
/// patterns remain in the group and are applied only after admission.
fn closed_ho_signature(
    group: &crate::pipeline::asts::ddl::DefinitionGroup,
    positions: &[crate::pipeline::asts::ddl::HoPositionInfo],
) -> Result<Vec<HoParam>> {
    use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundPattern};

    let source_at = |position: usize, accepts: fn(&HoParam) -> bool| {
        group
            .clauses()
            .iter()
            .filter_map(|clause| clause.params().get(position))
            .find(|param| accepts(param))
    };
    let missing = |position: usize| {
        DelightQLError::database_error(
            format!(
                "the reconstructed higher-order family has no source parameter for analyzed position {position}"
            ),
            "catalog definition and position analysis disagree",
        )
    };

    positions
        .iter()
        .map(|position| {
            let name = position
                .column_name
                .as_ref()
                .map(|name| delightql_types::SqlIdentifier::new(name))
                .ok_or_else(|| missing(position.position))?;
            match (&position.column_kind, &position.ground_pattern) {
                (HoColumnKind::TableGlob, _) => Ok(HoParam::Relation {
                    name,
                    cols: crate::pipeline::asts::ddl::HeadItems::Glob,
                }),
                (HoColumnKind::TableArgumentative(_), _) => {
                    let HoParam::Relation { cols, .. } = source_at(position.position, |param| {
                        matches!(
                            param,
                            HoParam::Relation {
                                cols: crate::pipeline::asts::ddl::HeadItems::Listed(_),
                                ..
                            }
                        )
                    })
                    .ok_or_else(|| missing(position.position))?
                    else {
                        unreachable!("the source predicate admits only listed relations")
                    };
                    Ok(HoParam::Relation {
                        name,
                        cols: cols.clone(),
                    })
                }
                (HoColumnKind::Rule(signature), _) => Ok(HoParam::Rule {
                    name,
                    signature: signature.clone(),
                }),
                (HoColumnKind::Scalar, Some(HoGroundPattern::AllClauses)) => {
                    let HoParam::Ground { text, .. } = source_at(position.position, |param| {
                        matches!(param, HoParam::Ground { .. })
                    })
                    .ok_or_else(|| missing(position.position))?
                    else {
                        unreachable!("the source predicate admits only ground parameters")
                    };
                    Ok(HoParam::Ground {
                        name,
                        text: text.clone(),
                    })
                }
                (HoColumnKind::Scalar, None | Some(HoGroundPattern::SomeClauses)) => {
                    let HoParam::Scalar { callable, .. } = source_at(position.position, |param| {
                        matches!(param, HoParam::Scalar { .. })
                    })
                    .ok_or_else(|| missing(position.position))?
                    else {
                        unreachable!("the source predicate admits only scalar parameters")
                    };
                    Ok(HoParam::Scalar {
                        name,
                        guard: None,
                        callable: *callable,
                    })
                }
            }
        })
        .collect()
}

pub(in crate::defuse) fn use_closed_ho(
    instances: &InstanceTable,
    family: ClosedHoFamily,
    call_spelling: &str,
    actuals: HoActuals,
    absorbs_input: bool,
) -> Result<super::bound_use::HoUseOutcome> {
    let key = actuals.closed_semantic_key(family.params());
    let frame = match instances.admit_closed(&family.identity, key) {
        super::instance::Admitted::Fresh(frame) => frame,
        super::instance::Admitted::Reenter { frontier } => {
            if absorbs_input {
                return Err(DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::RECURSION_CONSULTED_CLAUSE_ORDER,
                    format!(
                        "the self-reference of '{call_spelling}' re-enters its own fixpoint and cannot also absorb an input"
                    ),
                    "recursive frontier read",
                ));
            }
            return Ok(super::bound_use::HoUseOutcome::Reenter { frontier });
        }
        super::instance::Admitted::Cycle { chain } => {
            return Err(mutual_recursion_refusal(chain))
        }
        super::instance::Admitted::Widening {
            building,
            requested,
        } => {
            return Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::RECURSION_PARAMETER_WIDENING,
                format!(
                    "'{call_spelling}' is recursive and changes a parameter actual (building [{}], requested [{}])",
                    building.join(", "),
                    requested.join(", ")
                ),
                "recursive parameters never widen",
            ))
        }
    };
    Ok(super::bound_use::HoUseOutcome::Open(HoUse {
        declaration: family.declaration,
        actuals,
        frame,
        definition: family.definition,
        group: family.group,
        positions: family.positions,
    }))
}

/// Use a selected family in PARAMETERIZED/HO RELATION position. Owns the
/// complete sequence: the caller-resolved actuals serialize to the
/// semantic key, the instance table judges admission BEFORE the body
/// opens (same key re-enters the fixpoint; a changed key is the ruled
/// terminal `semantic/recursion/parameter-widening` refusal and never
/// begins another expansion), then the body opens once with its analyzed
/// positions in the family's own world.

/// Open an ADMITTED ER rule's body as one relational query. The head
/// declares a CLOSED schema; each clause carries the projection its own
/// head declares — no text is regenerated and nothing is re-parsed. A
/// BADGED TARGET ALWAYS TAKES THE CTE ROAD: the badge is judged where
/// the self-reference binds, and only a binding has that decision.
pub(in crate::defuse) fn use_er_rule(bound: BoundUse<NoActuals>) -> Result<OpenedRelationBody> {
    bound.open_relation()?.into_relation_body()
}

/// Shape an admitted family into the one callable form — module-private:
/// only the consuming operations below reach it, paired with the same
/// use's world and actuals.
pub(in crate::defuse) fn shape_callable<A: ActualPayload>(
    bound: &BoundUse<A>,
) -> Result<crate::pipeline::asts::unresolved::CfeDefinition> {
    use crate::pipeline::asts::core::CfeFormals;
    use crate::pipeline::asts::ddl::{DefKind, HoParam};

    let family = &bound.family;
    let group = bound.reconstruct_group().map_err(|e| {
        DelightQLError::parse_error(format!(
            "No definition found for function '{}': {e}",
            family.name()
        ))
    })?;

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
        CfeFormals::from_role_groups([], family.params().iter().map(|p| p.name().clone()))
    };
    let context_mode = group.context().clone();
    let mut clauses = group.into_clauses();

    let body = if clauses.len() == 1 {
        let clause = clauses.pop().expect("length checked above");
        clause.into_scalar_body().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Expected scalar body for function '{}', got relational",
                family.name()
            ))
        })?
    } else {
        // Multi-clause: synthesize CASE expression with parameter Lvars
        // intact.
        crate::pipeline::resolver::grounding::build_case_body_from_clauses(family.name(), clauses)?
    };

    Ok(crate::pipeline::asts::unresolved::CfeDefinition::unbounded(
        family.name().clone(),
        formals,
        context_mode,
        body,
    ))
}

/// One bound SCALAR-CALL use — the family road or its scoped twin, spent
/// whole by [`Self::apply_call`].

/// THE CALLER-RESOLVED ACTUALS of one definition use, typed by position.
/// Construction is the caller's proof of the ratified order: values are
/// RESOLVED expressions (they resolved in the caller's world before any
/// admission), code members are CLOSED callable actuals, and higher-order
/// carriers are caller-resolved relations under opaque identities. The
/// semantic key is derived HERE, from the same object admission stores —
/// never separately supplied.

/// THE OPERATION KIND of a definition use, as a TYPE: what a position
/// supplies is what the transition binds, and no road can recover a
/// different variant than the one it was constructed with. Fields are
/// private — an actual value enters the carrier and is spent by the
/// consuming operation; nothing reads it back out.
pub(in crate::defuse) trait ActualPayload: sealed::Sealed {
    /// The semantic instance key: one canonical structural serialization
    /// per actual, in supplied/declared order.
    fn semantic_key(&self, family: &LinkedFamily) -> Vec<String>;
    /// The scoped-binding key: family-independent serialization (a scoped
    /// binding has no declared-parameter order to consult).
    fn scoped_key(&self) -> Vec<String>;
}

/// An unparameterized use (relation, ER edge, declared mode,
/// runtime-served splice).
pub(in crate::defuse) struct NoActuals;

/// Ordered value actuals, resolved in the caller (sigma, effect, cover
/// cell, pattern slot).
#[derive(Default)]
pub(in crate::defuse) struct ValueActuals(
    pub(in crate::defuse) Vec<crate::pipeline::asts::resolved::DomainExpression>,
);

impl ValueActuals {
    pub(in crate::defuse) fn of(
        values: Vec<crate::pipeline::asts::resolved::DomainExpression>,
    ) -> Self {
        ValueActuals(values)
    }
}

/// An effect invocation's caller-resolved scalar values and closed pure rule
/// values. One payload keys admission and later builds the formal frame, so
/// neither half can be substituted after the effect family is selected.
pub(in crate::defuse) struct EffectActuals {
    values: Vec<crate::pipeline::asts::resolved::DomainExpression>,
    rules: std::collections::HashMap<delightql_types::SqlIdentifier, super::ho::RuleValueId>,
}

impl EffectActuals {
    fn of(
        values: Vec<crate::pipeline::asts::resolved::DomainExpression>,
        rules: std::collections::HashMap<delightql_types::SqlIdentifier, super::ho::RuleValueId>,
    ) -> Self {
        EffectActuals { values, rules }
    }
}

/// A scalar-callable use: the context marker, the caller-CLOSED code
/// actuals, and the resolved value actuals, in supplied order.
#[derive(Default)]
pub(in crate::defuse) struct ScalarActuals {
    pub(in crate::defuse) context: bool,
    pub(in crate::defuse) code: Vec<super::callable::CallableBinding>,
    pub(in crate::defuse) values: Vec<crate::pipeline::asts::resolved::DomainExpression>,
}

impl ScalarActuals {
    pub(in crate::defuse) fn of(
        context: bool,
        code: Vec<super::callable::CallableBinding>,
        values: Vec<crate::pipeline::asts::resolved::DomainExpression>,
    ) -> Self {
        ScalarActuals {
            context,
            code,
            values,
        }
    }
}

/// A parameterized/HO use: the structural binding facts the normalizer
/// consumes, the CALLER-RESOLVED scalar actuals as the body's formal
/// frame, and the authored-syntax facts the discriminator judgments read.
#[derive(Clone)]
pub(in crate::defuse) struct HoActuals {
    /// THE CALLER'S RESOLVED RELATION CARRIERS — resolved in the caller's
    /// world BEFORE admission and owned by the admitted use, so a carrier
    /// can never stand beside another use.
    pub(in crate::defuse) carriers: crate::defuse::carriers::CarrierRecord,
    /// The structural facts (carrier scopes, patterns, literal fills) the
    /// clause normalizer consumes. Carries NO caller expression: scalar
    /// formals ride as a name set and are answered by `frame`.
    pub(in crate::defuse) bindings: crate::pipeline::query_features::HoParamBindings,
    /// param -> the value the CALLER resolved for it.
    pub(in crate::defuse) values: std::collections::HashMap<
        delightql_types::SqlIdentifier,
        crate::pipeline::asts::resolved::DomainExpression,
    >,
    /// param -> the bare spelling the caller wrote, where the actual was a
    /// bare name: a SYNTAX fact for the dispatch-witness and shadowing
    /// judgments — never a lookup key. The clause-dispatch splice reaches the
    /// body by IDENTITY: the actual is a port of a carrier the body reads,
    /// and the body's heading carries its value class by construction.
    /// Spelling, ordinal, carry depth, and resemblance reconstruct nothing.
    pub(in crate::defuse) authored_bare: std::collections::HashMap<String, String>,
    /// Rule-valued formals receive only opaque closed-value identities.
    pub(in crate::defuse) rules:
        std::collections::HashMap<delightql_types::SqlIdentifier, super::ho::RuleValueId>,
}

impl sealed::Sealed for NoActuals {}
impl sealed::Sealed for ValueActuals {}
impl sealed::Sealed for EffectActuals {}
impl sealed::Sealed for ScalarActuals {}
impl sealed::Sealed for HoActuals {}

impl HoActuals {
    fn closed_semantic_key(&self, params: &[HoParam]) -> Vec<String> {
        use crate::lispy::ToLispy;
        params
            .iter()
            .map(|param| {
                let name = param.name();
                if let Some(value) = self.values.get(name) {
                    return format!("value:{}", value.to_lispy());
                }
                if let Some(rule) = self.rules.get(name) {
                    return format!("rule:{}", rule.0);
                }
                let spelling = name.as_str();
                if let Some(scope) = self.bindings.table_scope_params.get(spelling) {
                    return format!("relation:{scope:?}");
                }
                if let Some((carrier, scope)) = &self.bindings.pipe_carrier {
                    if carrier == spelling {
                        return format!("relation:{scope:?}");
                    }
                }
                if let Some(ground) = self.bindings.scalar_literals.get(spelling) {
                    return format!("ground:{}", ground.to_lispy());
                }
                "unbound".to_string()
            })
            .collect()
    }
}

impl ActualPayload for NoActuals {
    fn semantic_key(&self, _family: &LinkedFamily) -> Vec<String> {
        Vec::new()
    }
    fn scoped_key(&self) -> Vec<String> {
        Vec::new()
    }
}

impl ActualPayload for ValueActuals {
    fn semantic_key(&self, _family: &LinkedFamily) -> Vec<String> {
        self.scoped_key()
    }
    fn scoped_key(&self) -> Vec<String> {
        use crate::lispy::ToLispy;
        self.0
            .iter()
            .map(|value| format!("value:{}", value.to_lispy()))
            .collect()
    }
}

impl ActualPayload for EffectActuals {
    fn semantic_key(&self, _family: &LinkedFamily) -> Vec<String> {
        self.scoped_key()
    }

    fn scoped_key(&self) -> Vec<String> {
        use crate::lispy::ToLispy;
        let mut key: Vec<String> = self
            .values
            .iter()
            .map(|value| format!("value:{}", value.to_lispy()))
            .collect();
        let mut rules: Vec<_> = self.rules.iter().collect();
        rules.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
        key.extend(
            rules
                .into_iter()
                .map(|(name, value)| format!("rule:{name}:{}", value.0)),
        );
        key
    }
}

impl ActualPayload for ScalarActuals {
    fn semantic_key(&self, _family: &LinkedFamily) -> Vec<String> {
        self.scoped_key()
    }
    fn scoped_key(&self) -> Vec<String> {
        use crate::lispy::ToLispy;
        let mut key = Vec::new();
        if self.context {
            key.push("ctx".to_string());
        }
        for binding in &self.code {
            key.push(binding.semantic_key());
        }
        for value in &self.values {
            key.push(format!("value:{}", value.to_lispy()));
        }
        key
    }
}

impl ActualPayload for HoActuals {
    fn semantic_key(&self, family: &LinkedFamily) -> Vec<String> {
        use crate::lispy::ToLispy;
        family
            .params()
            .iter()
            .map(|param| {
                let name = param.name();
                if let Some(value) = self.values.get(&name) {
                    return format!("value:{}", value.to_lispy());
                }
                if let Some(rule) = self.rules.get(&name) {
                    return format!("rule:{}", rule.0);
                }
                let name = name.as_str();
                if let Some(scope) = self.bindings.table_scope_params.get(name) {
                    return format!("relation:{scope:?}");
                }
                if let Some((carrier, scope)) = &self.bindings.pipe_carrier {
                    if carrier == name {
                        return format!("relation:{scope:?}");
                    }
                }
                if let Some(ground) = self.bindings.scalar_literals.get(name) {
                    return format!("ground:{}", ground.to_lispy());
                }
                "unbound".to_string()
            })
            .collect()
    }
    fn scoped_key(&self) -> Vec<String> {
        Vec::new()
    }
}

/// ONE BOUND DEFINITION USE — the complete carrier: the selected family,
/// the declaration environment DERIVED FROM that family (its declaring
/// namespace's reach, captured), the typed caller actuals,
/// and the held instance frame, paired by construction. THERE IS NO
/// PROJECTION SURFACE: no road can take the family, the environment, the
/// actuals, or the raw body out of this value — a bound use is spent
/// whole, by the one consuming operation its actual kind selects, and
/// that operation owns body opening, world construction, actual binding,
/// and the result.
pub(in crate::defuse) struct BoundUse<'s, A: ActualPayload> {
    family: LinkedFamily<'s>,
    declaration: DeclarationEnvironment,
    actuals: A,
    _frame: super::instance::InstanceFrame,
}

/// The closed outcome of BINDING a definition use.
pub(in crate::defuse) enum BoundAdmission<'s, A: ActualPayload> {
    Fresh(BoundUse<'s, A>),
    /// An open instance with the SAME actual key: the position's policy
    /// interprets it (a fixpoint re-enters through the frontier it minted;
    /// a valueless position refuses).
    Reenter,
    /// Re-entry through at least one other open family.
    Cycle {
        chain: Vec<String>,
    },
    /// The ruled terminal: an open instance with a DIFFERENT actual key.
    Widening {
        building: Vec<String>,
        requested: Vec<String>,
    },
}

/// THE ONE TRANSITION, construction-owned:
///
/// ```text
/// selected position
///   + declaration environment derived from that family
///   + typed actuals resolved in the caller
///         -> admitted bound use (family + world + semantic key + frame)
///         -> spent WHOLE by its kind's consuming operation
///         -> position artifact
/// ```
pub(in crate::defuse) fn bind_definition_use<'s, A: ActualPayload>(
    instances: &InstanceTable,
    family: LinkedFamily<'s>,
    actuals: A,
) -> Result<BoundAdmission<'s, A>> {
    // THE DECLARATION ENVIRONMENT DERIVES FROM THE FAMILY'S OWN READ: the
    // reach is captured under the read that selected the family, so no
    // caller can pair a family with a registry of its own.
    let declaration = DeclarationEnvironment::of_family(&family)?;
    let key = actuals.semantic_key(&family);
    Ok(match instances.admit_identity(&family, key) {
        super::instance::Admitted::Fresh(frame) => BoundAdmission::Fresh(BoundUse {
            family,
            declaration,
            actuals,
            _frame: frame,
        }),
        super::instance::Admitted::Reenter { .. } => BoundAdmission::Reenter,
        super::instance::Admitted::Cycle { chain } => BoundAdmission::Cycle { chain },
        super::instance::Admitted::Widening {
            building,
            requested,
        } => BoundAdmission::Widening {
            building,
            requested,
        },
    })
}

impl<A: ActualPayload> BoundUse<'_, A> {
    /// Open the bound body — module-private: only the consuming
    /// operations in this file reconstruct, and each pairs the opened
    /// group with THIS use's world and actuals.
    pub(in crate::defuse) fn reconstruct_group(
        &self,
    ) -> Result<crate::pipeline::asts::ddl::DefinitionGroup> {
        crate::ddl::reconstruct::group(self.family.definition())
    }
}

/// One opened relational definition. The stored body and the admitted use
/// that licenses it are captured in the same private construction act. Only
/// its consuming expansion may mint a recursive frontier, and that frontier
/// is installed solely on CTEs built from this carrier's own clauses.
struct OpenedRelationDefinition<'s> {
    bound: BoundUse<'s, NoActuals>,
    group: crate::pipeline::asts::ddl::DefinitionGroup,
}

/// The expansion result retains the admitted instance while its query is
/// resolved, but no longer exposes either clauses or frontier evidence.
pub(in crate::defuse) struct OpenedRelationBody<'s> {
    pub(in crate::defuse) bound: BoundUse<'s, NoActuals>,
    pub(in crate::defuse) query: ast_unresolved::Query,
}

impl<'s> BoundUse<'s, NoActuals> {
    fn open_relation(self) -> Result<OpenedRelationDefinition<'s>> {
        let group = crate::ddl::reconstruct::group(self.family.definition())?;
        Ok(OpenedRelationDefinition { bound: self, group })
    }
}

impl<'s> OpenedRelationDefinition<'s> {
    fn requires_frontier(&self) -> bool {
        self.group.clauses().len() > 1 || self.group.fixpoint().is_badged()
    }

    fn into_relation_body(self) -> Result<OpenedRelationBody<'s>> {
        let OpenedRelationDefinition { bound, group } = self;
        let fixpoint = group.fixpoint();
        let frontier = (group.clauses().len() > 1 || fixpoint.is_badged())
            .then(|| bound._frame.frontier(&group));
        let mut clauses = group.spend_heads()?;

        if clauses.len() <= 1 && !fixpoint.is_badged() {
            let clause = clauses.pop().ok_or_else(|| {
                DelightQLError::parse_error(format!(
                    "No definition found for view '{}'",
                    bound.family.name()
                ))
            })?;
            let query = clause.into_query().ok_or_else(|| {
                DelightQLError::parse_error(format!(
                    "Expected relational body for view '{}', got scalar",
                    bound.family.name()
                ))
            })?;
            return Ok(OpenedRelationBody { bound, query });
        }

        // A badged target always takes the CTE road: the badge is judged
        // where the self-reference binds, and only a binding has that
        // decision. The frontier is minted only after this carrier owns the
        // exact clauses it is about to label.
        let frontier = frontier.expect("multi-clause or badged relation has frontier");
        let view_name = frontier.name().as_str().to_string();
        let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
        for clause in clauses {
            let query = clause.into_query().ok_or_else(|| {
                DelightQLError::parse_error(
                    "Expected relational body for disjunctive view clause, got scalar",
                )
            })?;
            if !query.cfes().is_empty() || !query.hos().is_empty() {
                return Err(DelightQLError::parse_error(
                    "Unsupported query form in disjunctive view clause: a query-scoped \
                     function definition"
                        .to_string(),
                ));
            }
            let ast_unresolved::Query { locals, body } = query;
            // The clause's own block is ABSORBED, never rebuilt: a clause
            // CTE's authored name is claimed where the clause claimed it,
            // so a reference to it inside the clause is a query-local hit
            // rather than a fall-through to the catalog.
            block.absorb(locals)?;
            block.admit_relation(ast_unresolved::CteBinding::frontier(
                super::FrontierCte::new(
                    body,
                    frontier.clone(),
                    crate::pipeline::asts::core::CteAuthority {
                        horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
                        head: crate::pipeline::asts::core::definitions::Head::glob(),
                        origin:
                            crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                        fixpoint,
                    },
                ),
            ))?;
        }

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
            },
            ast_unresolved::Access::All,
        );
        Ok(OpenedRelationBody {
            bound,
            query: ast_unresolved::Query::binding(block.seal()?, main_query),
        })
    }
}

/// One bound QUERY-SCOPED use: the inline binding's shaped definition,
/// the typed caller actuals, and the held instance — the scoped twin of
/// [`BoundUse`] (a scoped binding has no stored bytes to open and no
/// publication; it resolves in the world it was born in). The same
/// no-projection law holds.
pub(in crate::defuse) struct ScopedBoundUse<A: ActualPayload> {
    pub(in crate::defuse) cfe: crate::pipeline::asts::unresolved::CfeDefinition,
    pub(in crate::defuse) actuals: A,
    pub(in crate::defuse) _frame: super::instance::InstanceFrame,
}

pub(in crate::defuse) enum ScopedBoundAdmission<A: ActualPayload> {
    Fresh(ScopedBoundUse<A>),
    Reenter,
    Cycle {
        chain: Vec<String>,
    },
    Widening {
        building: Vec<String>,
        requested: Vec<String>,
    },
}

pub(in crate::defuse) fn bind_scoped_use<A: ActualPayload>(
    instances: &InstanceTable,
    name: &delightql_types::SqlIdentifier,
    cfe: crate::pipeline::asts::unresolved::CfeDefinition,
    actuals: A,
) -> ScopedBoundAdmission<A> {
    let key = actuals.scoped_key();
    match instances.admit_scoped(name, key) {
        super::instance::ScopedAdmission::Fresh(frame) => {
            ScopedBoundAdmission::Fresh(ScopedBoundUse {
                cfe,
                actuals,
                _frame: frame,
            })
        }
        super::instance::ScopedAdmission::Reenter => ScopedBoundAdmission::Reenter,
        super::instance::ScopedAdmission::Cycle { chain } => ScopedBoundAdmission::Cycle { chain },
        super::instance::ScopedAdmission::Widening {
            building,
            requested,
        } => ScopedBoundAdmission::Widening {
            building,
            requested,
        },
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Open a family's stored clauses as ONE relational query — the shared
/// opening for every position that reads a definition as a relation
/// (relation use here; the ER-edge road opens its rule body the same way).
/// The stored bytes normalize EXACTLY ONCE into a typed query.
fn open_relation_body(bound: BoundUse<NoActuals>) -> Result<OpenedRelationBody> {
    let name = bound.family.name().clone();
    let opened = bound.open_relation().map_err(|e| {
        DelightQLError::database_error(
            format!("Error while parsing borrowed view '{}': {}", name, e),
            e.to_string(),
        )
    })?;
    if opened.requires_frontier() {
        opened.into_relation_body().map_err(|e| {
            DelightQLError::database_error(
                format!("Error while expanding disjunctive view '{}': {}", name, e),
                e.to_string(),
            )
        })
    } else {
        opened.into_relation_body()
    }
}

/// The DECLARED-MODE tail: the arms resolve against the declared inputs
/// in the OWNED admission's world; the borrow keeps the instance alive.
fn resolve_mode_in(
    core: &mut ResolverCore<'_>,
    facts: BodyPositionFacts<'_>,
    bound: &BoundUse<NoActuals>,
    authored: crate::pipeline::asts::core::FactFunctionMode<
        crate::pipeline::asts::core::Unresolved,
    >,
    inputs: crate::pipeline::resolver::ResolvedRelation,
) -> Result<crate::pipeline::asts::core::FactFunctionMode<crate::pipeline::asts::core::Resolved>> {
    use crate::pipeline::asts::core::FactFunctionArm;
    let mut env = world_of(
        bound,
        &crate::defuse::carriers::CarrierRecord::default(),
        super::environment::FormalBindings::default(),
    );
    let mut body = body_fold_in(core, &mut env, facts);
    // THE ARMS STAND OVER THE DECLARED INPUT ROW and nothing else: the row
    // the declaration's own act declared, entered as it came.
    body.lexical
        .enter(inputs, crate::pipeline::resolver::Reach::Stage);
    let arms = authored.arms.try_map(
        |arm| -> Result<FactFunctionArm<crate::pipeline::asts::core::Resolved>> {
            Ok(FactFunctionArm {
                inputs: arm.inputs,
                outputs: arm.outputs.try_map(|value| body.transform_domain(value))?,
            })
        },
    )?;
    let default = match authored.default {
        Some(row) => Some(row.try_map(|value| body.transform_domain(value))?),
        None => None,
    };
    Ok(crate::pipeline::asts::core::FactFunctionMode {
        inputs: authored.inputs,
        outputs: authored.outputs,
        arms,
        default,
    })
}
