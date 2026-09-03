// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE ONE DEFINITION-USE OPERATION.
//!
//! Using a definition is one indivisible act: judge the selected family
//! against the use position, resolve the caller's actuals IN THE CALLER'S
//! WORLD, admit the use into the instance table, open the body IN ITS OWN
//! WORLD — a fresh body environment built from the family's declaration
//! reach, the resolved formals, and the family's grounding publication (or
//! the grounded closure it is derived under) — and hand the position's
//! opened artifact to the downstream
//! authority. Every position — relation, parameterized/HO, callable, sigma,
//! ER edge, effect rule, declared mode, pattern slot, cover cell — crosses
//! HERE. The steps are private: no caller can select a family and then skip
//! the admission, open a body beside a world the authority did not build,
//! or replay an unresolved actual inside the callee, because the pieces are
//! never separately reachable.

use super::environment::{Environment, RelationAnswer};
use super::instance::InstanceTable;
use super::select::LinkedFamily;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::unresolved as ast_unresolved;

pub(in crate::defuse) use super::admitted::{
    bind_definition_use, bind_scoped_use, family_display_namespace, require_fresh, use_er_rule,
    use_scoped_ho, BoundAdmission, BoundUse, HoActuals, HoUse, NoActuals, ScalarActuals,
    ScopedBoundAdmission, ScopedBoundUse, ValueActuals,
};
pub(crate) use super::admitted::{
    resolve_synthesized_body, use_effect_rule, EffectSelection, EffectUse, RelationUse,
    ScopedEffectUse,
};
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::resolution::ResolverCore;

pub(super) fn mutual_recursion_refusal(chain: Vec<String>) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::RECURSION_MUTUAL,
        format!(
            "circular consulted-definition expansion: mutual recursion is not supported; \
             the definition-instance cycle is {}",
            chain.join(" -> ")
        ),
        "break the cycle so each recursive definition reaches only its own established frontier",
    )
}

pub(crate) fn judge_recursive_frontier(
    instances: &InstanceTable,
    frontier: &super::instance::DefinitionFrontier,
) -> Result<()> {
    match instances.frontier_cycle(frontier) {
        Some(chain) => Err(mutual_recursion_refusal(chain)),
        None => Ok(()),
    }
}

fn mode_recursion_refusal(spelled: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "cfe/recursion",
        format!(
            "the declared mode of '{spelled}' reaches itself while its arms \
             resolve: a value definition cannot recurse"
        ),
        "a declared mode computes from its inputs; write recursion as a \
         relational rule",
    )
}

/// One SELECTED relation definition: the classification door's token.
/// Opaque — the family and its position kind travel together and cannot
/// be re-paired; the only thing a holder can do is hand it to
/// [`use_relation`], and holding it changes nothing.
#[derive(Debug)]
pub(crate) struct SelectedRelation<'s> {
    family: LinkedFamily<'s>,
    definition_kind: crate::relation::form::DefinitionKind,
}

/// Classify one QUALIFIED name for RELATION position over the world's
/// captured reach: the exhaustive selection and the kind judgment are ONE
/// act, and a consulted relational family comes back only as the opaque
/// [`SelectedRelation`] token inside the classification result. `None` is
/// a true miss — the caller's ladder stands.
pub(crate) fn classify_relation<'db>(
    core: &ResolverCore<'db>,
    reach: &super::environment::DeclarationReach,
    name: &str,
    stropped: bool,
    namespace_fq: &str,
) -> Result<Option<RelationAnswer<'db>>> {
    use crate::enums::EntityType;
    let Some(entity) = core
        .consult
        .select_entity(name, stropped, namespace_fq, reach)?
        .unique_or_refuse(name)?
    else {
        return Ok(None);
    };
    Ok(Some(match entity {
        super::select::Selected::Authored(family) => classify_family(family),
        super::select::Selected::Served(served) => match served.kind() {
            // A bin relation IS a relation; the runtime serves its rows.
            // Naming that category here is what keeps it out of the TVF
            // fallback, which would strip the namespace and generate SQL
            // against a phantom table.
            kind @ EntityType::BinRelation => RelationAnswer::RuntimeServedRelation {
                name: served.name().clone(),
                entity_type: kind,
            },
            EntityType::BinPseudoPredicate
            | EntityType::BinSigmaPredicate
            | EntityType::SyntaxDirective => RelationAnswer::DefinedNonRelation {
                name: served.name().clone(),
                entity_type: served.kind(),
            },
            _ => RelationAnswer::Unknown,
        },
    }))
}

/// Classify one selected AUTHORED family for relation position.
pub(in crate::defuse) fn classify_family(family: LinkedFamily) -> RelationAnswer {
    use crate::enums::EntityType;
    match family.kind() {
        EntityType::DqlTemporaryViewExpression => RelationAnswer::ConsultedView(SelectedRelation {
            family,
            definition_kind: crate::relation::form::DefinitionKind::View,
        }),
        // A fact IS a relational definition after elaboration; its
        // catalog kind stays Fact, and its relation-position road is
        // the view's.
        EntityType::DqlFactExpression => RelationAnswer::ConsultedView(SelectedRelation {
            family,
            definition_kind: crate::relation::form::DefinitionKind::Fact,
        }),
        kind => RelationAnswer::DefinedNonRelation {
            name: family.name().clone(),
            entity_type: kind,
        },
    }
}

/// One opened relation use: admission held, body opened and expanded to
/// its query, declaration world bound. Constructible only from the
/// classification token; consumed whole through [`Self::resolve_body`],
/// which resolves the body INSIDE the authority — the query, its name,
/// and its world never come apart in a caller's hands.
/// Use a selected relation token: admit the instance (a re-encounter
/// while the body is open is the ruled B5 refusal), open the body ONCE
/// and expand it to its query, in the family's own declaration world.
pub(crate) fn use_relation<'db>(
    caller: &ResolverFold<'_, 'db>,
    selected: SelectedRelation<'db>,
) -> Result<RelationUse<'db>> {
    let SelectedRelation {
        family,
        definition_kind,
    } = selected;
    let name = family.name().clone();
    let display_namespace = family.namespace().to_string();

    // THE MANDATORY TRANSITION: the family, its declaration environment,
    // and its (empty) actuals BIND before its body can open — the bound
    // use is the only opener. Re-encountering the family here means the
    // self-reference did NOT resolve as the in-progress CTE (recursive
    // clause before base, or an indirect cycle through another view) —
    // refuse with the teaching, never spin.
    let bound = require_fresh(
        bind_definition_use(&caller.config.instances, family, NoActuals)?,
        || DelightQLError::ValidationError {
            message: format!(
                "circular consulted-definition expansion: '{}::{}' is already \
                 being expanded. If this is a recursive rule, the \
                 base (non-recursive) clause must come FIRST in the consulted \
                 file — a self-reference is only recursive once a prior clause \
                 has established the name. If the cycle runs through another \
                 view, break the cycle. SEMANTICS/recursion-contract-law.md B5.",
                display_namespace, name
            ),
            context: "resolver::consulted_view_expansion".to_string(),
            subcategory: Some(crate::uri_registry::subcat::RECURSION_CONSULTED_CLAUSE_ORDER),
        },
    )?;

    super::admitted::relation_use(bound, name, definition_kind)
}

/// The closed outcome of a parameterized/HO use: a fresh instance opens,
/// or the use re-enters the active fixpoint — receiving that fixpoint's
/// frontier BY IDENTITY, never by finding a spelling again. Parameter
/// widening refuses INSIDE the entrance — it is the ruled terminal, never
/// an outcome to interpret.
pub(in crate::defuse) enum HoUseOutcome {
    Open(HoUse),
    Reenter {
        frontier: Option<super::instance::DefinitionFrontier>,
    },
}

/// THE FINISHED HO EXPANSION ARTIFACT: the resolved body with its bubbled
/// state, and the caller-resolved scalar actuals the body's formals spent
/// (by declared parameter) — the ports the call-site dispatch witness
/// selects its carriers by. This is what spending a [`HoUse`] returns —
/// resolved material only, never a body beside a world.
pub(crate) struct SquishedExpansion {
    pub(crate) resolved: crate::pipeline::resolver::ResolvedQuery,
    pub(crate) actuals: std::collections::HashMap<
        delightql_types::SqlIdentifier,
        crate::pipeline::asts::resolved::DomainExpression,
    >,
}

/// A RELATION-VALUED HIGHER-ORDER ACTUAL, ADMITTED AS A CLOSED RELATION
/// VALUE.
///
/// The one judgment (`admit`) is the only constructor, so a relation
/// formal can be bound to nothing but what it admitted. It judges the
/// FORM: a whole named relation or parameterized application (read whole),
/// an anonymous relation of any degree, or an explicit interior — and it
/// refuses an argumentative access, whose names are logical binders and
/// not a relation value. What it admits then resolves in a CLOSED world
/// (`resolve_carriers`): the actual reads its own source, its literals and
/// the statement's definitions, never the caller's row, its sibling
/// members, or its qualifiers — a name only the caller could answer
/// refuses as capture. Interior names do not escape; only the callee's
/// published result does.
#[derive(Debug, Clone)]
pub(crate) struct ClosedRelationActual {
    chain: ast_unresolved::Chain,
}

impl ClosedRelationActual {
    /// The form judgment. `callee`, `formal` and `position` are teaching
    /// vocabulary for the refusal.
    pub(in crate::defuse) fn admit(
        chain: ast_unresolved::Chain,
        callee: &str,
        formal: &str,
        position: usize,
    ) -> Result<Self> {
        // An explicit interior: `t(, cond |> (cols))` arrives wrapped as an
        // inner relation around the interior chain. The interior IS the
        // actual — its own read, over its own source.
        if !chain.has_steps() {
            if let ast_unresolved::GroundForm::Reference(
                ast_unresolved::Relation::InnerRelation {
                    pattern: ast_unresolved::InnerRelationPattern::Indeterminate { subquery, .. },
                    ..
                },
            ) = chain.head().form()
            {
                return Ok(Self {
                    chain: read_whole(*subquery.clone()),
                });
            }
        }
        let refuse = |shape: &str| {
            DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::HO_RELATION_ACTUAL_FORM,
                format!(
                    "parameter '{formal}' of '{callee}' is supplied at position {position} by \
                     {shape}: its names are logical binders, not a relation value, so it \
                     is not a closed relation this parameter can receive"
                ),
                "construct the relation with a closed interior — `t(, cond |> (cols))` — \
                 or bind it first with `:` and pass the whole named relation, `f(name(*))`",
            )
        };
        match chain.head_access() {
            // A whole read, or an inchoate one that reads whole.
            None | Some(ast_unresolved::Access::All | ast_unresolved::Access::Unasked) => {}
            Some(ast_unresolved::Access::Slots(_)) => {
                return Err(refuse("an argumentative access"));
            }
            // `.(cols)` / `.*` unify with the row the access stands in;
            // inside an argument there is no such row.
            Some(ast_unresolved::Access::Dequalify(_) | ast_unresolved::Access::DequalifyAll) => {
                return Err(refuse("a dequalifying access"));
            }
        }
        Ok(Self {
            chain: read_whole(chain),
        })
    }

    /// The admitted chain, spent into its carrier binding.
    pub(in crate::defuse) fn into_chain(self) -> ast_unresolved::Chain {
        self.chain
    }
}

/// A read with parens and no dimension named reads every dimension: the
/// carrier publishes the whole heading. Steps above the head's own access
/// are untouched.
fn read_whole(mut chain: ast_unresolved::Chain) -> ast_unresolved::Chain {
    if matches!(chain.head_access(), Some(ast_unresolved::Access::Unasked)) {
        if let Some(ast_unresolved::Continuation::Access { access, .. }) = chain
            .continuations_mut()
            .first_mut()
            .map(|step| step.form_mut())
        {
            *access = ast_unresolved::Access::All;
        }
    }
    chain
}

/// Resolve an ADMITTED ER body — single edge or composed chain — in its
/// declaration world. The one road that pairs an ER body with a world:
/// the world derives from the SAME bound use that opened the body; the
/// caller contributes its core and nothing lexical.
/// Use a selected family in SIGMA (existence test) position through the
/// MANDATORY transition: the caller's arguments resolve FIRST (in the
/// caller's world), the rule's instance is ADMITTED under their semantic
/// key, and the bound use is spent WHOLE by [`BoundUse::resolve_sigma`].
/// A self-citation while the body expands is a typed terminal — same
/// actuals refuse as circular expansion, changed actuals as the ruled
/// parameter widening — never unbounded compiler recursion. The polarity
/// is not applied here; it observes this body at the application.
pub(in crate::defuse) fn use_sigma(
    fold: &mut ResolverFold<'_, '_>,
    family: LinkedFamily,
    functor: &str,
    arguments: Vec<ast_unresolved::DomainExpression>,
) -> Result<crate::pipeline::asts::resolved::TruthExpression> {
    // THE CALLER'S ACTUALS RESOLVE FIRST, in the caller's world (its
    // formal frame included — a sigma cited inside a definition body sees
    // that body's formals in its arguments).
    let resolved_args: Vec<crate::pipeline::asts::resolved::DomainExpression> = arguments
        .into_iter()
        .map(|argument| fold.transform_domain(argument))
        .collect::<Result<Vec<_>>>()?;
    let bound = match bind_definition_use(
        &fold.config.instances,
        family,
        ValueActuals::of(resolved_args),
    )? {
        BoundAdmission::Fresh(bound) => bound,
        BoundAdmission::Reenter => {
            return Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::RECURSION_CONSULTED_CLAUSE_ORDER,
                format!(
                    "the sigma rule '{functor}' cites itself while its body expands: \
                     an existence test has no fixpoint to re-enter. Break the cycle, \
                     or write the recursion as a relational rule and test THAT."
                ),
                "resolver::consulted_view_expansion",
            ));
        }
        BoundAdmission::Cycle { chain } => return Err(mutual_recursion_refusal(chain)),
        BoundAdmission::Widening {
            building,
            requested,
        } => {
            return Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::RECURSION_PARAMETER_WIDENING,
                format!(
                    "'{functor}' is recursive and its self-citation changes an \
                     argument (building [{}], requested [{}]). A sigma rule's \
                     arguments select ONE expansion; recursive state belongs in a \
                     relational rule's ordinary columns.",
                    building.join(", "),
                    requested.join(", "),
                ),
                "recursive parameters never widen",
            ));
        }
    };
    bound.resolve_sigma(fold, functor)
}

/// One bound CALLABLE use — the consulted family road or its query-scoped
/// twin — typed by its actuals: scalar-call actuals for the call position,
/// value actuals for the cover-cell and pattern-slot positions. Spent
/// whole by the position's one consuming operation (`apply_call`,
/// `apply_cover`, `apply_slot`), which is what distinguishes the positions;
/// the carrier is the same closed pair of admissions.
pub(in crate::defuse) enum CallableUse<'s, A: super::admitted::ActualPayload> {
    Bound(BoundUse<'s, A>),
    Scoped(ScopedBoundUse<A>),
}

pub(crate) struct ModeUse<'s> {
    pub(crate) declaration: crate::resolution::registry::DeclaredMode,
    pub(crate) identity: crate::pipeline::asts::core::QualifiedName,
    pub(in crate::defuse) authored:
        crate::pipeline::asts::core::FactFunctionMode<crate::pipeline::asts::core::Unresolved>,
    /// The bound use, held while the declared arms resolve: a mode arm
    /// that reaches its own declaration re-encounters an OPEN instance
    /// instead of reopening forever.
    pub(in crate::defuse) bound: BoundUse<'s, NoActuals>,
}

/// Use a name in DECLARED-MODE (fact-function value) position. `None` is
/// a true miss — the callee declares no mode; the ordinary callable road
/// stands.
pub(crate) fn use_declared_mode<'db>(
    fold: &ResolverFold<'_, 'db>,
    spelled: &str,
    namespace: Option<&str>,
) -> Result<Option<ModeUse<'db>>> {
    let Some((family, declaration)) =
        fold.core
            .consult
            .select_declared_mode(spelled, namespace, fold.env.reach())?
    else {
        return Ok(None);
    };
    let identity = crate::pipeline::asts::core::QualifiedName {
        namespace_path: crate::pipeline::asts::core::NamespacePath::from_fq_string(
            family.namespace(),
        )
        .unwrap_or_else(|_| crate::pipeline::asts::core::NamespacePath::empty()),
        name: family.name().clone(),
    };
    // A declared-mode call carries its inputs as ordinary caller values;
    // the declaration itself is unparameterized for admission (one mode
    // per family). A mode arm reaching its own declaration is recursion
    // with no fixpoint: refuse.
    let bound = match bind_definition_use(&fold.config.instances, family, NoActuals)? {
        BoundAdmission::Fresh(bound) => bound,
        BoundAdmission::Cycle { chain } => return Err(mutual_recursion_refusal(chain)),
        BoundAdmission::Reenter | BoundAdmission::Widening { .. } => {
            return Err(mode_recursion_refusal(spelled));
        }
    };
    let group = bound.reconstruct_group()?;
    let Some(authored) = group.declared_mode() else {
        return Err(DelightQLError::database_error(
            "corrupt catalog: an entity declares a functional dependency and its stored \
             definition is not a fact function",
            spelled.to_string(),
        ));
    };
    // THE TWO READINGS ARE ONE DECLARATION. The catalog chose the
    // selected POSITION and the source supplies the expression at that
    // position, so they must agree about every name, its stropping, its
    // role and its order — equal widths under different names would
    // select the wrong output while looking consistent.
    if !declaration.agrees_with(
        &authored.inputs.iter().cloned().collect::<Vec<_>>(),
        &authored.outputs.iter().cloned().collect::<Vec<_>>(),
    ) {
        return Err(DelightQLError::database_error(
            "corrupt catalog: the stored mode and the stored definition are not the same \
             declaration",
            spelled.to_string(),
        ));
    }
    let authored = authored.clone();
    Ok(Some(ModeUse {
        declaration,
        identity,
        authored,
        bound,
    }))
}

/// Use a name in RUNTIME-SERVED VIEW position (the effect executor's
/// pre-splice): select the served definition, ADMIT it into the splice's
/// own instance table (the one detector — the family's identity, never
/// the authored call spelling), open the single unparameterized clause,
/// and run the expansion callback while the admitted instance holds —
/// under the SAME catalog read that selected the family: the expansion
/// receives the shared system borrow the family lives under, so no
/// mutation can be reached between selection and the expanded body.
/// `None` is a miss or a shape the splice does not serve (parameterized,
/// multi-clause, or non-relational); a stored body that fails to open is
/// catalog corruption, never a miss.
pub(crate) fn use_runtime_served_view<'s, R>(
    system: &'s crate::system::DelightQLSystem,
    name: &delightql_types::SqlIdentifier,
    namespace_fq: Option<&str>,
    spelled: &str,
    instances: &InstanceTable,
    expand: impl FnOnce(
        &'s crate::system::DelightQLSystem,
        crate::pipeline::asts::unresolved::Chain,
        &InstanceTable,
    ) -> Result<R>,
) -> Result<Option<R>> {
    let family = {
        let consult = crate::resolution::registry::ConsultRegistry::new_with_system(system);
        // The executor stands at the session; a qualified reference is
        // observed exactly, an unqualified one over the session's reach.
        let reach = super::environment::reach::capture(
            super::CatalogRead::of(system),
            "home",
            super::environment::reach::World::Session,
        )?;
        match consult.select_runtime_served_view(name, namespace_fq, &reach)? {
            Some(family) => family,
            None => return Ok(None),
        }
    };
    let bound = match bind_definition_use(instances, family, NoActuals)? {
        BoundAdmission::Fresh(bound) => bound,
        BoundAdmission::Cycle { chain } => return Err(mutual_recursion_refusal(chain)),
        BoundAdmission::Reenter | BoundAdmission::Widening { .. } => {
            return Err(DelightQLError::validation_error_categorized(
                "effect/runtime_served/cycle",
                format!(
                    "expanding '{}' reaches itself: a runtime-served relation's \
                     definitions cannot be recursive",
                    spelled
                ),
                "name the relation's source directly",
            ));
        }
    };
    // A selected family's body that fails to open is CATALOG CORRUPTION,
    // never a miss. A lawful shape the splice does not carry
    // (parameterized, multi-clause, non-relational) is a miss.
    let group = bound.reconstruct_group()?;
    let body = (|| {
        let mut clauses = group.into_clauses().into_iter();
        let (Some(clause), None) = (clauses.next(), clauses.next()) else {
            return None;
        };
        if !clause.params().is_empty() {
            return None;
        }
        clause
            .into_query()
            .map(crate::pipeline::asts::core::Query::into_bare_body)?
            .ok()
    })();
    let Some(body) = body else {
        return Ok(None);
    };
    // The bound instance holds across the expansion: a nested
    // self-reference re-encounters it and refuses above.
    let result = expand(system, body, instances);
    drop(bound);
    result.map(Some)
}

/// Select the unique HO family capable of PARAMETERIZED RELATION position
/// over the world's reach — kind judged over the complete candidate set,
/// never by probe order. `None` is a position miss (absence or wrong
/// kind; the caller's fallback ladder stands).
pub(in crate::defuse) fn select_enlisted_ho<'db>(
    core: &ResolverCore<'db>,
    env: &Environment,
    name: &str,
    stropped: bool,
) -> Result<Option<LinkedFamily<'db>>> {
    let candidates = core.consult.select_enlisted(name, stropped, env.reach())?;
    match super::select::judge_position(name, candidates, |k| {
        k == crate::enums::EntityType::DqlHoTemporaryViewExpression
    })? {
        super::select::PositionOutcome::Selected(super::select::Selected::Authored(family)) => {
            Ok(Some(family))
        }
        _ => Ok(None),
    }
}

/// The callable grade a use position supplies (ratified P11 / fixed law
/// 4): grade is a CONTRACT the position states, not a second identity.
/// A known DQL body or positive target descriptor must agree with it; an
/// unknown target callable accepts the position as the author's
/// assertion. The expectation flows INWARD through a DQL wrapper — the
/// instantiated body is judged where the position's value finally
/// stands, so a wrapper over an aggregate lawfully reduces and a wrapper
/// over a per-row expression cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallableGrade {
    RowWise,
    Reducing,
    Windowed,
}

/// What a RESOLVED value does to the group it stands in, judged by the
/// grade algebra over the value's own structure — never by the authored
/// spelling (an enlisted DQL definition named `sum` is its own body, not
/// the engine's aggregate).
pub(crate) enum ReductionStanding {
    /// Every row-column occurrence in the value is licensed: it stands
    /// under a reducing absorber (an engine aggregate, an unknown target
    /// callable — the author's assertion, a windowed application, a
    /// scalarized subquery, or a tree-group construction) or it IS a bare
    /// group key (constant per group by construction).
    Lawful,
    /// A row-column occurrence stands outside every absorber and is not a
    /// group key: one answer per row in a slot with one answer per group.
    PerRow,
}

/// The REDUCING grade's obligation, distributed over the value: an
/// absorber licenses its own subtree; every column occurrence outside an
/// absorber must be a bare group key. `sum:(x) + y` refuses unless `y` is
/// a key — an absorber somewhere does not license per-row reads
/// elsewhere, and there is no implicit aggregation, ever.
pub(crate) fn judge_grade(
    core: &ResolverCore<'_>,
    grade: CallableGrade,
    group_keys: &std::collections::HashSet<crate::relation::PortId>,
    value: &crate::pipeline::asts::resolved::DomainExpression,
) -> ReductionStanding {
    use crate::pipeline::ast_visit::{walk_visit_domain, AstVisit, Descent};
    use crate::pipeline::asts::core::Resolved;
    use crate::pipeline::asts::resolved as ast_resolved;

    match grade {
        CallableGrade::Reducing => {}
        // A row-wise or windowed position states its obligation at the
        // call, not on the finished value.
        CallableGrade::RowWise | CallableGrade::Windowed => return ReductionStanding::Lawful,
    }

    struct Judge<'a, 'reg> {
        core: &'a ResolverCore<'reg>,
        group_keys: &'a std::collections::HashSet<crate::relation::PortId>,
        unlicensed: bool,
    }
    impl AstVisit<Resolved> for Judge<'_, '_> {
        fn enter_domain(&mut self, e: &ast_resolved::DomainExpression) -> Result<Descent> {
            if let ast_resolved::DomainExpression::Reference(reference) = e {
                let is_key = match reference {
                    crate::pipeline::asts::core::Reference::Named(named) => {
                        self.group_keys.contains(&named.column().column)
                    }
                    _ => false,
                };
                if !is_key {
                    self.unlicensed = true;
                    return Ok(Descent::Break);
                }
            }
            Ok(Descent::Continue)
        }

        fn enter_function(&mut self, f: &ast_resolved::FunctionApplication) -> Result<Descent> {
            match f {
                ast_resolved::FunctionApplication::Standard(application) => {
                    // A windowed application is the engine's own judgment
                    // (window functions refuse in a grouped context there);
                    // its interior is not this obligation's.
                    if application.window.is_some() {
                        return Ok(Descent::SkipSubtree);
                    }
                    let mut name = String::new();
                    let spelled = self
                        .core
                        .identities
                        .write_function_name(
                            application.call().callee,
                            &mut crate::names::sink::Teaching(&mut name),
                        )
                        .is_ok();
                    if spelled {
                        let builtin = &self.core.built_in;
                        if builtin.is_aggregate(&name) || !builtin.is_known_function(&name) {
                            // The absorber licenses ITS OWN subtree only.
                            return Ok(Descent::SkipSubtree);
                        }
                    }
                    Ok(Descent::Continue)
                }
                // A scalarized subquery owns its interior scope; a
                // tree-group collects (the class's rows as an interior
                // relation).
                ast_resolved::FunctionApplication::Scalarized(_)
                | ast_resolved::FunctionApplication::Enclyph(_) => Ok(Descent::SkipSubtree),
                // A declared-mode call is one row PER INPUT ROW — its
                // arguments walk like any other value.
                ast_resolved::FunctionApplication::FieldSelect(_) => Ok(Descent::Continue),
                _ => Ok(Descent::Continue),
            }
        }
    }

    let mut judge = Judge {
        core,
        group_keys,
        unlicensed: false,
    };
    let _ = walk_visit_domain(&mut judge, value);
    if judge.unlicensed {
        ReductionStanding::PerRow
    } else {
        ReductionStanding::Lawful
    }
}

/// What the candidate set holds for a name in CALLABLE position, as the
/// closed presence the final target-fallback arm consults: a capable DQL
/// callable, a family of the wrong kind (present but unlicensed — it must
/// NOT silently reach the open target provider), or a true absence (the
/// only outcome that may).
pub(crate) enum CallablePresence {
    Callable,
    /// Present but unlicensed: the candidates' rendered provenance rides
    /// for the refusal — the pieces themselves stay inside the authority.
    WrongKind(String),
    Missing,
}

/// Judge callable presence for an unqualified name over the world's
/// complete candidate set. A windowed use of a consulted definition never
/// reaches this judgment: the instantiation road opens the body with the
/// window obligation armed, so the grade flows inward instead of refusing
/// by kind here.
pub(crate) fn callable_presence(
    core: &ResolverCore,
    env: &Environment,
    callee: &delightql_types::SqlIdentifier,
) -> Result<CallablePresence> {
    let capable = |k: crate::enums::EntityType| {
        k == crate::enums::EntityType::DqlFunctionExpression
            || k == crate::enums::EntityType::DqlContextAwareFunctionExpression
    };
    let candidates =
        core.consult
            .select_enlisted(callee.as_str(), callee.is_stropped(), env.reach())?;
    match super::select::judge_position(callee.as_str(), candidates, capable)? {
        super::select::PositionOutcome::Selected(_) => Ok(CallablePresence::Callable),
        super::select::PositionOutcome::WrongKind(candidates) => Ok(CallablePresence::WrongKind(
            candidates
                .iter()
                .map(|c| format!("{} ({})", c.namespace, c.kind.variant_name()))
                .collect::<Vec<_>>()
                .join(", "),
        )),
        super::select::PositionOutcome::Missing => Ok(CallablePresence::Missing),
    }
}

/// THE IDENTITY A QUALIFIED SELECTION ANSWERED WITH: a served bin sigma
/// predicate as the catalog activates it — its own namespace and its own
/// name — never the qualifier the author wrote. An alias, a case variant
/// and the exact spelling all select this one identity, and it is the only
/// thing the resolver may build the callee from.
pub(crate) struct SelectedBinSigma {
    namespace: Vec<String>,
    name: String,
}

impl SelectedBinSigma {
    fn of(served: &super::select::ServedEntity) -> Self {
        SelectedBinSigma {
            namespace: served.namespace().split("::").map(str::to_string).collect(),
            name: served.name().as_str().to_string(),
        }
    }

    /// The callee that names this identity in the resolved graph. Lowering
    /// reads the namespace back off this record, so generation looks the
    /// entity up exactly where the selection found it.
    pub(crate) fn callee(&self, identities: &crate::names::Registry) -> crate::names::FnId {
        let name = identities.intern(&self.name, false);
        let namespace = self
            .namespace
            .iter()
            .map(|part| identities.intern(part, false))
            .collect();
        identities.mint_function(name, namespace)
    }
}

/// A QUALIFIED sigma citation's outcome: the rule expanded, a served bin
/// sigma predicate selected in its own namespace, or neither (the arguments
/// ride back for the caller's inner-exists road — a qualified citation
/// always names a namespace entity).
pub(crate) enum SigmaQualified {
    Expanded(crate::pipeline::asts::resolved::TruthExpression),
    /// The qualifier selected a bin sigma predicate (`std::prelude.sql_eq`,
    /// through whatever spelling reached it): the caller takes the bin road
    /// with the SELECTED identity, and the arguments ride along.
    ServedBin {
        selected: SelectedBinSigma,
        arguments: Vec<ast_unresolved::DomainExpression>,
    },
    NotSigma(Vec<ast_unresolved::DomainExpression>),
}

/// Use a QUALIFIED name in SIGMA position: the qualifier resolves through
/// the same reach-aware selection as qualified relations; a sigma-rule
/// hit opens and expands HERE; any other outcome hands the arguments back
/// for the caller's qualified-relation road.
pub(crate) fn use_sigma_qualified(
    fold: &mut ResolverFold<'_, '_>,
    functor: &str,
    functor_stropped: bool,
    namespace_fq: &str,
    arguments: Vec<ast_unresolved::DomainExpression>,
) -> Result<SigmaQualified> {
    let selected = fold
        .core
        .consult
        .select_entity(functor, functor_stropped, namespace_fq, fold.env.reach())?
        .unique_or_refuse(functor)?;
    match selected {
        Some(super::select::Selected::Authored(family))
            if family.kind() == crate::enums::EntityType::DqlTemporarySigmaRule =>
        {
            Ok(SigmaQualified::Expanded(use_sigma(
                fold, family, functor, arguments,
            )?))
        }
        Some(super::select::Selected::Served(served))
            if served.kind() == crate::enums::EntityType::BinSigmaPredicate =>
        {
            Ok(SigmaQualified::ServedBin {
                selected: SelectedBinSigma::of(&served),
                arguments,
            })
        }
        _ => Ok(SigmaQualified::NotSigma(arguments)),
    }
}

/// An UNQUALIFIED existence test's closed outcome, judged over BOTH faces
/// at once — the sigma rule and the relation — so probe order never
/// decides the collision the checker exists for.
pub(crate) enum SigmaEnlisted {
    /// The unique sigma rule answered (no relation collides): expanded.
    Expanded(crate::pipeline::asts::resolved::TruthExpression),
    /// A relation answers and no sigma does: the arguments ride back for
    /// the caller's table-as-sigma road.
    RelationAnswers(Vec<ast_unresolved::DomainExpression>),
    /// Neither face answers: the bin fall-through stands.
    Neither(Vec<ast_unresolved::DomainExpression>),
}

/// Use an UNQUALIFIED name in SIGMA (existence) position. The complete
/// candidate set of the world's reach is enumerated ONCE; the relation
/// face is probed through the same one lookup the relation path uses; a
/// sigma rule and a relation both answering is the ambiguity refusal,
/// judged HERE.
pub(crate) fn use_sigma_enlisted(
    fold: &mut ResolverFold<'_, '_>,
    functor: &str,
    functor_stropped: bool,
    arguments: Vec<ast_unresolved::DomainExpression>,
) -> Result<SigmaEnlisted> {
    let spelled = if functor_stropped {
        delightql_types::SqlIdentifier::stropped(functor.to_string())
    } else {
        delightql_types::SqlIdentifier::new(functor.to_string())
    };
    // ONE typed judgment over one exhaustive enumeration: the sigma face
    // and the relation face come back together, so probe order and an
    // erased wrong-kind ambiguity can never invent a collision.
    match fold.env.sigma_position(fold.core, &spelled)? {
        crate::defuse::environment::lookup::SigmaPosition::Collision { sigma } => {
            Err(DelightQLError::validation_error_categorized(
                "resolution/ambiguous",
                format!(
                    "Ambiguous entity '{}': a relation and the sigma rule in \
                     namespace {} both answer this existence test. While both \
                     definitions are live neither may answer — qualify the \
                     reference to choose one.",
                    functor,
                    family_display_namespace(&sigma),
                ),
                "Ambiguous existence test",
            ))
        }
        crate::defuse::environment::lookup::SigmaPosition::Sigma(sigma) => Ok(
            SigmaEnlisted::Expanded(use_sigma(fold, sigma, functor, arguments)?),
        ),
        crate::defuse::environment::lookup::SigmaPosition::RelationAnswers => {
            Ok(SigmaEnlisted::RelationAnswers(arguments))
        }
        crate::defuse::environment::lookup::SigmaPosition::Neither => {
            Ok(SigmaEnlisted::Neither(arguments))
        }
    }
}

/// Refuse a qualified name that selects a RUNTIME-SERVED bin relation in
/// a road that cannot serve it (the TVF fallback would strip its
/// namespace and compile a phantom table). Selection and the kind
/// judgment are the authority's; the refusal carries the entity's
/// identity through the caller-supplied constructor.
pub(crate) fn refuse_served_bin_relation(
    core: &ResolverCore,
    env: &Environment,
    function: &str,
    function_stropped: bool,
    namespace_fq: &str,
    refusal: impl FnOnce(&delightql_types::SqlIdentifier, crate::enums::EntityType) -> DelightQLError,
) -> Result<()> {
    if let Some(entity) = core
        .consult
        .select_entity(function, function_stropped, namespace_fq, env.reach())?
        .unique_or_refuse(function)?
    {
        if entity.kind() == crate::enums::EntityType::BinRelation {
            return Err(refusal(entity.name(), entity.kind()));
        }
    }
    Ok(())
}
