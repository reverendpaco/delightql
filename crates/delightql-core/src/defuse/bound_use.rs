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
use crate::diagnostic::{Cfe, Effect, Ho, Internal, Recursion, Resolution};
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::unresolved as ast_unresolved;

pub(in crate::defuse) use super::admitted::{
    bind_definition_use, bind_scoped_use, family_display_namespace, require_fresh, use_er_rule,
    use_scoped_ho, BoundAdmission, BoundUse, HoActuals, HoUse, NoActuals, ScalarActuals,
    ScopedBoundAdmission, ScopedBoundUse, ValueActuals,
};
pub(crate) use super::admitted::{resolve_synthesized_body, RelationUse};
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::resolution::ResolverCore;

pub(super) fn mutual_recursion_refusal(chain: Vec<String>) -> DelightQLError {
    DelightQLError::from(Recursion::Mutual {
        message: format!(
            "circular consulted-definition expansion: mutual recursion is not supported; \
             the definition-instance cycle is {}",
            chain.join(" -> ")
        ),
    })
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
    DelightQLError::from(Cfe::Recursion {
        message: format!(
            "the declared mode of '{spelled}' reaches itself while its arms \
             resolve: a value definition cannot recurse"
        ),
    })
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
        || {
            DelightQLError::from(Recursion::ConsultedClauseOrder {
                message: format!(
                    "circular consulted-definition expansion: '{}::{}' is already \
                 being expanded. If this is a recursive rule, the \
                 base (non-recursive) clause must come FIRST in the consulted \
                 file — a self-reference is only recursive once a prior clause \
                 has established the name. If the cycle runs through another \
                 view, break the cycle. SEMANTICS/recursion-contract-law.md B5.",
                    display_namespace, name
                ),
            })
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
/// (`bind_relation_formals`): the actual reads its own source, its literals and
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
            DelightQLError::from(Ho::RelationActualForm {
                message: format!(
                    "parameter '{formal}' of '{callee}' is supplied at position {position} by \
                     {shape}: its names are logical binders, not a relation value, so it \
                     is not a closed relation this parameter can receive"
                ),
            })
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
            return Err(DelightQLError::from(Recursion::ConsultedClauseOrder {
                message: format!(
                    "the sigma rule '{functor}' cites itself while its body expands: \
                     an existence test has no fixpoint to re-enter. Break the cycle, \
                     or write the recursion as a relational rule and test THAT."
                ),
            }));
        }
        BoundAdmission::Cycle { chain } => return Err(mutual_recursion_refusal(chain)),
        BoundAdmission::Widening {
            building,
            requested,
        } => {
            return Err(DelightQLError::from(Recursion::ParameterWidening {
                message: format!(
                    "'{functor}' is recursive and its self-citation changes an \
                     argument (building [{}], requested [{}]). A sigma rule's \
                     arguments select ONE expansion; recursive state belongs in a \
                     relational rule's ordinary columns.",
                    building.join(", "),
                    requested.join(", "),
                ),
            }));
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
        return Err(Internal::invariant(
            "defuse::bound_use",
            "corrupt catalog: an entity declares a functional dependency and its stored \
             definition is not a fact function",
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
        return Err(Internal::invariant(
            "defuse::bound_use",
            "corrupt catalog: the stored mode and the stored definition are not the same \
             declaration",
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
            return Err(DelightQLError::from(Effect::RuntimeServedCycle {
                message: format!(
                    "expanding '{}' reaches itself: a runtime-served relation's \
                     definitions cannot be recursive",
                    spelled
                ),
            }));
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
    /// An ordinary aggregate stands where its input is already one row of
    /// a reduction — inside another aggregate's argument, or inside the
    /// row a collecting record or tuple gathers — with no relation
    /// boundary between the two reductions. The teaching names both.
    Nested { teaching: String },
}

/// The one classification of a resolved standard application the grade
/// judgments dispatch on. Judged from the RESOLVED callee and the
/// application's own window, never from the authored spelling.
enum ApplicationRole {
    /// A windowed application: one value per input row, the engine's own
    /// judgment over its interior.
    Windowed,
    /// An engine aggregate: a reducing absorber.
    Aggregate(String),
    /// A callable the registry cannot classify: the author's assertion.
    UnknownTarget,
    /// A known scalar function: its arguments stand where it stands.
    Scalar,
}

fn application_role(
    core: &ResolverCore<'_>,
    application: &crate::pipeline::asts::resolved::StandardApplication,
) -> ApplicationRole {
    if application.window.is_some() {
        return ApplicationRole::Windowed;
    }
    let mut name = String::new();
    let spelled = core
        .identities
        .write_function_name(
            application.call().callee,
            &mut crate::names::sink::Teaching(&mut name),
        )
        .is_ok();
    if !spelled {
        return ApplicationRole::Scalar;
    }
    let builtin = &core.built_in;
    if builtin.is_aggregate(&name) {
        ApplicationRole::Aggregate(name)
    } else if builtin.is_known_function(&name) {
        ApplicationRole::Scalar
    } else {
        ApplicationRole::UnknownTarget
    }
}

/// The REDUCING grade's obligation, distributed over the value.
///
/// Two laws share one walk. THE ABSORBER LAW: an absorber licenses its own
/// subtree; every column occurrence outside an absorber must be a bare
/// group key. `sum:(x) + y` refuses unless `y` is a key — an absorber
/// somewhere does not license per-row reads elsewhere, and there is no
/// implicit aggregation, ever. THE NESTING LAW: a collecting record or
/// tuple gathers the group's rows, and an aggregate's argument is one row
/// of its input, so an ordinary aggregate standing inside either has no
/// group of its own to reduce — `sum:(sum:(x))` and `{ k, "t": sum:(x) }`
/// both refuse until a stage supplies the relation boundary. A window is
/// one value per row and stands anywhere a value stands; a scalarized
/// subquery is its own relation and its own boundary; a metadata group's
/// target is a collector over each key's partition, judged like the record
/// it stands in.
pub(crate) fn judge_grade(
    core: &ResolverCore<'_>,
    grade: CallableGrade,
    group_keys: &std::collections::HashSet<crate::relation::PortId>,
    value: &crate::pipeline::asts::resolved::DomainExpression,
) -> ReductionStanding {
    use crate::pipeline::asts::resolved as ast_resolved;

    match grade {
        CallableGrade::Reducing => {}
        // A row-wise or windowed position states its obligation at the
        // call, not on the finished value.
        CallableGrade::RowWise | CallableGrade::Windowed => return ReductionStanding::Lawful,
    }

    let mut judge = Judge {
        core,
        group_keys,
        collector_depth: 0,
        aggregate_depth: 0,
        unlicensed: false,
        nested: None,
    };
    match value {
        // THE COLLECTOR AT THE SLOT: its members are the collected row.
        ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Enclyph(enclyph),
        ) => judge.collected(enclyph),
        other => judge.leaf(other),
    }
    if let Some(teaching) = judge.nested {
        ReductionStanding::Nested { teaching }
    } else if judge.unlicensed {
        ReductionStanding::PerRow
    } else {
        ReductionStanding::Lawful
    }
}

/// The same judgment for the collectors a GROUPING-KEY record holds: its
/// induced members and its metadata members collect within the group the
/// record keys, exactly as they do under `~>`. The record's plain members
/// are keys, not collected rows, and are not this judgment's.
pub(crate) fn judge_key_record_collectors(
    core: &ResolverCore<'_>,
    record: &crate::pipeline::asts::resolved::Record,
) -> ReductionStanding {
    use crate::pipeline::asts::core::RecordMember;
    let mut judge = Judge {
        core,
        group_keys: &std::collections::HashSet::new(),
        collector_depth: 0,
        aggregate_depth: 0,
        unlicensed: false,
        nested: None,
    };
    for member in record.members.iter() {
        if judge.nested.is_some() {
            break;
        }
        match member {
            RecordMember::Induced { value, .. } => judge.collected(value),
            RecordMember::Metadata { group, .. } => judge.metadata_target(group),
            RecordMember::Keyed { .. } | RecordMember::SelfKeyed(_) | RecordMember::Spread(_) => {}
        }
    }
    match judge.nested {
        Some(teaching) => ReductionStanding::Nested { teaching },
        None => ReductionStanding::Lawful,
    }
}

/// The same judgment for a metadata group standing at the reduction slot:
/// its target collects each key's partition, so its members are judged as
/// a collected row. A metadata group has no per-row reading to refuse, only
/// the nesting law.
pub(crate) fn judge_metadata_reduction(
    core: &ResolverCore<'_>,
    group: &crate::pipeline::asts::resolved::MetadataGroup,
) -> ReductionStanding {
    let mut judge = Judge {
        core,
        group_keys: &std::collections::HashSet::new(),
        collector_depth: 0,
        aggregate_depth: 0,
        unlicensed: false,
        nested: None,
    };
    judge.metadata_target(group);
    match judge.nested {
        Some(teaching) => ReductionStanding::Nested { teaching },
        None => ReductionStanding::Lawful,
    }
}

struct Judge<'a, 'reg> {
    core: &'a ResolverCore<'reg>,
    group_keys: &'a std::collections::HashSet<crate::relation::PortId>,
    /// How many collecting constructors enclose the position being read:
    /// inside one, a column is one row of the collected relation and
    /// licensed; an aggregate is a reduction of a reduction.
    collector_depth: usize,
    /// How many engine aggregates enclose the position being read.
    aggregate_depth: usize,
    unlicensed: bool,
    nested: Option<String>,
}

impl Judge<'_, '_> {
    fn inside_reduction(&self) -> bool {
        self.collector_depth > 0 || self.aggregate_depth > 0
    }

    /// The row a collecting record or tuple gathers, member by member. An
    /// induced member is a collector of its own beneath this one, and so is
    /// a metadata member's target, partitioned by its key.
    fn collected(&mut self, enclyph: &crate::pipeline::asts::resolved::Enclyph) {
        use crate::pipeline::asts::core::{Enclyph, RecordMember};
        self.collector_depth += 1;
        match enclyph {
            Enclyph::Record(record) => {
                for member in record.members.iter() {
                    if self.nested.is_some() {
                        break;
                    }
                    match member {
                        RecordMember::SelfKeyed(_) => {}
                        RecordMember::Keyed { value, .. } => self.leaf(value),
                        RecordMember::Induced { value, .. } => self.collected(value),
                        RecordMember::Metadata { group, .. } => self.metadata_target(group),
                        RecordMember::Spread(spread) => self.leaf(spread.expanded()),
                    }
                }
            }
            Enclyph::EmptyRecord(_) => {}
            Enclyph::Tuple(tuple) => {
                for element in tuple.elements.iter() {
                    if self.nested.is_some() {
                        break;
                    }
                    self.leaf(element.value());
                }
            }
        }
        self.collector_depth -= 1;
    }

    /// A metadata group's target: the collector at the bottom of its key
    /// chain, one partition per key value.
    fn metadata_target(&mut self, group: &crate::pipeline::asts::resolved::MetadataGroup) {
        use crate::pipeline::asts::core::MetadataTarget;
        match &group.target {
            MetadataTarget::Enclyph(enclyph) => self.collected(enclyph),
            MetadataTarget::Group(nested) => self.metadata_target(nested),
        }
    }

    /// One value, wherever it stands: at the slot, or as a collected cell.
    fn leaf(&mut self, value: &crate::pipeline::asts::resolved::DomainExpression) {
        let _ = crate::pipeline::ast_visit::walk_visit_domain(self, value);
    }

    fn refuse_nested(&mut self, aggregate: &str) {
        let where_ = if self.aggregate_depth > 0 {
            "inside another aggregate's argument, which is already one row of that \
             reduction's input"
                .to_string()
        } else {
            "inside a collecting record or tuple, whose members are the rows it \
             gathers"
                .to_string()
        };
        self.nested = Some(format!(
            "`{aggregate}:` stands {where_}: a reduction of a reduction needs a \
             relation boundary between the two — reduce in a stage of its own \
             (`|> %(keys ~> {aggregate}:(…) as name)`) and read the result here"
        ));
    }
}

impl crate::pipeline::ast_visit::AstVisit<crate::pipeline::asts::core::Resolved> for Judge<'_, '_> {
    fn enter_domain(
        &mut self,
        e: &crate::pipeline::asts::resolved::DomainExpression,
    ) -> Result<crate::pipeline::ast_visit::Descent> {
        use crate::pipeline::ast_visit::Descent;
        if self.nested.is_some() {
            return Ok(Descent::Break);
        }
        if let crate::pipeline::asts::resolved::DomainExpression::Reference(reference) = e {
            let is_key = match reference {
                crate::pipeline::asts::core::Reference::Named(named) => {
                    self.group_keys.contains(&named.column().column)
                }
                _ => false,
            };
            if !is_key && !self.inside_reduction() {
                self.unlicensed = true;
            }
        }
        Ok(Descent::Continue)
    }

    fn enter_function(
        &mut self,
        f: &crate::pipeline::asts::resolved::FunctionApplication,
    ) -> Result<crate::pipeline::ast_visit::Descent> {
        use crate::pipeline::ast_visit::Descent;
        use crate::pipeline::asts::resolved as ast_resolved;
        match f {
            ast_resolved::FunctionApplication::Standard(application) => {
                match application_role(self.core, application) {
                    // One value per input row; the engine judges the
                    // interior of the window itself.
                    ApplicationRole::Windowed => Ok(Descent::SkipSubtree),
                    ApplicationRole::Aggregate(name) => {
                        if self.inside_reduction() {
                            self.refuse_nested(&name);
                            return Ok(Descent::Break);
                        }
                        self.aggregate_depth += 1;
                        Ok(Descent::Continue)
                    }
                    // Outside every reduction the position is reducing and
                    // the unknown callable is the author's assertion of it;
                    // inside one the position is row-wise and its arguments
                    // stand where it stands.
                    ApplicationRole::UnknownTarget => Ok(if self.inside_reduction() {
                        Descent::Continue
                    } else {
                        Descent::SkipSubtree
                    }),
                    ApplicationRole::Scalar => Ok(Descent::Continue),
                }
            }
            // A construction inside a value gathers its members as one row
            // of the enclosing value: still the collected row, or still the
            // aggregate's argument.
            ast_resolved::FunctionApplication::Enclyph(_) => {
                self.collector_depth += 1;
                Ok(Descent::Continue)
            }
            // A scalarized subquery is its own relation: the boundary the
            // nesting law asks for, and a scope of its own.
            ast_resolved::FunctionApplication::Scalarized(_) => Ok(Descent::SkipSubtree),
            _ => Ok(Descent::Continue),
        }
    }

    fn exit_function(
        &mut self,
        f: &crate::pipeline::asts::resolved::FunctionApplication,
    ) -> Result<crate::pipeline::ast_visit::Descent> {
        use crate::pipeline::ast_visit::Descent;
        use crate::pipeline::asts::resolved as ast_resolved;
        match f {
            ast_resolved::FunctionApplication::Standard(application) => {
                if let ApplicationRole::Aggregate(_) = application_role(self.core, application) {
                    self.aggregate_depth -= 1;
                }
            }
            ast_resolved::FunctionApplication::Enclyph(_) => {
                self.collector_depth -= 1;
            }
            _ => {}
        }
        Ok(Descent::Continue)
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
            Err(DelightQLError::from(Resolution::Ambiguous {
                message: format!(
                    "Ambiguous entity '{}': a relation and the sigma rule in \
                     namespace {} both answer this existence test. While both \
                     definitions are live neither may answer — qualify the \
                     reference to choose one.",
                    functor,
                    family_display_namespace(&sigma),
                ),
            }))
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
