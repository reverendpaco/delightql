// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE EFFECT-BODY AUTHORITY.
//!
//! An effect body is unresolved syntax whose interpretation depends on the
//! world its declaration stands in: the query's own text for the program
//! body, a consulted rule's declaration for a catalogued rule, and the
//! declaring site for a query-scoped effect CHOE clause or an effect-marked
//! CTE label's arm. This module owns the four acts that together decide what
//! such a body means — SELECTION at a demand site, ADMISSION of the use,
//! DERIVATION of the world the body reads in, and the TRAVERSAL that reads
//! it — as one operation on one value, under one privacy boundary.
//!
//! **What holds this together is where it stands.** It is a child of the
//! scoped-definition authority, so the clauses a [`ScopedHo`] holds and the
//! arms a [`ScopedEffectArms`] holds are readable here through their private
//! fields and nowhere else: no accessor yields them, and no signature in the
//! crate accepts them. Every carrier below — the demand's selection bound to
//! the world it was selected from, the invoked rule holding the world it
//! derived, the clause body paired with the declaration it opens — has
//! private fields, no constructor function, and one spending operation. A
//! struct literal is the only way to make one, and a struct literal is legal
//! only in this module tree, at the derivation site that holds both halves.
//!
//! **The planner is a service.** [`PlanBuilder`] stores plan state, allocates
//! scratch, lowers RESOLVED statements and renders SQL. Every operation the
//! walk asks of it takes facts or finished artifacts — a resolved query, a
//! SQL expression, a relation, a name — and none takes an unresolved chain
//! or a walk context. The syntax of an effect body, scoped or not, does not
//! leave this module tree except as the resolved, lowered thing it means.

mod demand;
mod invoke;
mod statement;
#[cfg(test)]
mod tests;
mod walk;

use std::collections::{HashMap, HashSet};

use super::{ScopedEffectArms, ScopedHo};
use crate::defuse::admitted::{
    bind_definition_use, choe_recursion_refusal, require_fresh, ActualPayload, BoundUse,
    EffectActuals, EffectInput,
};
use crate::defuse::environment::{
    DeclaredArms, DeclaredBlock, Environment, FormalInventory, FormalRole, SharedInstantiated,
    UseEnvironment,
};
use crate::defuse::instance::{InstanceFrame, InstanceTable, ScopedAdmission};
use crate::defuse::select::LinkedFamily;
use crate::diagnostic::Effect;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved::{self, Chain, Query};
use crate::pipeline::asts::ddl::HoParam;
use crate::pipeline::asts::effects;
use crate::pipeline::compiled_query::CompiledPlan;
use crate::pipeline::effect_transformer::PlanBuilder;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::resolution::{ConsultRegistry, ResolverCore};
use crate::system::DelightQLSystem;

use walk::bare_name;

// ============================================================================
// The entrances: a plan is compiled from a body this module walks
// ============================================================================

/// COMPILE AN AD-HOC PROGRAM: the top-level directive-demanding statement,
/// walked in the plan's own program world with its block declared for the
/// whole walk, its value shipped as the run's return.
pub(crate) fn compile_program(
    plan: &mut PlanBuilder<'_>,
    body: effects::EffectBody,
) -> Result<CompiledPlan> {
    let world = EffectWorld::program(plan.system(), plan.namespace().unwrap_or("home"))?;
    let mut walk = EffectWalk::over(plan);
    let top_ctx = WalkCtx {
        world: &world,
        guards: Vec::new(),
        sink: None,
        bindings: HashMap::new(),
        receipt_name: "main".to_string(),
    };
    // THE BODY'S BLOCK IS DECLARED ON ITS WORLD for the whole walk, so
    // every statement and every directive demand beneath reads the one
    // declaration set. It closes when the walk does.
    let _block = DeclaredBlock::open(world.cell(), body.locals);
    let value = walk.walk_value(body.expression, &top_ctx)?;
    // The run's return value, compiled while the block still stands.
    let final_text = walk.compile_value_text(&value, &top_ctx)?;
    walk.plan.finish(final_text)
}

/// COMPILE A DEMANDED RULE — `main!`, or any registered effect rule — into
/// the same bracketed plan shape: the rule is invoked at the top of the
/// plan's program world and its receipt is the run's return.
pub(crate) fn compile_rule(
    plan: &mut PlanBuilder<'_>,
    rule: EffectUse<'_>,
) -> Result<CompiledPlan> {
    let world = EffectWorld::program(plan.system(), plan.namespace().unwrap_or("home"))?;
    let mut walk = EffectWalk::over(plan);
    let top_ctx = WalkCtx {
        world: &world,
        guards: Vec::new(),
        sink: None,
        bindings: HashMap::new(),
        receipt_name: bare_name(rule.rule_name().as_str()).to_string(),
    };
    // A demanded program is invoked with an empty row; a declared parameter
    // row is judged against it like any other application's.
    let value = walk.invoke_rule(
        EffectSelection::Consulted(rule),
        &ast_unresolved::CallArguments::None,
        &top_ctx,
        true,
    )?;
    let final_text = walk.compile_value_text(&value, &top_ctx)?;
    walk.plan.finish(final_text)
}

/// THE WITNESS THAT A RECEIPT IS BEING NAMED BY THE PLAN: a scratch row is
/// placed under an authored name only here, where the walk places it, and
/// only this module constructs the witness.
pub struct ReceiptNaming(());

// ============================================================================
// The walk: the one reader of effect syntax
// ============================================================================

/// ONE PASS'S WALK over an effect body, and the state that is the walk's
/// own rather than the plan's: the higher-order inputs bound by the
/// invocations it is inside, the invocation path for step display, and the
/// hazard map of the views it has created. It holds the planner as a
/// service for exactly the pass's extent.
struct EffectWalk<'p, 'a> {
    plan: &'p mut PlanBuilder<'a>,
    /// HO inputs bound during rule invocations (`WalkCtx.bindings` indexes).
    bound_inputs: Vec<BoundInput>,
    /// The CURRENT invocation path, FOR STEP-MARK DISPLAY ONLY: nothing
    /// branches on membership — recursion is the definition-use
    /// authority's admission law, not this list's.
    rule_stack: Vec<String>,
    /// Base tables read by each plan-created temp VIEW — the
    /// self-reference hazard map.
    view_bases: HashMap<String, HashSet<String>>,
    /// Monotone mutation counter (CTAS / INSERT / UPDATE / DELETE bump it).
    mutation_epoch: u64,
}

impl<'p, 'a> EffectWalk<'p, 'a> {
    fn over(plan: &'p mut PlanBuilder<'a>) -> Self {
        EffectWalk {
            plan,
            bound_inputs: Vec::new(),
            rule_stack: Vec::new(),
            view_bases: HashMap::new(),
            mutation_epoch: 0,
        }
    }
}

// ============================================================================
// Walk-time context
// ============================================================================

/// A gate accumulated from a left conjunct (conjunction evaluates left
/// to right; an empty step ends the chain — so a directive to the RIGHT of
/// a conjunct executes gated on the conjunct's non-emptiness).
#[derive(Clone)]
enum GuardSource {
    /// The left conjunct is a bare glob read of a plan scratch table (the
    /// receipt-gate case) — renders `EXISTS (SELECT 1 FROM t)`, the
    /// TORTURE-TEST-NORMAL spelling.
    Table(crate::relation::SemanticRelation),
    /// Arbitrary pure left conjunct — compiled to a subquery at stamp time.
    ///
    /// Lowered once per consumer: a gated DML carries the guard, and so does
    /// the receipt insert reporting on that DML. Each lowering is a separate
    /// occurrence of the conjunct and mints its own scopes, which is why the
    /// expression stored here holds no pre-decided identity to collide over.
    /// The conjunct's own block is the body's, declared on the world for
    /// the body's extent — a lowering stands inside it and carries none.
    Expr { body: Box<Chain> },
}

/// A higher-order input bound into a rule invocation (`X |> rule!(*)` binds
/// X to the rule's one table parameter). The pure input may
/// re-evaluate at its splice site ONLY within a mutation-free window; if a
/// mutation was emitted between binding and splice, the input is
/// retro-materialized at `insertion_index` (before the mutation) and the
/// splice reads the snapshot instead.
struct BoundInput {
    /// The plan scratch the piped input was staged into AT THE DEMAND
    /// SITE; every mention inside the rule reads it by its receipt.
    scope: crate::relation::ScratchRow,
}

/// Per-walk lexical context. Cloned at scope boundaries.
#[derive(Clone)]
struct WalkCtx<'w> {
    /// THE WORLD THIS WALK'S STATEMENTS RESOLVE IN: the invoked rule's
    /// world, owned by the invocation that built it, or the plan's own
    /// program world. Reached only through the world's named operations.
    world: &'w EffectWorld,
    /// EXISTS gates from enclosing left conjuncts.
    guards: Vec<GuardSource>,
    /// When walking a rule CLAUSE, the shared receipt table its ENDING
    /// directive writes into (a multi-clause rule's receipts
    /// land in ONE receipt table). Propagates only along the value path:
    /// through a pipe to its terminal, to a join's right, into every union
    /// arm; cleared into pipe sources / join lefts / filters.
    sink: Option<ReceiptSink>,
    /// HO parameter bindings (param name → index into `bound_inputs`).
    bindings: HashMap<String, usize>,
    /// The enclosing effect rule's receipt family.
    receipt_name: String,
}

impl<'w> WalkCtx<'w> {
    /// The child context for a non-value position (pipe source, join left):
    /// same scope, no sink.
    fn without_sink(&self) -> WalkCtx<'w> {
        let mut c = self.clone();
        c.sink = None;
        c
    }

    /// One pure statement of THIS body, carrying no block: the body's claims
    /// and definitions are declared on the world for the body's whole
    /// extent, so a statement re-declares nothing. Its relation bindings are
    /// resolved by the standing-block entrance, against the world as this
    /// statement finds it.
    fn pure_query(&self, body: Chain) -> Query {
        Query::binding(crate::pipeline::asts::core::QueryLocals::none(), body)
    }
}

/// WHAT A CLAUSE WALK CARRIES THAT IS NOT A WORLD: the enclosing guards,
/// the bound higher-order inputs, and the receipt family. An invocation is
/// given these and supplies the world itself, so no arrangement of calls
/// hands a body one world's facts and another world's declarations.
struct PlanFacts {
    guards: Vec<GuardSource>,
    bindings: HashMap<String, usize>,
    receipt_name: String,
}

impl PlanFacts {
    /// Stand these facts in a world. Called by the invocation that owns
    /// both the body and the world.
    fn standing_in<'w>(self, world: &'w EffectWorld, sink: Option<ReceiptSink>) -> WalkCtx<'w> {
        WalkCtx {
            world,
            guards: self.guards,
            sink,
            bindings: self.bindings,
            receipt_name: self.receipt_name,
        }
    }
}

/// The shared receipt table of a rule invocation.
#[derive(Clone)]
struct ReceiptSink {
    /// The receipt of the shared receipt table, minted at its allocation;
    /// a built-in rule value evaluated over it stands over this receipt.
    table: crate::relation::ScratchRow,
}

// ============================================================================
// The worlds an effect plan resolves in
// ============================================================================

/// THE WORLD AN EFFECT PLAN RESOLVES IN — the plan's own program world, or
/// the world an invocation built for the rule it invoked — reachable only
/// through the named operations below. The environment inside never
/// leaves: nothing outside this module can retain it, stack it, or pair it
/// with another plan.
struct EffectWorld {
    world: std::cell::RefCell<Environment>,
}

impl EffectWorld {
    fn of(world: Environment) -> Self {
        EffectWorld {
            world: std::cell::RefCell::new(world),
        }
    }

    /// The world's cell, for a declaration or body-frame lease over it.
    fn cell(&self) -> &std::cell::RefCell<Environment> {
        &self.world
    }

    /// The plan's OWN session world — the scope of statements standing
    /// outside every rule body, rooted at the plan's namespace (`home` for
    /// an ad-hoc statement).
    fn program(system: &DelightQLSystem, root_fq: &str) -> Result<Self> {
        let consult = ConsultRegistry::new_with_system(system);
        Ok(Self::of(Environment::Use(UseEnvironment::session(
            &consult, root_fq,
        )?)))
    }

    /// THE ONE QUERY-LOCAL SELECTION at an effect demand site. The world
    /// answers from the block declared on it — an effect walk holds no
    /// collection of its own to search and no ledger to consult beside one
    /// — and what it answers LEAVES BOUND TO THIS WORLD: the label's arms
    /// and the mirror CHOE alike are spent in the declarations they were
    /// selected from, because the value that carries them carries the
    /// world too.
    fn select_effect_local(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Result<Option<EffectDemand<'_>>> {
        use crate::defuse::environment::QueryLocalSelection;
        let selected = self.world.borrow().select_query_local(
            name,
            crate::pipeline::asts::core::QueryLocalDemand::Effect,
            None,
        )?;
        Ok(match selected {
            Some(QueryLocalSelection::EffectRelation(arms)) => {
                Some(EffectDemand::Label(BoundArms { arms, world: self }))
            }
            Some(QueryLocalSelection::EffectHigherOrder(definition)) => {
                Some(EffectDemand::Mirror(BoundMirror {
                    definition,
                    world: self,
                }))
            }
            Some(_) => None,
            None => None,
        })
    }

    /// Register a relation an earlier statement of the plan created. A
    /// PROGRAM world reads it as its own state; a consulted body world
    /// reads a plan creation only through an explicit actual, never
    /// ambiently, so the registration lands nowhere there.
    fn register_materialized(
        &self,
        name: delightql_types::SqlIdentifier,
        relation: crate::relation::SemanticRelation,
    ) {
        if let Environment::Use(world) = &mut *self.world.borrow_mut() {
            world.register_materialized(name, relation);
        }
    }

    /// RESOLVE ONE STATEMENT OF THIS PLAN, inside the block already
    /// standing on this world. The statement arrives as its own body; the
    /// relation bindings it resolves under are the standing block's, taken
    /// from the frame that declared them — so no walk carries a block, and
    /// no statement re-declares one.
    fn resolve_query(
        &self,
        core: &mut ResolverCore<'_>,
        config: crate::pipeline::resolver::ResolutionConfig,
        query: ast_unresolved::Query,
    ) -> Result<crate::pipeline::resolver::ResolvedQuery> {
        let mut world = self.world.borrow_mut();
        let query = world.restate_with_standing_bindings(query)?;
        let mut fold = ResolverFold::new(core, &mut world, config);
        crate::pipeline::resolver::resolve_statement_in_standing_block(&mut fold, query)
    }

    /// Resolve a compiler-built application of one already-closed rule value.
    /// The opaque id enters through the same formal inventory and residual
    /// spending road as an authored higher-order body; callers provide only
    /// the synthetic formal spelling used by their compiler-built query.
    fn resolve_query_with_rule_value(
        &self,
        core: &mut ResolverCore<'_>,
        config: crate::pipeline::resolver::ResolutionConfig,
        query: ast_unresolved::Query,
        formal: delightql_types::SqlIdentifier,
        value: crate::defuse::ho::RuleValueId,
    ) -> Result<crate::pipeline::resolver::ResolvedQuery> {
        let mut inventory =
            FormalInventory::declared(std::iter::once((formal.clone(), FormalRole::Rule)));
        inventory.bind_rule_named(&formal, value)?;
        let mut world = self.world.borrow_mut();
        // A rule application is one statement of this plan like any other:
        // it stands in the block already declared here.
        let query = world.restate_with_standing_bindings(query)?;
        let mut lease = world.instantiated(inventory.sealed());
        let mut fold = ResolverFold::new(core, lease.world(), config);
        crate::pipeline::resolver::resolve_statement_in_standing_block(&mut fold, query)
    }

    /// Resolve a demand site's row-free argument values in this world —
    /// the CALLER's actuals, resolved before the callee is admitted.
    fn resolve_values(
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
    fn close_rule_value(
        &self,
        core: &mut ResolverCore<'_>,
        config: crate::pipeline::resolver::ResolutionConfig,
        designator: &ast_unresolved::Chain,
        expected: &crate::pipeline::asts::core::definitions::ResidualSignature,
        evaluation_relation: Option<crate::relation::ScratchRow>,
    ) -> Result<crate::defuse::ho::RuleValueId> {
        let mut world = self.world.borrow_mut();
        // The designator stands in the block declared on this world, so the
        // relation bindings it may read are resolved here and lead the
        // residual — the same act every statement of this body performs.
        let bindings = world.standing_bindings()?;
        let mut fold = ResolverFold::new(core, &mut world, config);
        let leading_ctes = if bindings.is_empty() {
            Vec::new()
        } else {
            crate::pipeline::bindings::resolve_cte_bindings(bindings, &mut fold)?
        };
        crate::defuse::carriers::construct_effect_residual(
            designator,
            expected,
            &mut fold,
            evaluation_relation,
            leading_ctes,
        )
    }

    /// Select one effect family from this world's captured reach. Qualified
    /// and enlisted demands share the catalog's exhaustive selection and
    /// kind judgment; the returned use carries the exact selected family.
    fn select_effect_rule<'s>(
        &self,
        system: &'s DelightQLSystem,
        namespace: Option<&str>,
        name: &str,
        stropped: bool,
    ) -> Result<Option<EffectUse<'s>>> {
        use crate::defuse::select::{judge_position, PositionOutcome, Selected};
        let consult = ConsultRegistry::new_with_system(system);
        let world = self.world.borrow();
        let selected = match namespace {
            Some(namespace) => consult
                .select_entity(name, stropped, namespace, world.reach())?
                .unique_or_refuse(name)?,
            None => match judge_position(
                name,
                consult.select_enlisted(name, stropped, world.reach())?,
                |kind| kind == crate::enums::EntityType::DqlEffectRule,
            )? {
                PositionOutcome::Selected(selected) => Some(selected),
                _ => None,
            },
        };
        match selected {
            Some(Selected::Authored(family))
                if family.kind() == crate::enums::EntityType::DqlEffectRule =>
            {
                Ok(Some(EffectUse { family }))
            }
            _ => Ok(None),
        }
    }
}

// ============================================================================
// Selection at a demand site
// ============================================================================

/// WHAT A QUERY-LOCAL EFFECT DEMAND SELECTED — a label's arms or the
/// query's own effect-mirror CHOE — each BOUND to the world it was selected
/// from. Selection produced both halves, so spending cannot pair the arms
/// or the definition of one world with the declarations of another.
enum EffectDemand<'w> {
    Label(BoundArms<'w>),
    Mirror(BoundMirror<'w>),
}

/// AN EFFECT LABEL'S ARMS AND THE WORLD THEY WERE SELECTED FROM, as one
/// value with no accessor for either half and no constructor: it is made
/// only by [`EffectWorld::select_effect_local`], from that world's own
/// declarations.
struct BoundArms<'w> {
    arms: ScopedEffectArms,
    world: &'w EffectWorld,
}

impl BoundArms<'_> {
    /// WALK EVERY ARM, IN AUTHORED ORDER, each paired with this label's own
    /// declaration — the same value, spent the same way, as a CHOE clause.
    /// The arms are read off the carrier the parent authority stamped, here
    /// and nowhere else.
    fn walk(&self, facts: PlanFacts, walk: &mut EffectWalk<'_, '_>) -> Result<Vec<Chain>> {
        let facts = facts.standing_in(self.world, None);
        self.arms
            .arms
            .iter()
            .map(|arm| {
                ScopedEffectBody {
                    expression: arm.body().clone(),
                    facts: facts.clone(),
                    _standing: StandingDeclaration::Site(DeclaredArms::open(
                        self.world.cell(),
                        self.arms.site,
                    )),
                }
                .walk(walk)
            })
            .collect()
    }
}

/// THE QUERY'S OWN EFFECT-MIRROR CHOE, SELECTED, AND THE WORLD IT WAS
/// SELECTED FROM. The definition arrives on the carrier the parent
/// authority stamped with its declaration site; the world is the one whose
/// declarations answered the demand. Neither half can be replaced, read
/// out, or supplied: the value is made only by
/// [`EffectWorld::select_effect_local`] and spent only by [`Self::invoke`].
struct BoundMirror<'w> {
    definition: ScopedHo,
    world: &'w EffectWorld,
}

// ============================================================================
// The two invocation roads
// ============================================================================

/// WHAT AN EFFECT DEMAND SELECTED: the query's own effect-mirror CHOE, bound
/// to its world, or a consulted effect rule. Both invoke through one
/// entrance and compile through one operation.
enum EffectSelection<'s, 'w> {
    Consulted(EffectUse<'s>),
    Scoped(BoundMirror<'w>),
}

impl EffectSelection<'_, '_> {
    /// The rule's name, `!` included.
    fn rule_name(&self) -> delightql_types::SqlIdentifier {
        match self {
            EffectSelection::Consulted(consulted) => consulted.rule_name().clone(),
            EffectSelection::Scoped(scoped) => scoped.rule_name(),
        }
    }

    /// The exact declared row, reconstructed from the same selected family
    /// for consulted effects and read directly from a scoped definition.
    fn declared_params(&self) -> Result<Vec<HoParam>> {
        match self {
            EffectSelection::Consulted(consulted) => Ok(crate::ddl::reconstruct::group(
                consulted.family.definition(),
            )?
            .params()
            .to_vec()),
            EffectSelection::Scoped(scoped) => Ok(scoped.definition.params().to_vec()),
        }
    }

    /// INVOKE with the actuals the admitted application was spent into:
    /// values in declared order, closed rule values by formal, and the
    /// staged relation under the formal the admission paired it with.
    fn invoke(
        self,
        instances: &InstanceTable,
        actuals: EffectActuals,
        walk: &mut EffectWalk<'_, '_>,
        ctx: &WalkCtx<'_>,
        root: bool,
    ) -> Result<Chain> {
        match self {
            EffectSelection::Consulted(consulted) => {
                consulted.invoke(instances, actuals, walk, ctx, root)
            }
            EffectSelection::Scoped(scoped) => scoped.invoke(instances, actuals, walk, ctx),
        }
    }
}

/// One selected CONSULTED effect rule, NOT yet opened: invocation is the
/// use, and [`Self::invoke`] is its one entrance — the complete bound-use
/// transition (resolved actuals -> semantic key -> admission -> a fresh
/// body world with the parameter frame) with restoration owned
/// STRUCTURALLY by the entrance, never by a consumer convention. The body
/// is a catalog fact reconstructed under the admission; this value holds
/// no syntax.
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
    /// this operation DERIVES from the admission and owns for exactly its
    /// extent. The rule's syntax and its world are both read off the one
    /// bound use, so there is no pair to assemble.
    fn invoke(
        self,
        instances: &InstanceTable,
        actuals: EffectActuals,
        walk: &mut EffectWalk<'_, '_>,
        ctx: &WalkCtx<'_>,
        root: bool,
    ) -> Result<Chain> {
        let EffectUse { family } = self;
        let display_name = family.name().to_string();
        let input = actuals.input().cloned();
        let bound = require_fresh(bind_definition_use(instances, family, actuals)?, || {
            DelightQLError::from(Effect::TransformUnsupported {
                message: format!(
                    "effect rule '{display_name}' recursed during plan expansion (R6)"
                ),
            })
        })?;
        // BOTH HALVES FROM THE ONE ADMISSION: the rule is shaped from the
        // group the bound use reconstructs, and the world is the one the
        // bound use derives from its own declaration — a demanded PROGRAM
        // (root) is the plan's own use world rooted at the rule's
        // namespace, a rule invoked FROM a body opens a consulted body
        // world where only its formals cross.
        let group = bound.reconstruct_group()?;
        let rule = ShapedEffectRule::of(&group)?;
        let world = EffectWorld::of(bound.effect_world(&group, &display_name, root)?);
        let invoked = InvokedRule {
            rule,
            world: InvokedWorld::Owned(world),
            input,
            _held: HeldInvocation::Bound { _bound: bound },
        };
        walk.compile_invoked(&invoked, ctx)
    }
}

impl<'w> BoundMirror<'w> {
    /// The rule's name, `!` included.
    fn rule_name(&self) -> delightql_types::SqlIdentifier {
        delightql_types::SqlIdentifier::new(format!("{}!", self.definition.name().as_str()))
    }

    /// INVOKE THE QUERY'S OWN EFFECT CHOE at its own declaration, on the
    /// world it was selected from. The rule is shaped from the clauses the
    /// selected carrier holds — read through the parent authority's private
    /// field, here alone — and the body opens at the site that carrier was
    /// stamped with, on the world this value was bound to at selection. No
    /// caller supplies either, and nothing reads either back out.
    fn invoke(
        self,
        instances: &InstanceTable,
        actuals: EffectActuals,
        walk: &mut EffectWalk<'_, '_>,
        ctx: &WalkCtx<'_>,
    ) -> Result<Chain> {
        let display_name = self.rule_name().to_string();
        let BoundMirror { definition, world } = self;
        let input = actuals.input().cloned();
        let frame = match instances.admit_scoped(definition.name(), actuals.scoped_key()) {
            ScopedAdmission::Fresh(frame) => frame,
            ScopedAdmission::Reenter
            | ScopedAdmission::Cycle { .. }
            | ScopedAdmission::Widening { .. } => {
                return Err(choe_recursion_refusal(&display_name));
            }
        };
        let rule = ShapedEffectRule::of(definition.definition.group())?;
        let formals = actuals.formals(definition.params(), &display_name)?;
        // THE BODY OPENS AT ITS OWN DECLARATION, on the world the selection
        // was bound to — the site travels on the carrier, so this invocation
        // cannot open the body anywhere else, and what the body may see is
        // decided by the declaration standing here rather than by a block
        // rebuilt for the walk.
        let lease = SharedInstantiated::opened_body(world.cell(), formals, &definition);
        let invoked = InvokedRule {
            rule,
            world: InvokedWorld::Caller {
                world,
                _lease: lease,
            },
            input,
            _held: HeldInvocation::Scoped { _frame: frame },
        };
        walk.compile_invoked(&invoked, ctx)
    }
}

/// Use a name in EFFECT-RULE position: select the family in the demanded
/// namespace and judge the kind. The body does NOT open here — invocation
/// is the use, and it happens inside this module. `None` is a position
/// miss (no such rule in the namespace).
pub(crate) fn use_effect_rule<'s>(
    system: &'s DelightQLSystem,
    namespace: &str,
    rule_name: &str,
) -> Result<Option<EffectUse<'s>>> {
    use crate::defuse::select::Selected;
    let consult = ConsultRegistry::new_with_system(system);
    // The demanded namespace's own reach, captured for this selection.
    let reach = crate::defuse::environment::reach::capture(
        crate::defuse::CatalogRead::of(system),
        namespace,
        crate::defuse::environment::reach::World::Session,
    )?;
    let Some(Selected::Authored(family)) = consult
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

// ============================================================================
// The invoked rule and its paired clause bodies
// ============================================================================

/// ONE EFFECT RULE'S SHAPED SYNTAX, held where syntax is held. Its clauses
/// are reachable through no accessor: what it answers is METADATA — a name,
/// the clause count, each clause's ending disposition — and one paired body
/// per clause. Which formal the invocation's relation binds is not read
/// off a head here: the admission decided it, and the invoked rule carries
/// the answer.
///
/// A consulted rule's syntax is confined by the same value as a query-scoped
/// one's, so there is one shape to reason about rather than two.
struct ShapedEffectRule {
    rule: effects::EffectRule,
}

impl ShapedEffectRule {
    /// Assemble from one definition group. The heads are read for the clause
    /// count and each clause's ending disposition; the BODIES bind their
    /// parameter references through the frame and the bound input.
    fn of(group: &crate::pipeline::asts::ddl::DefinitionGroup) -> Result<Self> {
        Ok(ShapedEffectRule {
            rule: effects::EffectRule::from_definition_group(group)?,
        })
    }

    fn name(&self) -> &str {
        &self.rule.name
    }

    fn clause_count(&self) -> usize {
        self.rule.clauses.len()
    }

    /// EACH CLAUSE'S ENDING DISPOSITION — the receipt shape it produces and
    /// whether it sinks its own. Reading a disposition off a body is a
    /// judgment ABOUT the syntax; what crosses is the judgment.
    fn ending_kinds(&self) -> Vec<Option<(Vec<String>, bool)>> {
        self.rule
            .clauses
            .iter()
            .map(|clause| walk::ending_kind(&clause.body.expression))
            .collect()
    }

    /// ONE CLAUSE, PAIRED WITH THE WORLD THAT INTERPRETS IT: the clause's
    /// own block is declared on the facts' world for exactly the returned
    /// value's life.
    fn body_at<'a>(&self, index: usize, facts: WalkCtx<'a>) -> Result<ScopedEffectBody<'a>> {
        let clause = self.rule.clauses.get(index).ok_or_else(|| {
            crate::diagnostic::Internal::invariant(
                "defuse::effect",
                "an invoked rule is asked only for clauses it declares",
            )
        })?;
        let block = DeclaredBlock::open(facts.world.cell(), clause.body.locals.clone());
        Ok(ScopedEffectBody {
            expression: clause.body.expression.clone(),
            facts,
            _standing: StandingDeclaration::Block(block),
        })
    }
}

/// THE WORLD AN INVOKED RULE COMPILES IN: a consulted rule's own, derived by
/// the invocation from its admission; or — for the query's effect-mirror
/// CHOE — the world its selection was bound to, entered through a body
/// frame the invocation holds open for its extent.
enum InvokedWorld<'w> {
    Owned(EffectWorld),
    /// The selection's own world, held BY THIS INVOCATION for its extent
    /// together with the body-frame lease taken over it.
    Caller {
        world: &'w EffectWorld,
        _lease: SharedInstantiated<'w>,
    },
}

/// The admission an invocation holds while its clauses compile — held for
/// its extent, read by nothing.
enum HeldInvocation<'s> {
    Bound { _bound: BoundUse<'s, EffectActuals> },
    Scoped { _frame: InstanceFrame },
}

/// ONE INVOKED EFFECT RULE — the shaped rule, the world its statements
/// resolve in, and the admission held while they compile, as one value
/// with private fields and no constructor. It is made by struct literal at
/// exactly two sites, [`EffectUse::invoke`] and [`BoundMirror::invoke`],
/// each of which derives the rule and the world from the one value it
/// consumed. The walk compiles it through the operations below, which
/// answer METADATA and one PAIRED clause body at a time.
struct InvokedRule<'s, 'w> {
    rule: ShapedEffectRule,
    world: InvokedWorld<'w>,
    /// THE RELATION THE INVOCATION BINDS, under the formal the admission
    /// paired it with — read off the actuals the invocation consumed, so the
    /// body's read of that formal and the caller's placement are one fact.
    input: Option<EffectInput>,
    _held: HeldInvocation<'s>,
}

impl InvokedRule<'_, '_> {
    /// The rule's name — a family fact for receipts and diagnostics.
    fn name(&self) -> &str {
        self.rule.name()
    }

    /// The staged relation and the formal it binds, when the invocation
    /// supplied one.
    fn input(&self) -> Option<&EffectInput> {
        self.input.as_ref()
    }

    fn clause_count(&self) -> usize {
        self.rule.clause_count()
    }

    /// EACH CLAUSE'S ENDING DISPOSITION — the receipt shape it produces and
    /// whether it sinks its own.
    fn ending_kinds(&self) -> Vec<Option<(Vec<String>, bool)>> {
        self.rule.ending_kinds()
    }

    /// WALK ONE CLAUSE. Both halves come from `self`: the clause's syntax
    /// from the rule this invocation shaped, the world from the one it
    /// derived. What the caller brings is the walk's own state — guards,
    /// bound inputs, a receipt family — and no world at all.
    ///
    /// A consulted rule's clause stands in the rule's own world; an effect
    /// CHOE's stands in the world its selection was bound to, where its
    /// declaration is already open — the lease this invocation holds put it
    /// there, at the site the selected definition carries. So the nesting is
    /// STRUCTURAL on both roads: the clause's own block is a frame above the
    /// site's, an enclosing frame is admitted only as far as the open
    /// declaration's horizon reaches, and a spelling the clause declares
    /// shadows the site's by standing nearer.
    fn walk_clause(
        &self,
        index: usize,
        sink: Option<ReceiptSink>,
        facts: PlanFacts,
        walk: &mut EffectWalk<'_, '_>,
    ) -> Result<Chain> {
        let world = match &self.world {
            InvokedWorld::Owned(world) => world,
            InvokedWorld::Caller { world, .. } => world,
        };
        self.rule
            .body_at(index, facts.standing_in(world, sink))?
            .walk(walk)
    }
}

/// ONE QUERY-SCOPED EFFECT BODY, INSEPARABLE FROM ITS WORLD: unresolved
/// syntax, the walk facts standing in the world its declaration owns, and
/// that declaration held open for exactly this value's life. A CHOE clause
/// and an effect-CTE arm are the same value here — one law, one spending —
/// and the spending is this module's own walk, which reads the syntax in
/// the facts it arrived paired with and hands nothing outward.
struct ScopedEffectBody<'a> {
    expression: Chain,
    facts: WalkCtx<'a>,
    _standing: StandingDeclaration<'a>,
}

/// The declaration a scoped effect body stands under: a clause's own block,
/// or the site an effect label's arms were declared at. Held, never read —
/// what it does it does by standing, and by closing when this value dies.
enum StandingDeclaration<'a> {
    Block(#[allow(dead_code)] DeclaredBlock<'a>),
    Site(#[allow(dead_code)] DeclaredArms<'a>),
}

impl ScopedEffectBody<'_> {
    /// SPEND THE BODY in the world it arrived paired with.
    fn walk(self, walk: &mut EffectWalk<'_, '_>) -> Result<Chain> {
        walk.walk_value(self.expression, &self.facts)
    }
}
