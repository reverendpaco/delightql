// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `semantic/effect/…` — the receipt algebra's discipline.

use super::Semantic;
use crate::diagnostic::{DelightQLError, Taxon};

/// `semantic/effect/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect))]
pub enum Effect {
    /// An effect rule's argument position expected a table and received
    /// something else.
    #[leaf("arguments/not_a_table", class = Syntax, summary = "An effect argument is not a table.")]
    #[error("Validation error: {message}")]
    ArgumentsNotATable { message: String },

    /// Bin executables (the built-in pseudo-predicates) and the arguments
    /// they consume.
    #[family(
        "bin",
        summary = "A bin executable received an argument it cannot consume."
    )]
    #[error(transparent)]
    Bin(EffectBin),

    /// What an effect body may contain.
    #[family("body", summary = "An effect body contains something it may not.")]
    #[error(transparent)]
    Body(EffectBody),

    /// A pure position (a relational rule, a witness, a CFE) demanded an
    /// effect during compilation.
    #[leaf("compile/purity", class = Syntax, summary = "A pure position demanded an effect.")]
    #[error("Validation error: {message}")]
    CompilePurity { message: String },

    /// Query-scoped bindings under an effect head: their labels and marks.
    #[family("cte", summary = "An effect binding's label or mark is wrong.")]
    #[error(transparent)]
    Cte(EffectCte),

    /// DDL effects: durable creation and its target namespace.
    #[family("ddl", summary = "A DDL effect cannot be realized as written.")]
    #[error(transparent)]
    Ddl(EffectDdl),

    /// A directive parameter received an argument that carries no value.
    #[leaf("directive/valueless_argument", class = Syntax, summary = "A directive argument carries no value.")]
    #[error("Validation error: {message}")]
    DirectiveValuelessArgument { message: String },

    /// A DML effect's target designator does not name a mutable relation
    /// in the effect's world.
    #[leaf("dml/target_designator", class = Syntax, summary = "A DML effect's target designator is not a mutable relation.")]
    #[error("Validation error: {message}")]
    DmlTargetDesignator { message: String },

    /// A directive that demands a landing was invoked with nothing piped
    /// into it and no landing named.
    #[leaf("landing/nowhere", class = Syntax, summary = "A directive's input landed nowhere.")]
    #[error("Validation error: {message}")]
    LandingNowhere { message: String },

    /// An observing session — one opened to inspect, fingerprint or hash a
    /// result — was handed a statement that would execute an effect. The
    /// refusal is made before any dispatcher runs.
    #[leaf("observation", class = Permission, summary = "An observation demanded an effect.")]
    #[error("Validation error: {message}")]
    Observation { message: String },

    /// One ledger release mixes `!>` payload reads of different receipt
    /// shapes.
    #[leaf("ledger/mixed_release", class = Syntax, summary = "A ledger release mixes receipt shapes.")]
    #[error("Validation error: {message}")]
    LedgerMixedRelease { message: String },

    /// A namespace's `main!` entry: one per namespace, one clause.
    #[family("main", summary = "A namespace's main! is ill-declared.")]
    #[error(transparent)]
    Main(EffectMain),

    /// How an effect lands from a pipe.
    #[family("pipe", summary = "An effect's pipe landing is ill-formed.")]
    #[error(transparent)]
    Pipe(EffectPipe),

    /// What one plan may span and which engines can run it.
    #[family("plan", summary = "The effect plan cannot be realized.")]
    #[error(transparent)]
    Plan(EffectPlan),

    /// An effect stood in a relational position where only a relation
    /// derives.
    #[leaf("position", class = Syntax, summary = "An effect stood in a relational position.")]
    #[error("Validation error: {message}")]
    Position { message: String },

    /// A predicate form the effect transformer cannot lower.
    #[leaf("predicate/unsupported", class = Syntax, summary = "A predicate form has no effect lowering.")]
    #[error("Validation error: {message}")]
    PredicateUnsupported { message: String },

    /// The directive's realization (syntax terminal, entity, bin) does not
    /// admit the context it was invoked in.
    #[leaf("realization/context", class = Syntax, summary = "A directive's realization does not admit this context.")]
    #[error("Validation error: {message}")]
    RealizationContext { message: String },

    /// Effect rules: their parameters, purity, ending, and recursion.
    #[family("rule", summary = "An effect rule is ill-declared.")]
    #[error(transparent)]
    Rule(EffectRule),

    /// run! and run_namespace!: where they stand and how their receipts
    /// are read.
    #[family("run", summary = "A run directive was misused.")]
    #[error(transparent)]
    Run(EffectRun),

    /// A runtime-served definition reaches itself through the entities it
    /// serves: a cycle the pre-splice cannot expand.
    #[leaf("runtime_served/cycle", class = Syntax, summary = "A runtime-served definition cycles.")]
    #[error("Validation error: {message}")]
    RuntimeServedCycle { message: String },

    /// A session directive stood inside an effect body; session directives
    /// change what a compilation can see and are demanded at the top level
    /// or in a liminal program.
    #[leaf("session/position", class = Syntax, summary = "A session directive stood inside an effect body.")]
    #[error("Validation error: {message}")]
    SessionPosition { message: String },

    /// The target engine owns this operation and DelightQL will not
    /// re-spell it: the effect is refused so the engine's own verb is used.
    #[leaf("target/engine_owned", class = Syntax, summary = "The target engine owns this operation.")]
    #[error("Validation error: {message}")]
    TargetEngineOwned { message: String },

    /// The effect transformer takes no position on this form yet.
    #[leaf("transform/unsupported", class = Syntax, summary = "The effect transformer cannot lower this form.")]
    #[error("Validation error: {message}")]
    TransformUnsupported { message: String },
}

/// `semantic/effect/bin/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Bin))]
pub enum EffectBin {
    /// Bin executables consume scalar arguments in this call shape. A table
    /// argument cannot be discarded or shift the later arguments; pass
    /// scalar expressions in the first parentheses.
    #[leaf("table_argument", class = Syntax, summary = "A bin executable received a table-valued argument.")]
    #[error("Validation error: {message}")]
    TableArgument { message: String },

    /// A bin executable's parameter received an argument that carries no
    /// value (a glob, a skip, a context marker).
    #[leaf("valueless_argument", class = Syntax, summary = "A bin executable received a valueless argument.")]
    #[error("Validation error: {message}")]
    ValuelessArgument { message: String },
}

/// `semantic/effect/body/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Body))]
pub enum EffectBody {
    /// run! consults and cannot execute inside a compiled effect body
    /// (EFFECT-ALGEBRA R9): consult before the run, then demand with
    /// run_namespace!.
    #[leaf("run", class = Syntax, summary = "run! stood inside a compiled effect body.")]
    #[error("Validation error: {message}")]
    Run { message: String },

    /// A session directive stood inside an effect body.
    #[leaf("session_directive", class = Syntax, summary = "A session directive stood inside an effect body.")]
    #[error("Validation error: {message}")]
    SessionDirective { message: String },
}

/// `semantic/effect/cte/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Cte))]
pub enum EffectCte {
    /// The pure mark on a binding whose body demands an effect.
    #[leaf("pure_mark", class = Syntax, summary = "A pure mark on an effectful binding.")]
    #[error("Validation error: {message}")]
    PureMark { message: String },
}

/// `semantic/effect/ddl/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Ddl))]
pub enum EffectDdl {
    /// A DDL effect's target is a whole-table designator — `name(*)`,
    /// optionally namespace-qualified — naming where to create; filters,
    /// projections, and derived relations do not belong in a target.
    #[leaf("target_designator", class = Syntax, summary = "A DDL effect's target designator is not a designator.")]
    #[error("Validation error: {message}")]
    TargetDesignator { message: String },

    /// A run created an object the session catalog cannot register on this
    /// connection kind.
    #[leaf("created_object_registration_unsupported", class = Syntax, summary = "A created object cannot be registered on this connection.")]
    #[error("Validation error: {message}")]
    CreatedObjectRegistrationUnsupported { message: String },

    /// A durable creation names an object that already exists in the
    /// target, and the effect does not replace.
    #[leaf("durable_clash", class = Syntax, summary = "A durable creation clashes with an existing object.")]
    #[error("Validation error: {message}")]
    DurableClash { message: String },

    /// A durable creation names a schema the target does not have.
    #[leaf("durable_schema_unknown", class = Syntax, summary = "A durable creation names an unknown schema.")]
    #[error("Validation error: {message}")]
    DurableSchemaUnknown { message: String },

    /// A DDL effect's target namespace is not one a creation may write
    /// into.
    #[leaf("target_namespace", class = Syntax, summary = "A DDL effect's target namespace is not writable.")]
    #[error("Validation error: {message}")]
    TargetNamespace { message: String },

    /// A session creation's physical temp name is held on the same
    /// connection for another durable namespace: one connection has one
    /// temp name pool.
    #[leaf("temp_name_held", class = Syntax, summary = "A session creation's temp name is held for another namespace on its connection.")]
    #[error("Validation error: {message}")]
    TempNameHeld { message: String },

    /// A creation would leave a durable object and a session object of one
    /// name on a connection whose engine cannot spell the durable object
    /// past the session one, so an exact read of it could never be served.
    #[leaf("unaddressable_durable", class = Syntax, summary = "A creation would leave a durable object its engine cannot address past a same-named session object.")]
    #[error("Validation error: {message}")]
    UnaddressableDurable { message: String },
}

/// `semantic/effect/main/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Main))]
pub enum EffectMain {
    /// A namespace declares main! more than once.
    #[leaf("duplicate", class = Syntax, summary = "main! is declared twice.")]
    #[error("Validation error: {message}")]
    Duplicate { message: String },

    /// main! has one clause; a second clause is a second entry.
    #[leaf("multi_clause", class = Syntax, summary = "main! has more than one clause.")]
    #[error("Validation error: {message}")]
    MultiClause { message: String },
}

/// `semantic/effect/pipe/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Pipe))]
pub enum EffectPipe {
    /// A lifted effect application (`f!(rows & …)`) has no lowering yet.
    #[leaf("lifted_not_yet", class = Syntax, summary = "A lifted effect application is not yet supported.")]
    #[error("Validation error: {message}")]
    LiftedNotYet { message: String },
}

/// `semantic/effect/plan/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Plan))]
pub enum EffectPlan {
    /// One plan runs in one transaction on one connection; its steps route
    /// to more than one.
    #[leaf("cross_connection", class = Syntax, summary = "An effect plan spans connections.")]
    #[error("Validation error: {message}")]
    CrossConnection { message: String },

    /// The routed engine cannot run this plan's shape.
    #[leaf("engine_unsupported", class = Syntax, summary = "The engine cannot run this effect plan.")]
    #[error("Validation error: {message}")]
    EngineUnsupported { message: String },
}

/// `semantic/effect/rule/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Rule))]
pub enum EffectRule {
    /// An effect rule's arguments do not match its declared parameters in
    /// kind.
    #[leaf("arguments", class = Syntax, summary = "An effect rule's arguments do not match its parameters.")]
    #[error("Validation error: {message}")]
    Arguments { message: String },

    /// An effect rule must end in a terminal or a demanded effect; this
    /// one ends in a relation nothing demands.
    #[leaf("ending", class = Syntax, summary = "An effect rule does not end in an effect.")]
    #[error("Validation error: {message}")]
    Ending { message: String },

    /// A namespace may not hold both a functor `foo` and an effect `foo!`.
    #[leaf("name_collision", class = Syntax, summary = "An effect rule's name collides with a functor's.")]
    #[error("Validation error: {message}")]
    NameCollision { message: String },

    /// An effect rule reaches itself; effect rules do not recurse.
    #[leaf("recursion", class = Syntax, summary = "An effect rule recurses.")]
    #[error("Validation error: {message}")]
    Recursion { message: String },
}

/// `semantic/effect/run/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Effect, Effect::Run))]
pub enum EffectRun {
    /// run!/run_namespace! stand at the top level or as a body's own
    /// invocation, never in the position written.
    #[leaf("position", class = Syntax, summary = "A run directive stood in a position it is not admitted in.")]
    #[error("Validation error: {message}")]
    Position { message: String },

    /// The namespace a run demands declares no `main!` (or the named
    /// effect rule) to demand: consult a file that defines it into the
    /// namespace first (EFFECT-ALGEBRA F3).
    #[leaf("no_main", class = Syntax, summary = "The demanded namespace has no main! to run.")]
    #[error("Validation error: {message}")]
    NoMain { message: String },

    /// A run receipt's heading is (success, operation, <echo>, returned) —
    /// the binding list is exact-arity; glob access `(*)` dumps the payload
    /// instead (EFFECT-ALGEBRA F5).
    #[leaf("receipt_access", class = Syntax, summary = "A run receipt was accessed with the wrong arity.")]
    #[error("Validation error: {message}")]
    ReceiptAccess { message: String },
}
