// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE PIPE FORM INVENTORY AND THE ONE CROSSING, load-bearing.
//!
//! `SEMANTICS/fundamentals.md` settles the domain: a PIPE FORM is exactly
//! one of a PIPE OPERATOR, a CALL, or a REDUCTION; every PIPE FORM is
//! SCOPE-DEQUALIFYING; and every SCOPE BARRIER hides the scope that stood
//! before it, so only what the form republishes crosses. This module
//! states that closed inventory over the NORMALIZED carriers — not the
//! relation authority's forms — and answers, exhaustively, how each
//! member's output crosses the stage publication:
//!
//! - most members' derivations OWN the crossing — the relation-authority
//!   form they resolve through takes the pipe-stage boundary, so every
//!   port they publish was dequalified by the one boundary act;
//! - a CALL owns no single derivation (a definition body inlines under its
//!   own forms), so the crossing appends the stage export over the
//!   completed result.
//!
//! The module also owns the CARRIERS that make the crossing unavoidable.
//! [`Standing`] is the relation standing here together with what answers
//! over it; [`ResolvedStep`] seals a run step's member, payload and
//! authored answer as one; [`CallOutcome`] does the same for the call
//! road and answers the landing question itself. A crossing consumes a
//! [`Standing`] and answers with the next one, so a run never holds a
//! crossed relation beside a scope that did not come from that crossing —
//! and a [`ResolvedRelation`]'s frontier has no `Clone`, so a scope the
//! barrier ended is gone rather than merely unused.
//!
//! A new normalized pipe-form member does not compile until the
//! declaration below states it (which is also what puts it in
//! [`PipeForm::ALL`], so the covering receipt cannot miss it) AND
//! [`crossing`] says how its output crosses.

use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::resolver::ResolvedRelation;

/// THE ONE DECLARATION. The types and the enumeration are generated from
/// the same rows in the same order, so membership and iteration cannot
/// disagree: a member that exists is a member the walk visits.
macro_rules! declare_pipe_forms {
    (
        operators: $($(#[doc = $operator_doc:expr])* $operator:ident),+ $(,)? ;
        others: $($(#[doc = $other_doc:expr])* $other:ident),+ $(,)? ;
    ) => {
        /// The exhaustive PIPE OPERATORS, in fundamentals' own vocabulary.
        /// The narrowing family has two members because the columns they
        /// pair are found two different ways — the access carries context,
        /// the destructure does not — and a receipt over one is not a
        /// receipt over the other.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(super) enum PipeOperator { $($(#[doc = $operator_doc])* $operator),+ }

        /// One member of fundamentals' exhaustive alternatives.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(super) enum PipeForm {
            /// A PIPE OPERATOR — the twelve above, both narrowing carriers
            /// among them.
            Operator(PipeOperator),
            $($(#[doc = $other_doc])* $other),+
        }

        impl PipeForm {
            /// THE COMPLETE INVENTORY, in declaration order. Written by
            /// the declaration that admits each member, never beside it:
            /// a member cannot be added to the language's alternatives and
            /// left out of what iterates them.
            ///
            /// Production never walks the inventory — it dispatches on the
            /// carrier in front of it — so this exists for the covering
            /// receipt, which is the only thing that must visit every
            /// member.
            #[cfg(test)]
            pub(super) const ALL: &'static [PipeForm] = &[
                $(PipeForm::Operator(PipeOperator::$operator),)+
                $(PipeForm::$other,)+
            ];
        }
    };
}

declare_pipe_forms! {
    operators:
        Project,
        ProjectOut,
        Rename,
        Embed,
        MapCover,
        EmbedMapCover,
        Transform,
        Group,
        Ordering,
        Reposition,
        NarrowingAccess,
        NarrowingDestructure;
    others:
        /// `|> f(...)` — a higher-order target consuming the piped
        /// relation by substitution into its one open parameter.
        Call,
        /// `~> f:(...)` — one row and one column per group.
        Reduction;
}

/// WHAT A COMPLETED STEP IS at the scope boundary it raises. Total, and
/// never absent: [`cross`] takes this and no option, so reaching a step's
/// far side means having said which kind of boundary it was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StepForm {
    /// One of fundamentals' PIPE FORMS: SCOPE-REPLACING and
    /// SCOPE-DEQUALIFYING both.
    Pipe(PipeForm),
    /// A step that raises the barrier without being a pipe form — WITNESS,
    /// METAIZE, SIGNED WITNESS (fundamentals lists them beside the pipe
    /// operators, not among them). It publishes its own scope and takes no
    /// pipe-stage dequalification.
    Replacing,
    /// A run's whole or dequalifying access: `t(*)` asks for every
    /// dimension the relation already published and for nothing else, so
    /// what answered over the relation still answers over the read.
    Preserving,
    /// A run's argumentative access: the slots bind bare and activate no
    /// name — neither the source's qualifier nor the definition's reaches
    /// the positions it publishes.
    Argumentative,
}

/// How a pipe form's output crosses the stage publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Crossing {
    /// The member's own relation-authority derivation takes the pipe-stage
    /// boundary: every port it publishes crossed the one boundary act.
    DerivationOwned,
    /// No single derivation owns the completed result; the crossing
    /// appends the stage export over it.
    StageExport,
    /// The drill KEEPS its context and opens the interior under the nest
    /// name. The narrowing's own dequalification is the projection of that
    /// nest's glob the normalizer writes after it — a PROJECT crossing —
    /// so the drilled read itself ends no scope.
    Drilling,
}

impl StepForm {
    /// The normalized pipe-operator carrier's member, exhaustively. The
    /// reduction spelling normalizes into the group carrier, so the group
    /// operator is where the REDUCTION alternative is told apart.
    pub(super) fn of_operator(operator: &ast_unresolved::PipeOp) -> StepForm {
        use crate::pipeline::asts::unresolved::PipeOp;
        StepForm::Pipe(match operator {
            PipeOp::Project(_) => PipeForm::Operator(PipeOperator::Project),
            PipeOp::ProjectOut(_) => PipeForm::Operator(PipeOperator::ProjectOut),
            PipeOp::Rename(_) => PipeForm::Operator(PipeOperator::Rename),
            PipeOp::Embed(_) => PipeForm::Operator(PipeOperator::Embed),
            PipeOp::MapCover(_) => PipeForm::Operator(PipeOperator::MapCover),
            PipeOp::EmbedMapCover(_) => PipeForm::Operator(PipeOperator::EmbedMapCover),
            PipeOp::Transform { .. } => PipeForm::Operator(PipeOperator::Transform),
            PipeOp::Group(crate::pipeline::asts::core::GroupSpec::Distinct { .. }) => {
                PipeForm::Operator(PipeOperator::Group)
            }
            PipeOp::Group(crate::pipeline::asts::core::GroupSpec::Reduce { .. }) => {
                PipeForm::Reduction
            }
        })
    }

    /// The structural family, exhaustively. Two of its members are pipe
    /// operators and two more are the narrowing pair; the rest raise the
    /// barrier without being pipe forms.
    pub(super) fn of_structural(form: &ast_unresolved::StructuralForm) -> StepForm {
        use crate::pipeline::asts::unresolved::StructuralForm;
        match form {
            StructuralForm::Ordering { .. } => {
                StepForm::Pipe(PipeForm::Operator(PipeOperator::Ordering))
            }
            StructuralForm::Reposition { .. } => {
                StepForm::Pipe(PipeForm::Operator(PipeOperator::Reposition))
            }
            StructuralForm::Drill { .. } => {
                StepForm::Pipe(PipeForm::Operator(PipeOperator::NarrowingAccess))
            }
            StructuralForm::Narrow { .. } => {
                StepForm::Pipe(PipeForm::Operator(PipeOperator::NarrowingDestructure))
            }
            StructuralForm::Meta
            | StructuralForm::Witness { .. }
            | StructuralForm::SignedWitness => StepForm::Replacing,
        }
    }

    /// A run's schema-free access, exhaustively. None of these is a PIPE
    /// FORM: they answer for the dimensions a relation already published
    /// rather than following a PIPE SIGIL. A whole or dequalifying read
    /// keeps what answered over the relation; a slot row is argumentative.
    pub(super) fn of_access(access: &ast_unresolved::Access) -> StepForm {
        use crate::pipeline::asts::unresolved::Access;
        match access {
            Access::Unasked | Access::All | Access::Dequalify(_) | Access::DequalifyAll => {
                StepForm::Preserving
            }
            Access::Slots(_) => StepForm::Argumentative,
        }
    }
}

/// THE ONE ANSWER per member. Total and wildcard-free: a member with no
/// stated crossing does not compile, and the two crossings — the only
/// consumers — match it totally, so there is no road on which a pipe form
/// resolves and its output does not cross the stage publication.
fn crossing(form: PipeForm) -> Crossing {
    match form {
        // Each operator resolves through a relation-authority form the
        // boundary judgment classifies as a pipe stage: Project and Embed
        // through `RelForm::Project`/`Embed` (stating `ProjectWhy::Stage`),
        // Rename through `RelForm::Rename`, ProjectOut/Reposition through
        // their edits, the covers through `RelForm::Cover`, Group and the
        // Reduction through `RelForm::Group`, the Ordering through
        // `ExportWhy::Stage`, and the narrowing pair through
        // `RelForm::Drill` and `RelForm::Narrow`.
        PipeForm::Operator(
            PipeOperator::Project
            | PipeOperator::ProjectOut
            | PipeOperator::Rename
            | PipeOperator::Embed
            | PipeOperator::MapCover
            | PipeOperator::EmbedMapCover
            | PipeOperator::Transform
            | PipeOperator::Group
            | PipeOperator::Ordering
            | PipeOperator::Reposition
            | PipeOperator::NarrowingDestructure,
        )
        | PipeForm::Reduction => Crossing::DerivationOwned,
        PipeForm::Operator(PipeOperator::NarrowingAccess) => Crossing::Drilling,
        PipeForm::Call => Crossing::StageExport,
    }
}

/// ONE RUN STEP, RESOLVED AND SEALED: the normalized member it is, the
/// step its resolution wrote, and the answer authored at its position.
///
/// All three come off ONE authored node, inside [`ResolvedStep::of`], and
/// the fields are private — so no caller pairs a member with another
/// step's payload or attaches a name written somewhere else.
pub(super) struct ResolvedStep {
    form: StepForm,
    step: ast_resolved::Step,
    answer: Option<delightql_types::SqlIdentifier>,
}

/// THE PAYLOAD a run step's resolution receives: the authored form with
/// its stage answer already taken off, so the resolution cannot read a
/// name the sealing did not see, and cannot answer for a member the
/// sealing did not classify.
pub(super) enum StepBody {
    Pipe(ast_unresolved::PipeOp),
    Structural(ast_unresolved::StructuralStep),
    Access(ast_unresolved::Access),
}

impl ResolvedStep {
    /// SEAL ONE RUN STEP. The authored form goes in WHOLE; its member is
    /// read off the same node its payload is taken from, and its answer
    /// off the same node again. `resolve` receives only the payload, so
    /// the three facts cannot come from three places.
    pub(super) fn of(
        form: ast_unresolved::RunForm,
        resolve: impl FnOnce(StepBody) -> Result<ast_resolved::Step>,
    ) -> Result<ResolvedStep> {
        let (member, body, answer) = match form {
            ast_unresolved::RunForm::Pipe { operator, named } => (
                StepForm::of_operator(&operator),
                StepBody::Pipe(operator),
                named,
            ),
            ast_unresolved::RunForm::Structural(mut step) => {
                // The stage name is the POSITION's, not the form's: it is
                // taken off here and spent at the crossing, exactly as an
                // operator's is.
                let answer = step.named.take();
                (
                    StepForm::of_structural(&step.form),
                    StepBody::Structural(step),
                    answer,
                )
            }
            ast_unresolved::RunForm::Access { access, named } => (
                StepForm::of_access(&access),
                StepBody::Access(access),
                named,
            ),
        };
        let step = resolve(body)?;
        Ok(ResolvedStep {
            form: member,
            step,
            answer,
        })
    }
}

/// THE ONE CROSSING. The relation standing here and the sealed step go
/// in; the far side comes out as the next standing relation.
///
/// A crossing is a REPUBLICATION, so it takes the carrier's own
/// republishing act: the operand carrier is consumed and what answers
/// over the result is derived from the relation that actually crossed.
/// A SCOPE BARRIER hides the scope before it, and that is why nothing
/// here has to remember not to carry it — there is no second copy, and
/// the act that would carry it does not exist.
///
/// The step's ANSWER is authored syntax, and that is all an author
/// states. Whether the name reaches the far side, and by which route, is
/// decided here: it is spent over the crossed result, so the stage owner
/// a later reference can address is always the stage this crossing
/// published and never the entity spelling the barrier hid.
pub(super) fn cross(
    standing: ResolvedRelation,
    step: ResolvedStep,
    identities: &crate::relation::Planning,
) -> Result<ResolvedRelation> {
    let ResolvedStep { form, step, answer } = step;
    // `|> … as f` — the stage answers to `f` from here on. The name is
    // spent over the CROSSED result: it is stamped on the relation this
    // crossing produced and reaches a later reference only through the
    // frontier born of that relation, so the owner a reference can address
    // is always the stage this crossing published and never the entity
    // spelling the barrier hid. A named stage is no longer one of the
    // unnamed pipes the deictic `_` enumerates.
    let answer = answer.map(|name| identities.intern(name.as_str(), name.is_stropped()));
    match form {
        StepForm::Pipe(pipe) => match crossing(pipe) {
            Crossing::DerivationOwned => standing.crossed(answer, identities, |chain| {
                identities.authority().reland(chain, step)
            }),
            Crossing::StageExport => standing.crossed(answer, identities, |chain| {
                let chain = identities.authority().reland(chain, step)?;
                stage_export(chain, identities)
            }),
            Crossing::Drilling => {
                let (nest, interior) = drilled_interior(&step, identities)?;
                standing.drilled(answer, nest, interior, identities, |chain| {
                    identities.authority().reland(chain, step)
                })
            }
        },
        StepForm::Replacing => standing.crossed(answer, identities, |chain| {
            identities.authority().reland(chain, step)
        }),
        // AN AUTHORED NAME REPLACES WHAT ANSWERS: a preserving form named
        // `as f` publishes the one occurrence `f` owns, and the spellings
        // that reached its operand do not reach around it. Unnamed, it
        // leaves every route its operand held open.
        StepForm::Preserving => match answer {
            Some(answer) => standing.crossed(Some(answer), identities, |chain| {
                identities.authority().reland(chain, step)
            }),
            None => standing.republished_within(None, identities, |chain| {
                identities.authority().reland(chain, step)
            }),
        },
        StepForm::Argumentative => standing.crossed_bare(answer, identities, |chain| {
            identities.authority().reland(chain, step)
        }),
    }
}

/// A RELATIONAL CALL'S RESULT, sealed by the road that resolved it.
///
/// The relation, what answers over it, the answer this read was given,
/// and whether the normalizer LANDED a piped relation in the call's one
/// open parameter all have ONE origin: [`CallOutcome::of`] reads the
/// landing off the very call it hands to the resolution, and hands that
/// resolution the very answer it records. There is no constructor that
/// takes the three facts from three places, and there is one consuming
/// operation, which answers the landing question itself.
///
/// What became of the caller row is NOT among them: the row's own carrier
/// records that, at the site that took it.
pub(super) struct CallOutcome {
    standing: ResolvedRelation,
    answer: Option<delightql_types::SqlIdentifier>,
    landed: bool,
}

impl CallOutcome {
    /// SEAL A CALL. The unresolved call and the authored answer go in
    /// whole; `resolve` receives them and answers with what it resolved.
    pub(super) fn of(
        call: ast_unresolved::FunctorCall,
        answer: Option<delightql_types::SqlIdentifier>,
        resolve: impl FnOnce(
            ast_unresolved::FunctorCall,
            Option<delightql_types::SqlIdentifier>,
        ) -> Result<ResolvedRelation>,
    ) -> Result<CallOutcome> {
        // A LANDED CALL IS A PIPE FORM (fundamentals: a PIPE FORM is a
        // PIPE OPERATOR, a CALL, or a REDUCTION) — read off the arguments
        // of the call this resolution is about to consume, never supplied.
        let landed = call.arguments.judged()?.landed().is_some();
        let answered = answer.clone();
        let standing = resolve(call, answer)?;
        Ok(CallOutcome {
            standing,
            answer: answered,
            landed,
        })
    }

    /// A LANDED CALL takes the crossing; a call with no landing is an
    /// ordinary relational read standing at a chain's head, raises no
    /// scope barrier, and keeps the scope it published. The outcome
    /// answers which it was; no caller decides.
    pub(super) fn crossed_if_landed(
        self,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let CallOutcome {
            standing,
            answer,
            landed,
        } = self;
        if !landed {
            return Ok(standing);
        }
        let answer = answer.map(|name| identities.intern(name.as_str(), name.is_stropped()));
        standing.crossed(answer, identities, |chain| {
            Ok(match crossing(PipeForm::Call) {
                Crossing::DerivationOwned => chain,
                Crossing::StageExport => stage_export(chain, identities)?,
                Crossing::Drilling => unreachable!("a call opens no interior"),
            })
        })
    }
}

/// THE INTERIOR A DRILL OPENED and the name it answers to, read off the
/// sealed step: the bound drill names the exact column it drilled, whose
/// published spelling is the nest name and whose interior the relation
/// store recorded when the column was born.
fn drilled_interior(
    step: &ast_resolved::Step,
    identities: &crate::relation::Planning,
) -> Result<(crate::names::Sym, crate::relation::SemanticRelation)> {
    use crate::pipeline::asts::core::{Continuation, StructuralForm, StructuralStep};
    let Continuation::Structural(StructuralStep {
        form: StructuralForm::Drill { drill },
        ..
    }) = step.form()
    else {
        return Err(crate::error::DelightQLError::transformation_error(
            "a narrowing access crossed without the drill it resolved",
            "pipe crossing",
        ));
    };
    let nest = identities
        .published_sym(drill.column.column())
        .ok_or_else(|| {
            crate::error::DelightQLError::transformation_error(
                "a drilled column publishes no name to open its interior under",
                "pipe crossing",
            )
        })?;
    let interior = crate::relation::interior(identities, drill.column)?.ok_or_else(|| {
        crate::error::DelightQLError::transformation_error(
            "a drilled column has no recorded interior",
            "pipe crossing",
        )
    })?;
    Ok((nest, interior))
}

/// THE STAGE PUBLICATION over a completed pipe-form result — the one act
/// a `Crossing::StageExport` member's output takes. The whole published
/// interface is republished one-to-one through a `ProjectWhy::Stage`
/// projection, whose derivation the boundary judgment dequalifies — and
/// which, being a pipe stage, SPENDS the support that stood under it:
/// what the operand owed its own predicates is not the pipe result's to
/// carry (a projection spends what stood under it; SCOPE DEATH ends the
/// operand's routes at the stage).
fn stage_export(
    chain: ast_resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<ast_resolved::Chain> {
    let result = chain.semantic_relation();
    let sources = crate::relation::published_ports(identities, &result)?;
    let slots: Vec<_> = sources
        .iter()
        .map(|source| crate::relation::form::ProjectSlot::Carried {
            source: *source,
            naming: crate::relation::form::Naming::Inherited,
        })
        .collect();
    identities.authority().extend(
        chain,
        crate::relation::builder::StepOp::Republish {
            of: crate::relation::builder::Republishing::Project(
                crate::relation::form::ProjectSpec {
                    input: result,
                    why: crate::relation::form::ProjectWhy::Stage,
                    slots: &slots,
                    dependencies: &[],
                },
            ),
            sources,
        },
    )
}
