// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Function expressions and related types
//! FunctionApplication, StandardApplication, InfixApplication, CaseExpression,
//! ValueTemplate

use super::super::{LiteralValue, Phase, Unresolved};
use super::domain::DomainExpression;
use super::truth::TruthExpression;
use crate::{enums::EntityType, lispy::ToLispy, ToLispy};
use std::ops::Deref;

/// The one callable payload used by relational, scalar, higher-order, sigma,
/// window, directive, TVF, and DML positions. The surrounding AST position
/// supplies the scalar/relational fence; the call itself does not grow a
/// second representation for each spelling.
///
/// CALL IDENTITY IS CALLEE, ARGUMENTS AND MARKS — and nothing else. A scalar
/// guard and a window are the SCALAR position's context, so they live on
/// `StandardApplication` and a relational, sigma, effect or DML call cannot
/// carry one. How the call ARRIVED is not identity either: a pipe puts its
/// relation in a source-role argument, so a direct call and a piped one
/// differ only in whether such an argument stands there.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctorCall<P: Phase = Unresolved> {
    pub callee: P::Entity,
    pub arguments: super::super::operators::CallArguments<P>,
    pub marks: crate::pipeline::asts::vocabulary::FunctorMarks,
}

/// A call whose authored callee has passed the pure/effect fence. The raw
/// carrier is private to this wrapper once it enters the production graph.
#[derive(Debug, Clone, PartialEq)]
pub struct PureCall<P: Phase = Unresolved>(FunctorCall<P>);

/// An effect call carries the same call data as every other invocation. Its
/// receipt is the ordinary access standing in the effect POSITION — after the
/// call, where a relational access stands — not a field of call identity.
#[derive(Debug, Clone, PartialEq)]
pub struct EffectCall<P: Phase = Unresolved>(FunctorCall<P>);

/// The graph stores one sealed call carrier. The variant is the load-bearing
/// pure/effect fence; phase conversion preserves it without reopening a raw
/// call construction door.
#[derive(Debug, Clone, PartialEq)]
pub enum SealedCall<P: Phase = Unresolved> {
    Pure(PureCall<P>),
    Effect(EffectCall<P>),
}

impl PureCall<Unresolved> {
    pub fn seal(call: FunctorCall<Unresolved>) -> Result<Self, FunctorCall<Unresolved>> {
        if call.call().callee.mark() == crate::pipeline::asts::vocabulary::Mark::Effect {
            return Err(call);
        }
        Ok(Self(call))
    }
}

impl EffectCall<Unresolved> {
    pub fn seal(call: FunctorCall<Unresolved>) -> Result<Self, FunctorCall<Unresolved>> {
        if call.call().callee.mark() != crate::pipeline::asts::vocabulary::Mark::Effect {
            return Err(call);
        }
        Ok(Self(call))
    }
}

impl<P: Phase> PureCall<P> {
    pub fn call(&self) -> &FunctorCall<P> {
        &self.0
    }

    pub(crate) fn call_mut(&mut self) -> &mut FunctorCall<P> {
        &mut self.0
    }

    pub(crate) fn into_inner(self) -> FunctorCall<P> {
        self.0
    }

    pub(crate) fn from_inner(call: FunctorCall<P>) -> Self {
        Self(call)
    }
}

impl<P: Phase> EffectCall<P> {
    pub fn call(&self) -> &FunctorCall<P> {
        &self.0
    }
}

impl<P: Phase> Deref for PureCall<P> {
    type Target = FunctorCall<P>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P: Phase> Deref for EffectCall<P> {
    type Target = FunctorCall<P>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P: Phase> SealedCall<P> {
    pub fn call(&self) -> &FunctorCall<P> {
        match self {
            Self::Pure(call) => call.call(),
            Self::Effect(call) => call.call(),
        }
    }

    pub fn is_effect(&self) -> bool {
        matches!(self, Self::Effect(_))
    }

    pub(crate) fn call_mut(&mut self) -> &mut FunctorCall<P> {
        match self {
            Self::Pure(call) => &mut call.0,
            Self::Effect(call) => &mut call.0,
        }
    }

    pub(crate) fn into_inner(self) -> FunctorCall<P> {
        match self {
            Self::Pure(call) => call.0,
            Self::Effect(call) => call.0,
        }
    }

    pub(crate) fn from_inner(call: FunctorCall<P>, effect: bool) -> Self {
        if effect {
            Self::Effect(EffectCall(call))
        } else {
            Self::Pure(PureCall(call))
        }
    }
}

impl SealedCall<Unresolved> {
    /// Decode the authored callee exactly once and seal its effect category.
    pub fn authored(call: FunctorCall<Unresolved>) -> Self {
        match EffectCall::seal(call) {
            Ok(call) => Self::Effect(call),
            Err(call) => {
                Self::Pure(PureCall::seal(call).expect("effect fence classification is exhaustive"))
            }
        }
    }
}

/// The effect fence is a mark on the authored NAME, so it can only be read
/// where the name is still there to read. A later phase seals a call by
/// carrying the category it already has.
impl From<FunctorCall<Unresolved>> for SealedCall<Unresolved> {
    fn from(call: FunctorCall<Unresolved>) -> Self {
        Self::authored(call)
    }
}

impl<P: Phase> ToLispy for SealedCall<P>
where
    FunctorCall<P>: ToLispy,
{
    fn to_lispy(&self) -> String {
        self.call().to_lispy()
    }
}

impl<P: Phase> crate::lispy::ToLispy for FunctorCall<P>
where
    P::Entity: crate::lispy::ToLispy,
{
    fn to_lispy(&self) -> String {
        let arguments = self.arguments.to_lispy();
        format!(
            "(functor-call {} ({}) {:?})",
            self.callee.to_lispy(),
            arguments,
            self.marks
        )
    }
}

/// A WINDOW MODIFIES ONE STANDARD APPLICATION. It is not a rival application
/// kind, so it has no callee of its own and reaches the lowering through the
/// application that owns it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("functor_window")]
pub struct WindowSpec<P: Phase = Unresolved> {
    pub partition: Vec<DomainExpression<P>>,
    pub ordering: Vec<super::super::OrderingSpec<P>>,
    pub frame: Option<super::super::operators::WindowFrame<P>>,
}

/// A CALL, WITH THE SCALAR CONTEXT THE VALUE POSITION GIVES IT.
///
/// The call says who is called and with what; this says what the scalar
/// position adds — the guard that filters the rows the call sees, and the
/// window it is computed over. Both are value-position facts: a relational,
/// sigma, effect or DML call reaches its callee through the same `PureCall`
/// and can hold neither.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("standard_application")]
pub struct StandardApplication<P: Phase = Unresolved> {
    pub call: PureCall<P>,
    pub guard: Option<Box<TruthExpression<P>>>,
    pub window: Option<WindowSpec<P>>,
}

impl<P: Phase> StandardApplication<P> {
    /// The plain application: a call with neither guard nor window.
    pub fn plain(call: PureCall<P>) -> Self {
        Self {
            call,
            guard: None,
            window: None,
        }
    }

    pub fn call(&self) -> &FunctorCall<P> {
        self.call.call()
    }

    pub(crate) fn call_mut(&mut self) -> &mut FunctorCall<P> {
        self.call.call_mut()
    }
}

impl<P: Phase> FunctorCall<P> {
    pub(crate) fn call(&self) -> &Self {
        self
    }

    pub(crate) fn call_mut(&mut self) -> &mut Self {
        self
    }

    /// The relations the higher-order group supplies, in argument order.
    /// The POSITION is the formal each one binds; the callee's descriptor
    /// or declared formals assign the roles.
    pub fn relations(&self) -> impl Iterator<Item = &super::super::Chain<P>> {
        self.arguments.relations()
    }
}

impl FunctorCall<Unresolved> {
    /// A written call: the name, what was handed to it, and what it asks of
    /// what it names. Nothing about a relation — a call publishes none, and
    /// the relational POSITION a call can stand in carries that.
    pub fn written(
        reference: crate::pipeline::asts::vocabulary::Ref,
        ho_arguments: Vec<super::super::operators::HoArgument<Unresolved>>,
    ) -> Self {
        Self {
            callee: reference,
            arguments: super::super::operators::CallArguments::higher_order(ho_arguments),
            marks: Default::default(),
        }
    }

    /// A scalar application's call: the argument row is the SCALAR stratum,
    /// present even when empty (`f:()` writes a row with no members).
    pub fn scalar_application(
        reference: crate::pipeline::asts::vocabulary::Ref,
        arguments: Vec<super::super::operators::ScalarArgument<Unresolved>>,
    ) -> Self {
        Self {
            callee: reference,
            arguments: super::super::operators::CallArguments::Scalar(arguments),
            marks: Default::default(),
        }
    }

    pub fn scalar(
        reference: crate::pipeline::asts::vocabulary::Ref,
        arguments: Vec<DomainExpression<Unresolved>>,
    ) -> Self {
        Self::scalar_application(
            reference,
            arguments
                .into_iter()
                .map(|value| {
                    super::super::operators::ScalarArgument::Value(
                        super::truth::ArgumentValue::plain(value),
                    )
                })
                .collect(),
        )
    }
}

/// A RELATION MADE ONE VALUE.
///
/// The two forms differ only in where the interior gets its base — a named
/// one reads the relation the form names, a sourceless one supplies its own
/// — and both answer to the same scalarization and degree judgments.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum ScalarRelation<P: Phase = Unresolved> {
    /// `orders:(…)`
    #[lispy("scalar_relation:named")]
    Named {
        identifier: super::helpers::QualifiedName,
        body: Box<ScalarizedRelation<P>>,
    },
    /// `_:(, …)`
    #[lispy("scalar_relation:sourceless")]
    Sourceless { body: Box<ScalarizedRelation<P>> },
}

/// THE COMPRESSION, RIDING IN THE TREE.
///
/// The semantic grammar calls the boundary a *compression*; this names the
/// semantic result — one relational expression becoming one scalar value —
/// and cannot be mistaken for storage compression.
///
/// Normalization removes the terminal compression from the interior exactly
/// once. What is left is the refinable `body`; the compression itself is
/// retained HERE as the proof, so no consumer re-derives cardinality by
/// inspecting how the interior happened to end, and no step can be moved
/// across it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("scalarized_relation")]
pub struct ScalarizedRelation<P: Phase = Unresolved> {
    /// THE COMPRESSED BODY. Private beside the compression, and for the
    /// same reason: the steps that came off this chain answer about THIS
    /// body, so a road that swaps the body out leaves a compression
    /// answering for a relation that is no longer under it.
    #[lispy("body")]
    body: super::chain::Chain<P>,
    /// THE COMPRESSION, RIDING AS THE STEPS IT CAME OFF AS.
    ///
    /// Not a plan to rebuild from: these are the exact steps the chain
    /// carried, relation included, so putting the compression back on is a
    /// move rather than a construction and nothing here can pair a step
    /// with a relation it did not publish.
    #[lispy("compression")]
    compression: Vec<super::chain::Step<P>>,
    #[lispy("output")]
    pub output: P::ScalarOutput,
}

impl<P: Phase> ScalarRelation<P> {
    /// The compressed body, whichever form named it.
    pub fn body(&self) -> &ScalarizedRelation<P> {
        match self {
            Self::Named { body, .. } | Self::Sourceless { body } => body,
        }
    }

    pub fn into_body(self) -> ScalarizedRelation<P> {
        match self {
            Self::Named { body, .. } | Self::Sourceless { body } => *body,
        }
    }
}

impl<P: Phase<Scope = (), ScalarOutput = ()>> ScalarizedRelation<P> {
    /// The authored compression: the steps it is are built here, because
    /// the authored phase has no relation for them to be paired with.
    pub fn authored(body: super::chain::Chain<P>, scalarization: Scalarization<P>) -> Self {
        use super::super::operators::PipeOp;
        use super::super::specs::GroupSpec;
        use super::chain::{Continuation, Step};
        use crate::pipeline::asts::core::specs::{TupleOrdinalClause, TupleOrdinalOperator};
        let compression = match scalarization {
            Scalarization::ZeroKeyReduction(items) => vec![Step::authored(Continuation::Pipe {
                operator: PipeOp::Group(GroupSpec::Reduce {
                    keys: Vec::new(),
                    reductions: items,
                    plan: crate::pipeline::asts::core::ReductionPlan::empty(),
                }),
                named: Default::default(),
            })],
            // THE BOUND-TO-ONE IS ONE STEP: the ordering it consumes and the
            // `#<1` are one membership act, so an ordered compression is the
            // ordering's node carrying its bound, and an unordered one is
            // the arbitrary bound alone.
            Scalarization::BoundToOne { ordering } => {
                let bound = TupleOrdinalClause {
                    operator: TupleOrdinalOperator::LessThan,
                    value: 1,
                    offset: None,
                };
                vec![Step::authored(if ordering.is_empty() {
                    Continuation::Bound { bound }
                } else {
                    Continuation::Structural(super::chain::StructuralStep {
                        form: super::chain::StructuralForm::Ordering {
                            specs: ordering,
                            bound: Some(bound),
                        },
                        named: Default::default(),
                    })
                })]
            }
        };
        ScalarizedRelation {
            body,
            compression,
            output: (),
        }
    }
}

impl<P: Phase> ScalarizedRelation<P> {
    /// THE COMPRESSION, PUT BACK ON THE CHAIN IT CLOSES.
    ///
    /// Two seams need the relation WITH its compression: resolution, to ask
    /// the registry about the heading the degree judgment judges, and
    /// lowering, to emit the subquery. Rebuilding it FROM THE PROOF is the
    /// only direction that stays honest — nothing here infers a compression
    /// from how a chain happened to end, which is the inference this carrier
    /// exists to make unnecessary.
    pub fn attached(self) -> super::chain::Chain<P> {
        let Self {
            body, compression, ..
        } = self;
        body.rejoin(compression)
    }

    /// The body alone, borrowed. What it publishes is NOT what the
    /// carrier publishes: the compression stands over it.
    pub fn body(&self) -> &super::chain::Chain<P> {
        &self.body
    }

    /// What the compression IS, read off the steps it rides as.
    pub fn scalarization(&self) -> Scalarization<P> {
        use super::super::operators::PipeOp;
        use super::super::specs::GroupSpec;
        use super::chain::Continuation;
        for step in &self.compression {
            match step.form() {
                Continuation::Pipe {
                    operator: PipeOp::Group(GroupSpec::Reduce { reductions, .. }),
                    ..
                } => return Scalarization::ZeroKeyReduction(reductions.clone()),
                Continuation::Bound { .. } => {
                    return Scalarization::BoundToOne {
                        ordering: Vec::new(),
                    };
                }
                Continuation::Structural(super::chain::StructuralStep {
                    form:
                        super::chain::StructuralForm::Ordering {
                            specs,
                            bound: Some(_),
                        },
                    ..
                }) => {
                    return Scalarization::BoundToOne {
                        ordering: specs.clone(),
                    };
                }
                _ => {}
            }
        }
        unreachable!("a scalarized relation is built from one of the two compressions")
    }

    /// The relation the compression publishes. A step publishes one, and
    /// this step is no exception for having been lifted out of the chain.
    pub fn scope(&self) -> &P::Scope {
        self.compression
            .last()
            .expect("a scalarized relation carries its compression")
            .result()
    }

    /// The inverse, over a chain THIS carrier built: the compression comes
    /// off the end it was put on, and what is left is the body.
    pub fn detach(
        mut chain: super::chain::Chain<P>,
        output: P::ScalarOutput,
    ) -> crate::error::Result<Self> {
        use super::super::operators::PipeOp;
        use super::super::specs::GroupSpec;
        use super::chain::Continuation;

        let missing = || {
            crate::error::DelightQLError::transformation_error(
                "a scalarized relation lost the compression it was built with",
                "scalarization",
            )
        };
        let closing = chain.steps_mut().pop().ok_or_else(missing)?;
        let compression = vec![closing];
        match compression[0].form() {
            Continuation::Pipe {
                operator: PipeOp::Group(GroupSpec::Reduce { .. }),
                ..
            }
            | Continuation::Bound { .. }
            | Continuation::Structural(super::chain::StructuralStep {
                form: super::chain::StructuralForm::Ordering { bound: Some(_), .. },
                ..
            }) => {}
            _ => return Err(missing()),
        }
        Ok(Self {
            body: chain,
            compression,
            output,
        })
    }
}

/// The two compressions the grammar admits, and no third.
///
/// A zero-key reduction carries only its nonempty reduction items: a keyed
/// group or a distinct group cannot inhabit this proof, because neither
/// proves one row. A bound-to-one owns the ordering it consumes, so the
/// ordering that decides WHICH row is part of the proof rather than a step
/// standing loose before it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Scalarization<P: Phase = Unresolved> {
    #[lispy("scalarization:zero_key_reduction")]
    ZeroKeyReduction(
        crate::pipeline::asts::vocabulary::Vec1<super::super::specs::ReductionItem<P>>,
    ),
    #[lispy("scalarization:bound_to_one")]
    BoundToOne {
        ordering: Vec<super::super::specs::OrderingSpec<P>>,
    },
}

/// THE CALLABLE: a form that takes what flows in.
///
/// A callable is NOT a value. It stands in exactly two positions — the cover
/// that applies it to each of a run of columns, and the function-pipe step
/// that applies it to what flows in — and neither is a value position. That
/// is why it is its own type rather than a member of the application family:
/// a value can no longer BE a callable, so a case cannot be one either, and
/// an outer landing cannot reach through a value into a slot this owns.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Callable<P: Phase = Unresolved> {
    /// `upper:(…)` — an application with a slot left open.
    #[lispy("callable:functor")]
    Functor(StandardApplication<P>),
    /// `:"…{@}…"` — a template whose interpolation is the slot.
    #[lispy("callable:string")]
    String(ValueTemplate<P>),
    /// `:(…)` — a body whose slot is written.
    #[lispy("callable:lambda")]
    Lambda(Lambda<P>),
}

/// A LAMBDA'S BODY, with its slot already spelled as the composition input.
///
/// THE BINDER NAMES THE FLOW, and the name is a syntax receipt: `:(|x| …)`
/// says the flowing value may be used more than once, and normalization
/// spends the name by putting the flow at each of its uses. What survives is
/// the body, so nothing downstream carries a binding environment and no
/// resolver meets a reference that addresses no column.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("lambda")]
pub struct Lambda<P: Phase = Unresolved> {
    pub body: Box<DomainExpression<P>>,
}

/// THE CLOSED NON-REFERENCE VALUE FAMILY.
///
/// A value is a reference or ONE of these. The member says what KIND of
/// value it is, so no consumer rediscovers the kind from a broad enum's
/// contents. Not every member applies a callable: a ground literal, a
/// synthesized selection and a crossed truth are computed values that are
/// not calls, and they live here because this is the one closed family of
/// values that are not references.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum FunctionApplication<P: Phase = Unresolved> {
    /// THE SELF-DENOTING NULLARIES — a literal or a mention. `LiteralValue`
    /// is already that vocabulary: `Symbol` and `Mention` are its mention
    /// members, so ground needs no second layer to own them.
    ///
    /// Ground has no alias field. A ground value MINTS unless the enclosing
    /// publication baptizes it.
    #[lispy("domain_expression:ground")]
    Ground(LiteralValue),
    /// THE OPEN LEAF, standing where an OPEN body leaves a slot. Only the
    /// open positions construct one and the position that applies the body
    /// spends it AT RESOLUTION — the payload is uninhabited afterwards, so
    /// a closed resolved or refined expression cannot carry one.
    #[lispy("domain_expression:hole")]
    Open(P::OpenLeaf),
    /// THE ORDINARY APPLICATION: a call plus the scalar context its position
    /// gives it. The guard and the window ride here because they are facts
    /// about applying the call in value position, not about the call.
    #[lispy("domain_expression:functor_call")]
    Standard(StandardApplication<P>),
    /// `{…}` / `[…]` — VALUE CONSTRUCTION. The one construction carrier;
    /// a destructuring pattern is a different family and has no derivation
    /// through this enum.
    #[lispy("domain_expression:function:enclyph")]
    Enclyph(super::enclyph::Enclyph<P>),
    /// `age * 2` — THE ARITHMETIC INFIX, over a closed operator vocabulary.
    #[lispy("domain_expression:function:infix")]
    Infix(InfixApplication<P>),
    /// `:"text {expr} text"` — a template that interpolates at least once.
    #[lispy("domain_expression:function:string_template")]
    Template(ValueTemplate<P>),
    /// `_:(…)` — the authored CASE, classified by its header.
    #[lispy("domain_expression:function:case")]
    Case(CaseExpression<P>),
    /// `orders:(…)` / `_:(, …)` — A RELATION MADE ONE VALUE, carrying the
    /// compression that proves it and the column it publishes.
    #[lispy("domain_expression:scalar_subquery")]
    Scalarized(ScalarRelation<P>),
    /// `foo:(x).out1` — THE PICK from a call the callee's declared mode
    /// already compressed to one row. The other way a relation reaches value
    /// position, and the only one that needed no authored compression.
    #[lispy("domain_expression:field_select")]
    FieldSelect(FieldSelect<P>),
    /// The selection a MULTI-CLAUSE value rule assembles into. Synthesized
    /// where the group's clauses are read, never authored.
    #[lispy("domain_expression:function:clause_selection")]
    ClauseSelection(ClauseSelection<P>),
    /// JSON path extraction: `x:{.path}` → `json_extract(x, '$.path')`.
    /// The reach is a typed `Path`, so no consumer re-validates it.
    #[lispy("domain_expression:function:json_access")]
    JsonAccess(super::paths::JsonAccess<P>),
    /// THE CROSSING — a truth read as a value. One directed edge: once
    /// crossed, the truth is an ordinary value and composes wherever a value
    /// composes; nothing crosses back.
    #[lispy("domain_expression:crossed")]
    Crossed(Crossing<P>),
}

/// A TRUTH READ AS A VALUE.
///
/// The one truth-to-value carrier, in every phase. Its answer is the
/// truth's own — TRUE, FALSE, or UNKNOWN carried as NULL — read in value
/// position instead of collapsed by a truth position. A comparison of a
/// null is UNKNOWN and REJECTS the row in truth position; crossed, it
/// CARRIES the NULL. Null-safe `=` answers false on a null and carries
/// false.
///
/// THE FIELD IS PRIVATE and there are two doors. [`Crossing::originate`]
/// makes an authored crossing and takes normalization's permit, so only the
/// authority that reads the surface can decide a truth stands in value
/// position. [`Crossing::folded`] carries an EXISTING crossing across a
/// phase boundary by handing its own truth to the fold's truth road. No
/// operation takes a truth and answers with a crossing except the mint, and
/// no operation replaces the truth an authored crossing holds: a crossing
/// that reaches a later phase is the one that was authored, transformed by
/// the same authority that transforms every truth in that phase.
#[derive(Debug, Clone, PartialEq)]
pub struct Crossing<P: Phase = Unresolved>(Box<TruthExpression<P>>);

impl Crossing<Unresolved> {
    /// THE ONE MINT. Normalization's permit is unforgeable outside the
    /// normalization module.
    pub fn originate(
        _permit: crate::pipeline::normalize::CrossingPermit,
        truth: TruthExpression<Unresolved>,
    ) -> Self {
        Crossing(Box::new(truth))
    }
}

impl<P: Phase> Crossing<P> {
    /// The truth this crossing reads.
    pub fn truth(&self) -> &TruthExpression<P> {
        &self.0
    }

    /// THE VALUES THE TRUTH READS AT ITS OWN SCOPE, writable in place. A
    /// walk that rewrites occurrences reaches a crossed truth's operands
    /// here exactly as it reaches an arithmetic operand. The truth itself —
    /// that THIS truth stands as a value — is not for it to replace.
    pub fn scalar_operands_mut(&mut self) -> Vec<&mut DomainExpression<P>> {
        self.0.scalar_operands_mut()
    }

    /// The truth, by value. Lowering spends the crossing here; a phase fold
    /// uses [`Self::folded`] instead, so the crossing survives the fold.
    pub fn into_truth(self) -> TruthExpression<P> {
        *self.0
    }

    /// THE ONE PHASE ROAD. The crossing hands its OWN truth to the fold's
    /// truth road and comes back as the same crossing in the next phase. A
    /// walk is never handed the truth and asked for the crossing back, so it
    /// cannot pair an existing crossing with a truth of its choosing; what
    /// it transforms is what was authored.
    pub fn folded<Q: Phase, F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
    ) -> crate::error::Result<Crossing<Q>> {
        Ok(Crossing(Box::new(walk.transform_boolean(*self.0)?)))
    }
}

impl<P: Phase> crate::lispy::ToLispy for Crossing<P> {
    fn to_lispy(&self) -> String {
        format!("(crossed {})", self.0.to_lispy())
    }
}

/// THE ARITHMETIC INFIX.
///
/// `BinOp` is closed and welded to the grammar's operator vocabulary. A
/// comparison is TRUTH and builds `TruthExpression::Comparison`, so no infix
/// value can spell one and no lowering owes an arm for the case where one
/// arrived. Authored parentheses are admission: they decide which expression
/// nests inside which and leave no receipt behind.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("infix_application")]
pub struct InfixApplication<P: Phase = Unresolved> {
    pub operator: crate::pipeline::asts::vocabulary::BinOp,
    pub left: Box<DomainExpression<P>>,
    pub right: Box<DomainExpression<P>>,
}

/// A TEMPLATE THAT INTERPOLATES.
///
/// The constructor is the only door and it requires at least one
/// interpolation: a template with nothing to interpolate is a ground string
/// and normalization builds one instead. Nothing downstream rescans the parts
/// to rediscover which of the two it holds.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("value_template")]
pub struct ValueTemplate<P: Phase = Unresolved> {
    parts: crate::pipeline::asts::vocabulary::Vec1<ValueTemplatePart<P>>,
}

impl<P: Phase> ValueTemplate<P> {
    /// The one door. `None` when the parts interpolate nothing — the caller
    /// owes the ground string instead.
    pub fn interpolating(parts: Vec<ValueTemplatePart<P>>) -> Option<Self> {
        if !parts
            .iter()
            .any(|part| matches!(part, ValueTemplatePart::Interpolation(_)))
        {
            return None;
        }
        Some(Self {
            parts: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(parts)?,
        })
    }

    pub fn parts(&self) -> impl Iterator<Item = &ValueTemplatePart<P>> {
        self.parts.iter()
    }

    pub fn into_parts(self) -> Vec<ValueTemplatePart<P>> {
        self.parts.into_vec()
    }
}

/// One piece of a template: authored text, or a value to render into it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum ValueTemplatePart<P: Phase = Unresolved> {
    /// Literal text segment
    #[lispy("template_part:text")]
    Text(String),
    /// Interpolated expression
    #[lispy("template_part:interpolation")]
    Interpolation(Box<DomainExpression<P>>),
}

/// THE HEADER CLASSIFIES.
///
/// An `@` header means anchored — every arm a ground match term — and its
/// absence means searched, every arm a condition. The grammar decides it and
/// arm content never reclassifies the case, so a mixed case cannot be
/// represented. The anchor is stored ONCE: it is the case's, not each arm's.
///
/// There is no curried third shape. A case has no input of its own; one that
/// needs the flowing value stands inside a callable that names it, and the
/// anchor is where that value lands.
///
/// AN AUTHORED CASE RESULT IS A DOMAIN EXPRESSION, by the grammar
/// (`match_arm`, `searched_arm`, `default_arm` all end in one); a crossed
/// truth is one such value. A multi-clause value rule's guarded selection is
/// a different shape with a different carrier — see `ClauseSelection`.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum CaseExpression<P: Phase = Unresolved> {
    /// `_:(subject @ term -> result; …)`
    #[lispy("case:anchored")]
    Anchored {
        anchor: Box<DomainExpression<P>>,
        arms: crate::pipeline::asts::vocabulary::Vec1<MatchArm<P>>,
        default: Option<Box<DomainExpression<P>>>,
    },
    /// `_:(condition -> result; …)`
    #[lispy("case:searched")]
    Searched {
        arms: crate::pipeline::asts::vocabulary::Vec1<SearchedArm<P>>,
        default: Option<Box<DomainExpression<P>>>,
    },
}

/// Matching is NULL-SAFE equality: a `null` term MATCHES a null anchor, which
/// is why the term is a ground VALUE and not a comparison.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("match_arm")]
pub struct MatchArm<P: Phase = Unresolved> {
    pub term: LiteralValue,
    pub result: Box<DomainExpression<P>>,
}

#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("searched_arm")]
pub struct SearchedArm<P: Phase = Unresolved> {
    pub condition: Box<TruthExpression<P>>,
    pub result: Box<DomainExpression<P>>,
}

/// ONE CLAUSE of a multi-clause value rule, as the group assembles.
///
/// A clause's result is its BODY, an ordinary value. The guard is the
/// clause's own, absent on the one clause that is the group's default.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("clause_arm")]
pub struct ClauseArm<P: Phase = Unresolved> {
    pub guard: Option<TruthExpression<P>>,
    pub result: DomainExpression<P>,
}

/// The SELECTION a multi-clause value rule assembles into.
///
/// It is not an authored CASE and does not borrow that carrier: nothing
/// writes this shape, the group synthesizes it where its clauses are read,
/// and its arms hold clause BODIES. Lowering spells it as the target's
/// `CASE`, which is a rendering choice and not a claim that the author
/// wrote one.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("clause_selection")]
pub struct ClauseSelection<P: Phase = Unresolved> {
    pub arms: Vec<ClauseArm<P>>,
}

/// THE DECLARED MODE: `f(a, b -> c, d ---- 1, 2 -> "x", "y"; … ; _ -> …)`.
///
/// One carrier for every nonempty width. The `->` in the head declares a
/// FUNCTIONAL DEPENDENCY — the inputs determine the outputs — and that
/// declaration is itself the one-row compression, so nothing here is a
/// desugaring into some other shape that happens to be one row at width one.
///
/// Callably this is the case law the arms spell, asked null-safely of the
/// supplied input row. Without a default it also has a finite fact face whose
/// heading is the inputs followed by the outputs and whose rows are the arms.
/// A default denotes the complement of those inputs over an unbounded domain,
/// so its family is callable-only.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("fact_function_mode")]
pub struct FactFunctionMode<P: Phase = Unresolved> {
    /// The declared input attributes, in order. Their count is the call's
    /// arity and the width of every arm's match row.
    pub inputs: crate::pipeline::asts::vocabulary::Vec1<delightql_types::SqlIdentifier>,
    /// The declared output attributes, in order. Their count is the width of
    /// the row a call compresses to, and a `field_select` names one of them.
    pub outputs: crate::pipeline::asts::vocabulary::Vec1<delightql_types::SqlIdentifier>,
    /// The explicit match arms. When there is no default they also comprise
    /// the finite relational face.
    pub arms: crate::pipeline::asts::vocabulary::Vec1<FactFunctionArm<P>>,
    /// `_ -> …`: the output row a call answers with when no arm matched.
    /// Callable fallback behavior only — it publishes no relational row.
    pub default: Option<crate::pipeline::asts::vocabulary::Vec1<DomainExpression<P>>>,
}

/// One arm: a GROUND match row, and the output row it determines.
///
/// AN INPUT ARM IS A GROUND MATCH ROW — a condition has no derivation here,
/// which is what separates this production from a searched case written as an
/// ordinary function rule. Matching is null-safe, and a multi-input arm
/// matches by conjunction over corresponding positions.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("fact_function_arm")]
pub struct FactFunctionArm<P: Phase = Unresolved> {
    pub inputs: crate::pipeline::asts::vocabulary::Vec1<LiteralValue>,
    pub outputs: crate::pipeline::asts::vocabulary::Vec1<DomainExpression<P>>,
}

/// A complete fact-function definition with its available face fixed once.
///
/// The classification is private and minted from the complete declared mode.
/// Catalog registration and body opening consume it; neither re-scans arms or
/// asks independently whether a default was present.
#[derive(Debug, Clone)]
pub struct FactFunctionDefinition {
    mode: FactFunctionMode<Unresolved>,
    face: FactFunctionFace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FactFunctionFace {
    FiniteRelation,
    CallableOnly,
}

impl FactFunctionDefinition {
    pub fn assemble(mode: FactFunctionMode<Unresolved>) -> Self {
        let face = if mode.default.is_some() {
            FactFunctionFace::CallableOnly
        } else {
            FactFunctionFace::FiniteRelation
        };
        FactFunctionDefinition { mode, face }
    }

    pub fn mode(&self) -> &FactFunctionMode<Unresolved> {
        &self.mode
    }

    pub(in crate::pipeline::asts) fn entity_type(&self) -> EntityType {
        match self.face {
            FactFunctionFace::FiniteRelation => EntityType::DqlFactExpression,
            FactFunctionFace::CallableOnly => EntityType::DqlDefaultFactFunctionExpression,
        }
    }

    pub(in crate::pipeline::asts) fn relational_body(
        &self,
    ) -> Option<super::super::queries::Query<Unresolved>> {
        match self.face {
            FactFunctionFace::FiniteRelation => Some(self.mode.finite_relational_body()),
            FactFunctionFace::CallableOnly => None,
        }
    }
}

/// Substitute the declared inputs' values into an authored output cell.
///
/// In a finite relational face, a reference to a declared input is answered
/// by the arm's own match value. Only a BARE name binds — a
/// qualifier addresses somebody else's relation, and normalization refused
/// one here.
fn spend_inputs(
    value: DomainExpression<Unresolved>,
    bound: &[(&delightql_types::SqlIdentifier, &LiteralValue)],
) -> DomainExpression<Unresolved> {
    use crate::pipeline::ast_transform::{self, AstTransform};

    struct Spend<'a> {
        bound: &'a [(&'a delightql_types::SqlIdentifier, &'a LiteralValue)],
    }
    impl AstTransform<Unresolved, Unresolved> for Spend<'_> {
        crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

        fn transform_domain(
            &mut self,
            expr: DomainExpression<Unresolved>,
        ) -> crate::error::Result<DomainExpression<Unresolved>> {
            if let DomainExpression::Reference(super::references::Reference::Named(
                super::references::NamedReference(column),
            )) = &expr
            {
                if column.qualifier.is_none() {
                    if let Some((_, value)) = self
                        .bound
                        .iter()
                        .find(|(declared, _)| *declared == &column.name)
                    {
                        return Ok(DomainExpression::Application(FunctionApplication::Ground(
                            (*value).clone(),
                        )));
                    }
                }
            }
            ast_transform::walk_transform_domain(self, expr)
        }
    }

    let mut spend = Spend { bound };
    spend
        .transform_domain(value)
        .expect("a same-phase substitution over an authored cell cannot refuse")
}

impl FactFunctionMode<Unresolved> {
    /// FACT ELABORATION, after absence of a default has proved this mode has
    /// a finite relational face.
    ///
    /// The heading is the declared inputs followed by the declared outputs,
    /// and the rows are the explicit arms. The DEFAULT is absent by
    /// construction: it is what a CALL answers with when no arm matched, and
    /// a relation has no such row to publish.
    fn finite_relation(&self) -> super::chain::AnonTable<Unresolved> {
        let header = self
            .inputs
            .iter()
            .chain(self.outputs.iter())
            .map(|name| {
                DomainExpression::Reference(super::references::Reference::Named(
                    super::references::NamedReference(super::super::columns::AuthoredColumn {
                        name: name.clone(),
                        qualifier: None,
                        namespace_path: super::super::metadata::NamespacePath::empty(),
                    }),
                ))
            })
            .collect();
        let rows = self
            .arms
            .iter()
            .map(|arm| {
                // THE ARM SPENDS ITS OWN MATCH ROW. An output cell may read a
                // declared input, and relationally the value of that input IS
                // the constant this arm matched — so the binding is spent
                // here and the published row is ground, exactly as a fact's
                // row is.
                let bound: Vec<(&delightql_types::SqlIdentifier, &LiteralValue)> =
                    self.inputs.iter().zip(arm.inputs.iter()).collect();
                arm.inputs
                    .iter()
                    .map(|value| {
                        DomainExpression::Application(FunctionApplication::Ground(value.clone()))
                    })
                    .chain(
                        arm.outputs
                            .iter()
                            .map(|out| spend_inputs(out.clone(), &bound)),
                    )
                    .collect()
            })
            .collect();
        super::chain::AnonTable::from_values(Some(header), rows)
            .expect("a declared mode has a nonempty heading and at least one arm")
    }

    /// The same, as the chain a relational body is.
    fn finite_relational_chain(&self) -> super::chain::Chain<Unresolved> {
        super::chain::Chain::authored(super::chain::GroundForm::Literal(
            super::chain::AnonRelation::plain(self.finite_relation()),
        ))
    }

    /// Build the body for the closed definition's finite face.
    fn finite_relational_body(&self) -> super::super::queries::Query<Unresolved> {
        super::super::queries::Query::relational(self.finite_relational_chain())
    }
}

impl<P: Phase> FactFunctionMode<P> {
    /// The position the named output occupies, by exact `SqlIdentifier`
    /// agreement — stropping included, because a strop is spelling and two
    /// spellings that differ are two names.
    pub fn output_position(&self, name: &delightql_types::SqlIdentifier) -> Option<usize> {
        self.outputs.iter().position(|declared| declared == name)
    }

    /// The declared input/output heading. It is callable metadata for every
    /// mode and the relational heading only when no default is present.
    pub fn heading(&self) -> Vec<delightql_types::SqlIdentifier> {
        self.inputs
            .iter()
            .chain(self.outputs.iter())
            .cloned()
            .collect()
    }
}

/// `foo:(x).out1` — THE PICK FROM A MODE-COMPRESSED CALL.
///
/// The call is one ROW by the callee's declared functional dependency, and
/// this names one of that row's columns. It is not a third scalarization
/// witness: no interior is compressed here, because nothing was authored to
/// compress — the declaration did it.
///
/// Bare `.name` immediately after a call is this and nothing else.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("field_select")]
pub struct FieldSelect<P: Phase = Unresolved> {
    /// The call whose declared mode compresses it to one row.
    pub application: StandardApplication<P>,
    /// The output the author picked.
    pub field: P::Col,
    /// The declaration that licensed the pick and the case law it carries.
    /// Nothing is fabricated before resolution: the declaration lives in the
    /// catalog, and the authored phase has not read it.
    pub dependency: P::FunctionalDependency,
}

/// WHAT RESOLUTION ANSWERED THE PICK WITH.
///
/// The declaration the catalog holds, the mode's arms resolved, and the
/// POSITION the selected output occupies. Lowering emits the selection by
/// that position; no phase past resolution addresses the field by characters.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("mode_witness")]
pub struct ModeWitness<P: Phase = Unresolved> {
    /// The entity that declared the mode, as the catalog names it.
    pub entity: super::helpers::QualifiedName,
    /// The declared mode, resolved.
    pub mode: FactFunctionMode<P>,
    /// The declared INPUTS as occurrences, in declared order. An output cell
    /// that reads an input reads one of these, and the callable face spends
    /// them by substituting the call's arguments at lowering. Empty before
    /// resolution, where no occurrence exists to name.
    pub inputs: Vec<P::ScalarOutput>,
    /// Which declared output the field named.
    pub selected: usize,
}
