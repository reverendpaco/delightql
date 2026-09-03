// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Effect chains.
//!
//! Every effect chain contains at least one effect call with no counting rule
//! needed: the first alternative IS one and the pipe and connective
//! alternatives ADD one. Ordinary continuations are pure by construction, so
//! the continuation alternative is how pure material attaches.
//!
//! THE PARENTHESIS GROUPS, read by POSITION: on the right of a pipe one group
//! is always receipt access and two groups are always
//! `(parameters)(receipt access)`. Neither group's contents nor the callee's
//! descriptor participates — descriptor arity is checked afterwards, by
//! something that knows the descriptor.
//!
//! The pipe alternative is SUBSTITUTION, not combination: `q |> f!(a)(acc)` is
//! the same call as `f!(q, a)(acc)`. The connective alternatives are genuine
//! peer joins. That is why a DML form appears only in the pipe alternative — a
//! mutation source exists solely to be fed to its terminal.

use super::Normalizer;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::definitions::Head;
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::Step;
use crate::pipeline::asts::core::{
    Access, Chain, Continuation, FunctorCall, GroundMention, PipeOp, Query, Relation, SealedCall,
    SetOperator, Unresolved,
};
use crate::pipeline::syntax::cst;
use delightql_types::SqlIdentifier;

/// THE FIRST PARENTHESES ARE ARGUMENTS, NEVER A TABLE — judged where the
/// authored arguments are still exactly what the author wrote, against what the
/// callee declares.
///
/// A parameter declared as a string, a path, or a namespace takes a term; a
/// relation standing there is a table written where an argument belongs —
/// which is also what a `;`-row spelling becomes, since lifted rows dissolve
/// into one anonymous table. `RelationTarget` is the declared exception: a
/// target NAMES where the effect lands.
///
/// A NON-BUILT-IN DECLARES NOTHING HERE: a user effect rule's parameters live
/// on its own definition, and resolution judges them against it.
fn judge_authored_arguments(
    reference: &crate::pipeline::asts::vocabulary::Ref,
    arguments: &[HoArgument<Unresolved>],
) -> Result<()> {
    use crate::pipeline::asts::effects::DirectiveParamKind;
    let Some(descriptor) = crate::pipeline::asts::effects::descriptor_for_reference(reference)
    else {
        return Ok(());
    };
    let name = reference.name_text();
    let bare = name.strip_suffix('!').unwrap_or(&name);
    for (position, argument) in arguments.iter().enumerate() {
        let Some(declared) = descriptor.params.get(position) else {
            continue;
        };
        if !matches!(argument, HoArgument::Relation(_) | HoArgument::Rule(_))
            || matches!(
                declared.kind,
                DirectiveParamKind::RelationTarget | DirectiveParamKind::RuleValue
            )
        {
            continue;
        }
        return Err(DelightQLError::validation_error_categorized(
            "effect/arguments/not_a_table",
            format!(
                "the first parentheses of {bare}! are its ARGUMENTS, never a table: \
                 '{}' takes a value, and a relation — including the one a `;`-row \
                 spelling builds — cannot be one",
                declared.name
            ),
            format!("supply the arguments as values: `{bare}!(arg, …)(*)`; pipe the relation in"),
        ));
    }
    Ok(())
}

/// STRICT LANDING (effect-algebra-law): piped data must land, visibly, exactly
/// once. A directive that declares NO parameters has only the slot its piped
/// input fills, so an argument written there leaves the pipe nowhere to land —
/// the law's own second clause, "no parameters at all".
///
/// A LANDING MARK IS NOT AN ARGUMENT: `@` names where the pipe goes, so a row
/// holding nothing else still leaves the slot free. The one exemption is
/// declared, not named — a directive that packages an OTHER relation says so
/// with its receipt payload, and its argument is that relation rather than an
/// occupant of the input slot.
fn judge_landing(
    reference: &crate::pipeline::asts::vocabulary::Ref,
    arguments: &[HoArgument<Unresolved>],
) -> Result<()> {
    use crate::pipeline::asts::effects::ReceiptPayload;
    let Some(descriptor) = crate::pipeline::asts::effects::descriptor_for_reference(reference)
    else {
        return Ok(());
    };
    let name = reference.name_text();
    if descriptor.receipt_payload == ReceiptPayload::OtherRelation
        || !descriptor.params.is_empty()
        || arguments
            .iter()
            .all(|argument| matches!(argument, HoArgument::Landing(_)))
    {
        return Ok(());
    }
    let bare = name.strip_suffix('!').unwrap_or(&name);
    Err(DelightQLError::validation_error_categorized(
        "effect/landing/nowhere",
        format!(
            "'{bare}!' declares no parameters, so its one slot is the piped relation's \
             — the written argument fills it and the pipe has nowhere to land"
        ),
        format!("pipe into it with the slot free: `… |> {bare}!(*)`"),
    ))
}

/// What rides an effect chain after its call: pure material, or an
/// annotation decorating the position it stands in. They interleave, so the
/// order they were written in is the order they are applied.
enum EffectToken<'t> {
    Continuation(cst::Continuation<'t>),
    Annotation(cst::Annotation<'t>),
}

impl<'t> Normalizer<'t> {
    pub(crate) fn effrelex_query(&mut self, node: cst::Effrelex<'t>) -> Result<Query<Unresolved>> {
        let chain = self.require(node.chain(), "an effect relex has a chain")?;
        let chain = self.effect_chain(chain)?;
        self.wrap_let_block(node.let_block(), chain)
    }

    #[stacksafe::stacksafe]
    pub(crate) fn effect_chain(&mut self, node: cst::EffectChain<'t>) -> Result<Chain<Unresolved>> {
        // A pipe form: the source substitutes into the terminal's first
        // parameter, and the landing is spent right here.
        if let Some(terminal) = node.terminal() {
            let source = self.require(node.source(), "a piped effect has a source")?;
            let unwraps = node
                .children()
                .any(|child| matches!(child, cst::EffectChainChild::UnwrapPipeOperator(_)));
            let source = match source {
                cst::EffectChainSource::LetFreeRelex(relex) => self.let_free_relex(relex)?,
                cst::EffectChainSource::EffectChain(chain) => self.effect_chain(chain)?,
                cst::EffectChainSource::DmlForm(dml) => self.dml_form(dml)?,
            };
            let mut chain = match terminal {
                cst::EffectChainTerminal::PostPipeEffrelex(terminal) => {
                    self.post_pipe_effrelex(terminal, Some(source))?
                }
                // The unwrap pipe's right side need not be an effect: the
                // equivalence pipes the source into the callable and reads
                // its `returned` payload, whoever produced it.
                cst::EffectChainTerminal::PureInvocation(invocation) => {
                    self.pure_invocation(invocation, source)?
                }
            };
            if unwraps {
                // THE UNWRAP PIPE: `Q !> S ≡ Q |> S |> .returned(*)`. It is a
                // PIPE FORM, never a boundary, so it lowers into the same two
                // continuations the long spelling writes.
                chain = chain
                    .then(Step::authored(
                        crate::pipeline::asts::core::Continuation::Structural(
                            crate::pipeline::asts::core::StructuralStep {
                                form: crate::pipeline::asts::core::StructuralForm::Drill {
                                    drill: crate::pipeline::asts::core::operators::AuthoredDrill {
                                        column: "returned".to_string(),
                                        glob: true,
                                        columns: Vec::new(),
                                        groundings: Vec::new(),
                                    },
                                },
                                named: Default::default(),
                            },
                        ),
                    ))
                    .pipe(PipeOp::Project(
                        crate::pipeline::asts::vocabulary::Vec1::new(
                            crate::pipeline::asts::core::OutItem::Many(
                                crate::pipeline::asts::core::Spread::Glob(
                                    crate::pipeline::asts::core::Glob::qualified(
                                        SqlIdentifier::new("returned"),
                                    ),
                                ),
                            ),
                        ),
                    ));
            }
            return Ok(chain);
        }

        // A peer join: two relations meet and a combined relation results.
        if let Some(connective) = node.connective() {
            let left = self.require(node.left(), "a connective effect has a left operand")?;
            let right = self.require(node.right(), "a connective effect has a right operand")?;
            let chain = match left {
                cst::EffectChainLeft::LetFreeRelex(relex) => self.let_free_relex(relex)?,
                cst::EffectChainLeft::EffectChain(inner) => self.effect_chain(inner)?,
            };
            let arm = self.direct_effrelex(right)?;
            return self.join(chain, connective, arm);
        }

        // Either a lone direct call, or a chain with pure material attached.
        let mut chain = None;
        let mut steps = Vec::new();
        for child in node.children() {
            match child {
                cst::EffectChainChild::DirectEffrelex(direct) => {
                    chain = Some(self.direct_effrelex(direct)?)
                }
                cst::EffectChainChild::EffectChain(inner) => {
                    chain = Some(self.effect_chain(inner)?)
                }
                cst::EffectChainChild::Continuation(continuation) => {
                    steps.push(EffectToken::Continuation(continuation))
                }
                // An annotation DECORATES the position it stands in, so it is
                // read where it stands — with the chain built SO FAR as its
                // anchor, exactly as in every other chain.
                cst::EffectChainChild::Annotation(annotation) => {
                    steps.push(EffectToken::Annotation(annotation))
                }
                cst::EffectChainChild::PipeOperator(_)
                | cst::EffectChainChild::UnwrapPipeOperator(_) => {}
            }
        }
        let mut chain = self.require(chain, "an effect chain contains an effect call")?;
        for step in steps {
            match step {
                EffectToken::Continuation(continuation) => {
                    chain = self.continuation(continuation, chain)?
                }
                EffectToken::Annotation(annotation) => self.annotation(annotation, &chain)?,
            }
        }
        Ok(chain)
    }

    fn join(
        &mut self,
        chain: Chain<Unresolved>,
        connective: cst::BinaryConnective<'t>,
        arm: Chain<Unresolved>,
    ) -> Result<Chain<Unresolved>> {
        let sigil = self.require(connective.child(), "a connective has a sigil")?;
        Ok(match sigil {
            cst::BinaryConnectiveChild::CommaSigil(_) => {
                chain.then(Step::authored(Continuation::Member {
                    rhs: arm,
                    correlation: None,
                    join_type: None,
                }))
            }
            cst::BinaryConnectiveChild::CorrespondingUnionSigil(_) => {
                chain.bag_op(SetOperator::UnionCorresponding, arm, ())
            }
            cst::BinaryConnectiveChild::SmartUnionSigil(_) => {
                chain.bag_op(SetOperator::SmartUnionAll, arm, ())
            }
            cst::BinaryConnectiveChild::PositionalUnionSigil(_) => {
                chain.bag_op(SetOperator::UnionAllPositional, arm, ())
            }
            cst::BinaryConnectiveChild::MinusSigil(_) => {
                chain.bag_op(SetOperator::MinusCorresponding, arm, ())
            }
        })
    }

    // -----------------------------------------------------------------
    // The calls
    // -----------------------------------------------------------------

    fn direct_effrelex(&mut self, node: cst::DirectEffrelex<'t>) -> Result<Chain<Unresolved>> {
        let (name, access, shaping, arguments) = match node {
            cst::DirectEffrelex::EffrelexArgumentativeFunctor(call) => {
                let name = self.require(call.name(), "an effect call has a name")?;
                let access = self.require(call.access(), "an effect call has a receipt access")?;
                let arguments = self.effect_arguments(call.arguments())?;
                (name, self.slot_access(access)?, Vec::new(), arguments)
            }
            cst::DirectEffrelex::EffrelexInteriorFunctor(call) => {
                let name = self.require(call.name(), "an effect call has a name")?;
                let access = self.require(call.access(), "an effect call has a receipt access")?;
                let arguments = self.effect_arguments(call.arguments())?;
                let (access, shaping) = self.call_group(access)?;
                (name, access, shaping, arguments)
            }
            // A lower-order ground call has ONE group, and it is receipt
            // access. Order is judged after parse, never during it.
            cst::DirectEffrelex::LowerOrderEffrelex(call) => {
                let name = self.require(call.name(), "an effect call has a name")?;
                let access = self.require(call.access(), "an effect call has a receipt access")?;
                let (access, shaping) = self.ground_receipt_group(access)?;
                (name, access, shaping, Vec::new())
            }
        };
        let mut chain = self.effect_call(name, arguments, access, None)?;
        for continuation in shaping {
            chain = self.continuation(continuation, chain)?;
        }
        Ok(chain)
    }

    fn post_pipe_effrelex(
        &mut self,
        node: cst::PostPipeEffrelex<'t>,
        source: Option<Chain<Unresolved>>,
    ) -> Result<Chain<Unresolved>> {
        let name = self.require(node.name(), "an effect call has a name")?;
        let access = self.require(node.access(), "an effect call has a receipt access")?;
        let arguments = self.effect_arguments(node.arguments())?;
        let (access, shaping) = self.receipt_group(access)?;
        let mut chain = self.effect_call(name, arguments, access, source)?;
        for continuation in shaping {
            chain = self.continuation(continuation, chain)?;
        }
        Ok(chain)
    }

    /// An empty `ho_part` is deliberately unspellable: the `()` in
    /// `f!()(access)` is a SURFACE marker that normalizes to an omitted
    /// argument row and never constructs an empty one.
    fn effect_arguments(
        &mut self,
        part: Option<cst::EffectArgumentPart<'t>>,
    ) -> Result<Vec<HoArgument<Unresolved>>> {
        let Some(part) = part else {
            return Ok(Vec::new());
        };
        match self.require(part.child(), "an argument part has a form")? {
            cst::EffectArgumentPartChild::HoPart(node) => self.ho_arguments(node),
            cst::EffectArgumentPartChild::EmptyEffectArguments(_) => Ok(Vec::new()),
        }
    }

    /// A receipt is the ordinary access, read by the same authority every
    /// other paren group is read by. It is not a dialect — which is why a
    /// receipt that SHAPES what it received reaches the same call-group road
    /// a pure invocation's does, and its shaping becomes continuations of the
    /// chain the directive heads.
    fn receipt_group(
        &mut self,
        access: cst::PostPipeEffrelexAccess<'t>,
    ) -> Result<(Access<Unresolved>, Vec<cst::Continuation<'t>>)> {
        match access {
            cst::PostPipeEffrelexAccess::ArgumentativeForm(form) => {
                Ok((self.slot_access(form)?, Vec::new()))
            }
            cst::PostPipeEffrelexAccess::Interior(interior) => self.call_group(interior),
        }
    }

    fn ground_receipt_group(
        &mut self,
        access: cst::LowerOrderEffrelexAccess<'t>,
    ) -> Result<(Access<Unresolved>, Vec<cst::Continuation<'t>>)> {
        match access {
            cst::LowerOrderEffrelexAccess::ArgumentativeForm(form) => {
                Ok((self.slot_access(form)?, Vec::new()))
            }
            cst::LowerOrderEffrelexAccess::Interior(interior) => self.call_group(interior),
        }
    }

    fn effect_call(
        &mut self,
        name: cst::EffectIdentifier<'t>,
        mut arguments: Vec<HoArgument<Unresolved>>,
        access: Access<Unresolved>,
        source: Option<Chain<Unresolved>>,
    ) -> Result<Chain<Unresolved>> {
        let reference = self.effect_reference(name)?;
        if let Some(descriptor) =
            crate::pipeline::asts::effects::descriptor_for_reference(&reference)
        {
            if matches!(
                descriptor.category,
                crate::pipeline::asts::effects::DirectiveCategory::Dml(_)
            ) {
                descriptor.judge_invocation(arguments.len(), source.is_some(), &access)?;
            }
        }
        // THE ARGUMENT ROW IS THE DESCRIPTOR'S, read before the pipe's
        // relation joins it: what the author wrote is judged against what the
        // callee declares, and the landing is spent below.
        judge_authored_arguments(&reference, &arguments)?;
        if let Some(source) = source {
            // THE PIPED RELATION LANDS LAST, here as everywhere. A mutation's
            // destination and every other directive's configuration are
            // written arguments; the relation the effect consumes follows
            // them. There is no per-category layout to consult, so no
            // directive can acquire a landing of its own.
            judge_landing(&reference, &arguments)?;
            super::landing::land_relation(&mut arguments, source)?;
        }
        let call = FunctorCall::written(reference, arguments);
        // THE RECEIPT STANDS IN THE EFFECT POSITION — after the call, where a
        // relational access stands. It is the ordinary access, read by the
        // ordinary reader; call identity carries no receipt field.
        Ok(Chain::read(
            Relation::FunctorCall {
                alias: None,
                call: SealedCall::authored(call),
            },
            access,
        ))
    }

    // -----------------------------------------------------------------
    // DML sources
    // -----------------------------------------------------------------

    /// ONE production for the mutation source; the CONSUMING terminal
    /// (`update!`/`delete!`) classifies it. A per-terminal pair would be
    /// byte-identical definitions.
    pub(crate) fn dml_form(&mut self, node: cst::DmlForm<'t>) -> Result<Chain<Unresolved>> {
        match node {
            cst::DmlForm::InsertSource(source) => {
                let relex = self.require(source.child(), "an insert source is a relex")?;
                let body = self.require(relex.body(), "a relex has a body")?;
                self.hoist_let_block(relex.let_block())?;
                self.let_free_relex(body)
            }
            cst::DmlForm::MutationSource(source) => {
                let mut chain = None;
                let mut steps = Vec::new();
                let mut annotations = Vec::new();
                for child in source.children() {
                    match child {
                        cst::MutationSourceChild::MarkedTarget(target) => {
                            chain = Some(self.marked_target(target)?)
                        }
                        cst::MutationSourceChild::Continuation(continuation) => {
                            steps.push(continuation)
                        }
                        cst::MutationSourceChild::Annotation(annotation) => {
                            annotations.push(annotation)
                        }
                    }
                }
                let mut chain = self.require(chain, "a mutation source names its target")?;
                for continuation in steps {
                    chain = self.continuation(continuation, chain)?;
                }
                for annotation in annotations {
                    self.annotation(annotation, &chain)?;
                }
                Ok(chain)
            }
        }
    }

    /// `!!` is call-site EVIDENCE written on the mention, not part of the
    /// name: there is no entity called `emp!!`.
    fn marked_target(&mut self, node: cst::MarkedTarget<'t>) -> Result<Chain<Unresolved>> {
        let name = self.require(node.name(), "a mutation target has a name")?;
        Ok(Chain::read(
            Relation::Ground {
                mention: GroundMention::Named {
                    identifier: self.qualified_reference_name(name)?,
                    alias: None,
                    mutation_target: true,
                    passthrough: false,
                },
                outer: false,
            },
            Access::All,
        ))
    }

    // -----------------------------------------------------------------
    // Effect bindings
    // -----------------------------------------------------------------

    /// The `!` on a label is an ASSERTION that the body is effectful, not a
    /// coercion that makes it so.
    pub(crate) fn effect_cte(
        &mut self,
        node: cst::EffectCte<'t>,
    ) -> Result<super::relex::LetBinding> {
        let (expression, name, head) = match node {
            cst::EffectCte::EffectHoCte(ho) => return self.effect_ho_cte(ho),
            cst::EffectCte::EffectLabelCte(label) => {
                let body = self.require(label.body(), "a label binds a body")?;
                let name = self.require(label.name(), "a label carries a name")?;
                let expression = match body {
                    cst::EffectLabelCteBody::LetFreeRelex(relex) => self.let_free_relex(relex)?,
                    cst::EffectLabelCteBody::EffectChain(chain) => self.effect_chain(chain)?,
                };
                (expression, self.identifier(name), Head::glob())
            }
            cst::EffectCte::EffectStandardCte(standard) => {
                let head = self.require(standard.head(), "a binding has a head")?;
                let body = self.require(standard.body(), "a binding has a body")?;
                let expression = match body {
                    cst::EffectStandardCteBody::LetFreeRelex(relex) => {
                        self.let_free_relex(relex)?
                    }
                    cst::EffectStandardCteBody::EffectChain(chain) => self.effect_chain(chain)?,
                };
                match head {
                    cst::EffectStandardCteHead::EffectGlobHead(head) => {
                        let name = self.require(head.name(), "a head names its subject")?;
                        (expression, self.effect_subject(name)?, Head::glob())
                    }
                    cst::EffectStandardCteHead::EffectArgumentativeHead(head) => {
                        let name = self.require(head.name(), "a head names its subject")?;
                        let mut items = Vec::new();
                        for child in head.children() {
                            match child {
                                cst::EffectArgumentativeHeadChild::HeadTerm(term) => {
                                    items.push(self.head_term(term)?)
                                }
                                cst::EffectArgumentativeHeadChild::CommaSigil(_) => {}
                            }
                        }
                        (expression, self.effect_subject(name)?, Head::listed(items))
                    }
                }
            }
        };
        if !crate::pipeline::asts::effects::expression_demands_directive(&expression) {
            return Err(DelightQLError::validation_error_categorized(
                "effect/cte/pure_mark",
                format!(
                    "the binding '{name}' is marked '!' but its body demands no directive. \
                     The mark asserts that the body is effectful; it cannot make it so. \
                     Drop the mark, or give the body the directive it claims to have."
                ),
                "effect mark on a pure binding",
            ));
        }
        self.binding(
            expression,
            name,
            head,
            // An effect binding's head has no badge position: recursion is
            // relation-form only.
            crate::pipeline::asts::vocabulary::Fixpoint::Bag,
            crate::pipeline::asts::core::CteEffectDeclaration::DemandsDirective,
        )
        .map(super::relex::LetBinding::Relation)
    }

    /// `name!(params)(*) : body` — one clause of the EFFECT MIRROR of a
    /// common higher-order expression: a query-local parameterized effect
    /// rule. Its body normalizes now, as an effect rule's does — the demand
    /// walk binds its relation formal to the piped input and its scalar
    /// formals through the invocation's frame — and the `!` is the same
    /// assertion it is on every effect binding.
    fn effect_ho_cte(&mut self, node: cst::EffectHoCte<'t>) -> Result<super::relex::LetBinding> {
        use crate::pipeline::asts::core::definitions::HoParam;
        use crate::pipeline::asts::ddl::{DdlBody, DefKind, DefSubject};

        let name = self.require(node.name(), "a head names its subject")?;
        let body = self.require(node.body(), "a binding has a body")?;
        let subject = self.effect_subject(name)?;
        let name = self.admit_cte(subject)?;
        let params: Vec<HoParam> = node
            .children()
            .filter_map(|child| match child {
                cst::EffectHoCteChild::HoParam(param) => Some(param),
                cst::EffectHoCteChild::Glob(_) | cst::EffectHoCteChild::CommaSigil(_) => None,
            })
            .map(|param| self.ho_param(param))
            .collect::<Result<_>>()?;
        let expression = match body {
            cst::EffectHoCteBody::LetFreeRelex(relex) => self.let_free_relex(relex)?,
            cst::EffectHoCteBody::EffectChain(chain) => self.effect_chain(chain)?,
        };
        if !crate::pipeline::asts::effects::expression_demands_directive(&expression) {
            return Err(DelightQLError::validation_error_categorized(
                "effect/cte/pure_mark",
                format!(
                    "the binding '{name}' is marked '!' but its body demands no directive. \
                     The mark asserts that the body is effectful; it cannot make it so. \
                     Drop the mark, or give the body the directive it claims to have."
                ),
                "effect mark on a pure binding",
            ));
        }
        // THE SUBJECT CARRIES THE MARK, as a consulted effect rule's does:
        // the demand that opens it names `p!`.
        let decl = self.clause(
            DefKind::Effect,
            DefSubject::Named(SqlIdentifier::new(format!("{}!", name.as_str()))),
            Head::signature(params),
            crate::pipeline::asts::vocabulary::Fixpoint::Bag,
            DdlBody::Relational(Query::relational(expression)),
            self.text(node),
            None,
        );
        Ok(super::relex::LetBinding::HigherOrder {
            name,
            effect: crate::pipeline::asts::core::CteEffectDeclaration::DemandsDirective,
            decl,
        })
    }

    /// The subject a `!`-marked head names. The mark is the SUBJECT's, and
    /// the binding's subject records it as its effect declaration, so the
    /// stored name is the bare one.
    fn effect_subject(&mut self, node: cst::EffectIdentifier<'t>) -> Result<SqlIdentifier> {
        for child in node.children() {
            if let cst::EffectIdentifierChild::PredicateIdentifier(inner) = child {
                let name = self.require(inner.name(), "a subject has a name")?;
                return Ok(self.identifier(name));
            }
        }
        Err(DelightQLError::parse_error(
            "an effect identifier has a predicate identifier",
        ))
    }
}
