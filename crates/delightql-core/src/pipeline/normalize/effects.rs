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

use super::relex::two_landings;
use super::Normalizer;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::definitions::Head;
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::{
    Access, Chain, Continuation, CteBinding, FunctorCall, GroundMention, PipeOp, Query, Relation,
    SealedCall, SetOperator, Unresolved,
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
fn judge_authored_arguments(name: &str, arguments: &[HoArgument<Unresolved>]) -> Result<()> {
    use crate::pipeline::asts::effects::DirectiveParamKind;
    let Some(descriptor) = crate::pipeline::asts::effects::descriptor(name) else {
        return Ok(());
    };
    let bare = name.strip_suffix('!').unwrap_or(name);
    for (position, argument) in arguments.iter().enumerate() {
        let Some(declared) = descriptor.params.get(position) else {
            continue;
        };
        if !matches!(argument, HoArgument::Relation(_))
            || declared.kind == DirectiveParamKind::RelationTarget
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
/// The two exemptions are declared, not named: a directive that packages an
/// OTHER relation says so with its receipt payload, and its argument is that
/// relation rather than an occupant of the input slot; and a mutation's source
/// follows the target it is written to, which is the landing classification
/// this judgment stands beside.
fn judge_landing(
    name: &str,
    arguments: &[HoArgument<Unresolved>],
    target_reads_first: bool,
) -> Result<()> {
    use crate::pipeline::asts::effects::ReceiptPayload;
    let Some(descriptor) = crate::pipeline::asts::effects::descriptor(name) else {
        return Ok(());
    };
    if target_reads_first
        || descriptor.receipt_payload == ReceiptPayload::OtherRelation
        || !descriptor.params.is_empty()
        || arguments.is_empty()
    {
        return Ok(());
    }
    let bare = name.strip_suffix('!').unwrap_or(name);
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
enum Step<'t> {
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
            let mut chain = self.post_pipe_effrelex(terminal, Some(source))?;
            if unwraps {
                // THE UNWRAP PIPE: `Q !> S ≡ Q |> S |> .returned(*)`. It is a
                // PIPE FORM, never a boundary, so it lowers into the same two
                // continuations the long spelling writes.
                chain = chain
                    .then(crate::pipeline::asts::core::Continuation::Structural(
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
                            cpr_schema: (),
                        },
                    ))
                    .pipe(PipeOp::Project(crate::pipeline::asts::vocabulary::Vec1::new(
                        crate::pipeline::asts::core::OutItem::Many(
                            crate::pipeline::asts::core::Spread::Glob(
                                crate::pipeline::asts::core::Glob::qualified(SqlIdentifier::new(
                                    "returned",
                                )),
                            ),
                        ),
                    )));
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
                    steps.push(Step::Continuation(continuation))
                }
                // An annotation DECORATES the position it stands in, so it is
                // read where it stands — with the chain built SO FAR as its
                // anchor, exactly as in every other chain.
                cst::EffectChainChild::Annotation(annotation) => {
                    steps.push(Step::Annotation(annotation))
                }
                cst::EffectChainChild::PipeOperator(_)
                | cst::EffectChainChild::UnwrapPipeOperator(_) => {}
            }
        }
        let mut chain = self.require(chain, "an effect chain contains an effect call")?;
        for step in steps {
            match step {
                Step::Continuation(continuation) => {
                    chain = self.continuation(continuation, chain)?
                }
                Step::Annotation(annotation) => self.annotation(annotation, &chain)?,
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
                chain.then(Continuation::Member {
                    rhs: arm,
                    correlation: None,
                    join_type: None,
                    cpr_schema: (),
                })
            }
            cst::BinaryConnectiveChild::CorrespondingUnionSigil(_) => {
                chain.bag_op(SetOperator::UnionCorresponding, arm, (), ())
            }
            cst::BinaryConnectiveChild::SmartUnionSigil(_) => {
                chain.bag_op(SetOperator::SmartUnionAll, arm, (), ())
            }
            cst::BinaryConnectiveChild::PositionalUnionSigil(_) => {
                chain.bag_op(SetOperator::UnionAllPositional, arm, (), ())
            }
            cst::BinaryConnectiveChild::MinusSigil(_) => {
                chain.bag_op(SetOperator::MinusCorresponding, arm, (), ())
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
        // A DML terminal's authored argument is its TARGET — where to write —
        // and its piped relation is the SOURCE. The roles are the callee's,
        // declared in the directive table, so the classification is read from
        // the descriptor rather than from the argument's shape: a relation in
        // the first group means one thing for `insert!` and another for every
        // pure functor.
        let dml = matches!(
            crate::pipeline::asts::effects::directive_category(&reference.name_text()),
            crate::pipeline::asts::effects::DirectiveCategory::Dml(_)
        );
        // THE ARGUMENT ROW IS THE DESCRIPTOR'S, read before the pipe's
        // relation joins it: what the author wrote is judged against what the
        // callee declares, and the landing is spent below.
        judge_authored_arguments(&reference.name_text(), &arguments)?;
        let mut landing = None;
        if let Some(source) = source {
            let landings: Vec<usize> = arguments
                .iter()
                .enumerate()
                .filter(|(_, argument)| matches!(argument, HoArgument::Landing(_)))
                .map(|(index, _)| index)
                .collect();
            let landed_at = match landings.len() {
                0 => {
                    // THE TARGET READS FIRST. A mutation source is the
                    // relation being written FROM; it follows the destination
                    // it is written to — the descriptor's formals put the
                    // target at the first position and the source after it.
                    let target_reads_first = dml || source_is_mutation(&source);
                    judge_landing(&reference.name_text(), &arguments, target_reads_first)?;
                    if target_reads_first {
                        arguments.push(HoArgument::Relation(source));
                        arguments.len() - 1
                    } else {
                        arguments.insert(0, HoArgument::Relation(source));
                        0
                    }
                }
                1 => {
                    arguments[landings[0]] = HoArgument::Relation(source);
                    landings[0]
                }
                count => return Err(two_landings(count)),
            };
            landing = Some(landed_at);
        }
        let mut call = FunctorCall::written(reference, arguments);
        if let Some(part) = call.arguments.ho_mut() {
            part.landing = landing;
        }
        // THE RECEIPT STANDS IN THE EFFECT POSITION — after the call, where a
        // relational access stands. It is the ordinary access, read by the
        // ordinary reader; call identity carries no receipt field.
        Ok(Chain::read(
            Relation::FunctorCall {
                alias: None,
                call: SealedCall::authored(call),
                cpr_schema: (),
            },
            access,
            (),
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
                    identifier: self.qualified_name(name)?,
                    alias: None,
                    mutation_target: true,
                    passthrough: false,
                },
                outer: false,
                cpr_schema: (),
            },
            Access::All,
            (),
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
    ) -> Result<CteBinding<Unresolved>> {
        let (expression, name, head) = match node {
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
        Ok(self.binding(
            expression,
            name,
            head,
            crate::pipeline::asts::core::CteEffectDeclaration::DemandsDirective,
        ))
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

/// A mutation source is fed to its terminal, never joined: the `!!` on its
/// head is what says the argument is one.
fn source_is_mutation(chain: &Chain<Unresolved>) -> bool {
    matches!(
        match &chain.head {
            crate::pipeline::asts::core::Grelex::Reference(relation) => Some(relation),
            crate::pipeline::asts::core::Grelex::Literal(_) => None,
        },
        Some(Relation::Ground {
            mention: GroundMention::Named {
                mutation_target: true,
                ..
            },
            ..
        })
    )
}
