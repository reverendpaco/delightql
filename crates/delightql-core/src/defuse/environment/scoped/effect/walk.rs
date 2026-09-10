// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE TRAVERSAL: an effect body read in demand order. Every directive
//! demand emits plan statements through the planner's services; what the
//! walk answers is the PURE value with directive demands replaced by
//! receipt reads. Syntax enters here from the authority's paired bodies and
//! from the program text the entrance was handed, and leaves only resolved.

use super::{EffectDemand, EffectSelection, EffectWalk, PlanFacts, ReceiptNaming, WalkCtx};
use crate::diagnostic::{Directive, DirectiveBinding, Effect};
use crate::error::{DelightQLError, Result};
use crate::names::DmlVerb;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved::{Chain, Continuation, GroundMention, PipeOp, Relation};
use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::{
    Access, DomainExpression, GroundForm, NamedReference, QualifiedName, Reference, SealedCall,
    Step, Unresolved,
};
use crate::pipeline::asts::effects::{self, DirectiveCategory};
use crate::pipeline::compiled_query;
use crate::pipeline::effect_transformer::{internal, unsupported, MarkedStepKind};
use std::collections::HashSet;

/// The `(returned.*)` interior-heading projection — the second operator
/// of the canonical release shape (the fusion trigger).
pub(super) fn is_returned_heading_projection(op: &PipeOp) -> bool {
    use crate::pipeline::asts::core::{Glob, Spread};
    matches!(
        op,
        PipeOp::Project(items)
            if items.len() == 1
                && matches!(
                    &items[0],
                    crate::pipeline::asts::core::OutItem::Many(Spread::Glob(Glob {
                        qualifier: Some(q),
                        ..
                    })) if q.as_str() == "returned"
                )
    )
}

/// The exact glob drill into `returned` — the first step of the
/// canonical release shape (no narrowing, no groundings).
pub(super) fn is_returned_glob_drill(step: &Continuation) -> bool {
    matches!(
        step,
        Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
            form: crate::pipeline::asts::core::StructuralForm::Drill { drill },
            ..
        })
            if drill.column == "returned"
                && drill.glob
                && drill.columns.is_empty()
                && drill.groundings.is_empty()
    )
}

/// The outcome of an observed-payload fusion attempt.
pub(super) enum FuseOutcome {
    Fused(Chain),
    NotApplicable(Chain),
}

impl<'p, 'a> EffectWalk<'p, 'a> {
    // ========================================================================
    // The walker: expression → emitted statements + rewritten pure value
    // ========================================================================

    /// Walk an effectful expression in demand order. Every
    /// directive demand emits plan statements; the returned expression is
    /// the PURE value with directive demands replaced by receipt reads.
    #[stacksafe::stacksafe]
    pub(super) fn walk_value(&mut self, expr: Chain, ctx: &WalkCtx) -> Result<Chain> {
        // The fold reads the chain from the OUTSIDE in.
        let mut expr = expr;
        let Some(last) = expr.pop_step() else {
            let (head, access, _) = expr.split_head_access();
            return match head.into_form() {
                GroundForm::Reference(rel) => self.walk_read(rel, access, ctx),
                head => {
                    let expr = Chain::authored(head);
                    self.refuse_if_effectful(&expr)?;
                    Ok(expr)
                }
            };
        };
        match last.into_form() {
            Continuation::Member {
                rhs,
                correlation,
                join_type,
            } => {
                // A join condition carries the same boolean subquery edges as
                // a filter predicate. A directive demanded there is not
                // lowered on the spine.
                if let Some(jc) = correlation
                    .as_ref()
                    .and_then(crate::pipeline::ast_unresolved::MemberCorrelation::condition)
                {
                    if effects::boolean_demands_directive(jc) {
                        return Err(effect_head_predicate_unsupported("a join condition"));
                    }
                }
                let walked_left = self.walk_value(expr, &ctx.without_sink())?;
                // Emission 3: a left conjunct gates directives demanded to
                // its right (E1: an empty step ends the chain). Only gate
                // when the right actually demands one.
                let walked_right = if effects::expression_demands_directive(&rhs) {
                    let mut gated = ctx.clone();
                    gated.guards.push(self.guard_from_value(&walked_left));
                    self.walk_value(rhs, &gated)?
                } else {
                    self.walk_value(rhs, &ctx.without_sink())?
                };
                Ok(walked_left.then(Step::authored(Continuation::Member {
                    rhs: walked_right,
                    correlation,
                    join_type,
                })))
            }

            Continuation::Restrict { condition, origin } => {
                // The predicate is NOT on the lowered source spine. A
                // directive demanded through an IN/EXISTS/scalar subquery here
                // reaches SQL unprocessed under the old walker; instead refuse
                // it with the honest not-yet-lowerable diagnostic.
                if effects::boolean_demands_directive(&condition) {
                    return Err(effect_head_predicate_unsupported(
                        "a predicate subquery (IN / EXISTS / scalar)",
                    ));
                }
                let walked = self.walk_value(expr, &ctx.without_sink())?;
                Ok(walked.then(Step::authored(Continuation::Restrict { condition, origin })))
            }

            // A bound names no expression to demand a directive; a
            // destructure's source is a value position the domain probe
            // already covers. An access spec CAN hide one, in a positional
            // scalar subquery — refuse it honestly rather than hand it on
            // unlowered.
            // The signed witness is a VALUE-level marker whose lowering
            // happens when the value compiles (`compile_value_qe`); the
            // sink flows through it, so the arm's ending directives land
            // in the one rule receipt.
            step @ Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::SignedWitness,
                ..
            }) => {
                let walked = self.walk_value(expr, ctx)?;
                Ok(walked.then(Step::authored(step)))
            }
            step @ (Continuation::Access { .. }
            | Continuation::Bound { .. }
            | Continuation::Correlate { .. }
            | Continuation::Destructure { .. }
            | Continuation::Structural(_)) => {
                if let Continuation::Destructure { source, .. } = &step {
                    if effects::domain_demands_directive(source) {
                        return Err(effect_head_predicate_unsupported("a destructure source"));
                    }
                }
                if let Continuation::Access { access, .. } = &step {
                    if effects::access_demands_directive(access) {
                        return Err(effect_head_predicate_unsupported(
                            "a relation's access specification",
                        ));
                    }
                }
                // CATEGORY ERROR, taught: releasing `returned` from a
                // receipt that declares NO payload — identical for the `!>`
                // sugar and the longhand drill, because they are the same
                // operation.
                if let Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                    form: crate::pipeline::asts::core::StructuralForm::Drill { drill },
                    ..
                }) = &step
                {
                    if drill.column == "returned" {
                        if let Some(name) = tail_payload_free_directive(&expr) {
                            let bare = bare_name(&name);
                            return Err(DelightQLError::from(Directive::ReceiptNoPayload {
                                message: format!(
                                    "{bare}!'s receipt declares no `returned` payload — \
                                     its receipt continues through `|>` (unwrapping a \
                                     payload-free receipt is a category error; see \
                                     EFFECT-ALGEBRA §3)"
                                ),
                            }));
                        }
                    }
                }
                let walked = self.walk_value(expr, &ctx.without_sink())?;
                Ok(walked.then(Step::authored(step)))
            }

            Continuation::BagOp {
                operator,
                arm,
                correlation,
            } => {
                // Disjunction — both operands evaluate, in order. The
                // sink (if any) flows into each: a union value's ending
                // directives all land in the one rule receipt table.
                let left = self.walk_value(expr, ctx)?;
                let arm = self.walk_value(arm, ctx)?;
                Ok(left.bag_op(operator, arm, correlation))
            }

            Continuation::Pipe { operator, .. } => self.walk_pipe(expr, operator, ctx),

            last @ Continuation::ErJoin(_) => {
                let other = expr.then(Step::authored(last));
                self.refuse_if_effectful(&other)?;
                Ok(other)
            }
        }
    }

    /// Walk a READ: the relation, and what its parens asked of it.
    pub(super) fn walk_read(
        &mut self,
        rel: Relation,
        access: Option<Access>,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let restore = |head: Relation, access: Option<Access>| match access {
            Some(access) => Chain::read(head, access),
            None => Chain::authored(GroundForm::Reference(head)),
        };
        match rel {
            // A scratch or receipt read names compiler-owned storage by the
            // receipt of its allocation: nothing in it can be an HO
            // parameter and nothing in it can hide a directive, so it
            // passes through whole.
            Relation::Ground {
                mention:
                    GroundMention::Scratch { .. }
                    | GroundMention::Receipt { .. }
                    | GroundMention::Structural { .. },
                ..
            } => Ok(restore(rel, access)),
            Relation::FunctorCall { call, alias, .. } => {
                self.walk_functor_call(call, alias, access.unwrap_or(Access::Unasked), ctx)
            }
            Relation::Ground {
                mention:
                    GroundMention::Named {
                        ref identifier,
                        alias: _,
                        ..
                    },
                outer: _,
            } => {
                let access = access.unwrap_or(Access::Unasked);
                // The lowering walker closes every recursive position where a
                // directive can hide: a Ground read's access spec can hide a
                // directive in a scalar subquery — NOT on the lowered spine,
                // so refuse it honestly rather than return it unprocessed.
                // Pinned at the constructible AST boundary by
                // `ground_access_spec_directive_refuses_at_lowering` and the
                // collector test
                // `access_demands_directive_reaches_positional_scalar_subquery`.
                if effects::access_demands_directive(&access) {
                    return Err(effect_head_predicate_unsupported(
                        "a relation's access specification",
                    ));
                }
                // A bare capitalized name may be a bound HO parameter: the
                // rule body's `Bad(*)` reads the invocation's piped input.
                if identifier.namespace_path.is_empty() {
                    if let Some(&idx) = ctx.bindings.get(identifier.name.as_str()) {
                        if !access.is_whole() {
                            return Err(unsupported(format!(
                                "HO parameter '{}' is referenced with a reshaping \
                                 access spec; only '{}(*)' is supported in v0.1 \
                                 effect bodies",
                                identifier.name, identifier.name
                            )));
                        }
                        return self.splice_bound_input(idx);
                    }
                }
                Ok(restore(rel, Some(access)))
            }

            other @ (Relation::InnerRelation { .. } | Relation::ConsultedView { .. }) => {
                let expr = restore(other, access);
                self.refuse_if_effectful(&expr)?;
                Ok(expr)
            }
        }
    }

    /// Walk a directive invocation and the RECEIPT it was written with.
    ///
    /// The receipt is the access standing in the effect position, handed in
    /// beside the call — call identity carries no receipt of its own.
    /// A RECEIPT IS A RELATION, and a name authored on the call names it:
    /// `|> insert!(t(*))(*) as r` makes `r` the receipt's owner exactly as
    /// `as r` names any other landed result. The read the walk produces
    /// for the receipt is a plan read of the receipt table; the authored
    /// name rides on that read, where resolution binds it.
    pub(super) fn walk_functor_call(
        &mut self,
        call: SealedCall,
        alias: Option<delightql_types::SqlIdentifier>,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let mut chain = self.walk_functor_call_read(call, alias.clone(), receipt, ctx)?;
        if let Some(alias) = alias {
            if let GroundForm::Reference(Relation::Ground { mention, .. }) =
                chain.head_mut().form_mut()
            {
                match mention {
                    // THE RECEIPT IS NAMED HERE: the plan pairs the row it
                    // allocated with the name the author wrote on the call.
                    GroundMention::Scratch { row } => {
                        let row = *row;
                        *mention = GroundMention::Receipt {
                            receipt: crate::relation::NamedScratch::under(
                                row,
                                alias,
                                ReceiptNaming(()),
                            ),
                            alias: None,
                        };
                    }
                    GroundMention::Receipt {
                        alias: slot @ None, ..
                    } => {
                        *slot = Some(alias);
                    }
                    GroundMention::Named { .. }
                    | GroundMention::Receipt { .. }
                    | GroundMention::Structural { .. } => {}
                }
            }
        }
        Ok(chain)
    }

    pub(super) fn walk_functor_call_read(
        &mut self,
        mut call: SealedCall,
        // The name the READ answers to. A pure relation call keeps it; a
        // demanded effect publishes a receipt whose shape the plan owns.
        alias: Option<delightql_types::SqlIdentifier>,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let name = call.call().callee.name_text();

        // Ordinary named relation calls retain their pure relation behavior.
        // The exclamation mark is the syntax boundary for effect/directive
        // eligibility; an unavailable callable descriptor must not turn
        // `foo(*)` into a demanded effect rule.
        if !name.ends_with('!') && effects::descriptor_for_reference(&call.call().callee).is_none()
        {
            // A PURE CALL: its landed member continues the evaluation spine
            // and is walked; its authored relation and rule arguments are
            // enclosed positions, fenced before anything executes. What
            // each position IS is not this walk's to change.
            effects::refuse_enclosed_effects(call.call())?;
            call.call_mut().arguments.rewrite_landed(|relation| {
                self.walk_value(relation.clone(), &ctx.without_sink())
            })?;
            let read = Chain::read(Relation::FunctorCall { call, alias }, receipt);
            self.refuse_if_effectful(&read)?;
            return Ok(read);
        }

        // A USER DIRECTIVE TAKES ITS ROW WHOLE. Which formal each member
        // binds — the landed relation included — is the admission's answer
        // against the rule's own declaration, made where the rule is
        // invoked; nothing here takes a relation out of the row.
        let Some(builtin) = effects::kind_for_reference(&call.call().callee) else {
            return self.walk_user_directive(call, ctx);
        };

        // A BUILT-IN DIRECTIVE HAS A DESCRIPTOR, and the descriptor's layout
        // is the contract its arguments are read under: the written
        // arguments, then the relation the effect consumes, for every
        // spelling alike. THE ROLES ARE THE MEMBERS' OWN — a landed relation
        // says it is the pipe's where it stands, so the two roles separate
        // as the row is read rather than by comparing an index against a
        // position list.
        let judged = call.call().arguments.judged()?;
        let mut table_arguments: Vec<Chain> = judged
            .relations()
            .iter()
            .map(|argument| argument.relation.clone())
            .collect();
        let mut scalar_arguments = Vec::new();
        for argument in call.call().arguments.ho_members() {
            match argument {
                HoArgument::Value(value) => scalar_arguments.push(value.value.clone()),
                HoArgument::Relation(_)
                | HoArgument::Rule(_)
                | HoArgument::Landed(_)
                | HoArgument::Landing(_)
                | HoArgument::Skip => {}
            }
        }
        let landed_at = judged.landed().map(|landed| landed.position);
        // THE GLOB IS HOW A DEMAND SPELLS "WHOLE", not a value handed to a
        // parameter, so a rule's arity is counted without it.
        for member in call.call().arguments.scalar_members() {
            if let Some(expression) = member.scalar_domain() {
                scalar_arguments.push(expression.clone());
            }
        }
        let relational_count = table_arguments.len();

        if relational_count == 0 {
            return self.walk_directive_call(builtin, &call, &scalar_arguments, ctx);
        }

        if relational_count > 2 {
            return Err(unsupported(format!(
                "directive '{}' has more than one relational argument",
                name
            )));
        }
        if relational_count == 1 {
            let only = table_arguments
                .pop()
                .expect("one relational argument exists");
            call.call_mut().arguments = if landed_at.is_some() {
                crate::pipeline::asts::core::operators::CallArguments::higher_order(
                    call.call()
                        .arguments
                        .ho_members()
                        .filter(|member| !matches!(member, HoArgument::Landed(_)))
                        .cloned()
                        .collect(),
                )
            } else {
                crate::pipeline::asts::core::operators::CallArguments::higher_order(
                    call.call()
                        .arguments
                        .ho_members()
                        .filter(|member| {
                            matches!(member, HoArgument::Rule(_) | HoArgument::Value(_))
                        })
                        .cloned()
                        .collect(),
                )
            };
            return self.walk_directive_terminal(builtin, only, call, receipt, ctx);
        }
        // Two relations: the LANDED member is the pipe's, and the other is
        // the authored argument. Without one — a direct call — the layout is
        // the same row the landing would have built, written arguments then
        // the consumed relation, and there is no per-category layout to look
        // up.
        let mut relations = table_arguments.into_iter();
        let (first, second) = (
            relations.next().expect("two relational arguments exist"),
            relations.next().expect("two relational arguments exist"),
        );
        let (argument, source) = match landed_at {
            Some(0) => (second, first),
            _ => (first, second),
        };
        call.call_mut().arguments =
            crate::pipeline::asts::core::operators::CallArguments::higher_order(
                std::iter::once(HoArgument::Relation(argument))
                    .chain(scalar_arguments.into_iter().map(|value| {
                        HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(value))
                    }))
                    .collect(),
            );
        self.walk_directive_terminal(builtin, source, call, receipt, ctx)
    }

    /// A USER DIRECTIVE, DEMANDED WITH ITS ROW WHOLE. The query's own effect
    /// label or effect-mirror CHOE answers a bare demand first — nearest
    /// wins — and a consulted rule answers otherwise; a qualifier names
    /// another namespace and is never query-local. Either way the row
    /// reaches the invocation as the author wrote it, and the invocation
    /// admits it against the selected rule's own declaration.
    fn walk_user_directive(&mut self, call: SealedCall, ctx: &WalkCtx) -> Result<Chain> {
        let name = call.call().callee.name_text();
        let qualifier = call.call().callee.namespace_fq();
        let demanded = bare_demand_identifier(&call.call().callee.name_identifier());
        let selected = if qualifier.is_none() {
            ctx.world.select_effect_local(&demanded)?
        } else {
            None
        };
        match selected {
            Some(EffectDemand::Label(arms)) => {
                self.walk_effect_label(&name, arms, &call.call().arguments, ctx)
            }
            Some(EffectDemand::Mirror(scoped)) => self.invoke_rule(
                EffectSelection::Scoped(scoped),
                &call.call().arguments,
                ctx,
                false,
            ),
            None => {
                let rule = ctx
                    .world
                    .select_effect_rule(
                        self.plan.system(),
                        qualifier.as_deref(),
                        &name,
                        demanded.is_stropped(),
                    )?
                    .ok_or_else(|| {
                        unsupported(format!(
                            "directive '{}' is not a built-in, not an effect-CTE label of this body, and no effect rule of that name is visible from this demand site",
                            name
                        ))
                    })?;
                self.invoke_rule(
                    EffectSelection::Consulted(rule),
                    &call.call().arguments,
                    ctx,
                    false,
                )
            }
        }
    }

    /// AN EFFECT-CTE LABEL, DEMANDED: the mention IS the instantiation, and
    /// ALL same-label definitions accumulate — the label denotes their
    /// corresponding union, the same label semantics as main-pipeline
    /// duplicate CTE labels and multi-clause rules. A first-match here
    /// dropped every later arm silently, mutations included, under a
    /// success receipt. A label declares no parameter, so its demand takes
    /// no argument.
    fn walk_effect_label(
        &mut self,
        name: &str,
        arms: super::BoundArms<'_>,
        arguments: &crate::pipeline::asts::core::operators::CallArguments<Unresolved>,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        require_bare_demand(name, arguments)?;
        let bare = bare_name(name);
        self.plan.comment_next(|| format!("[arm {}!]", bare));
        // THE ARMS ARE WALKED BY THEIR OWN CARRIER, under the declaration it
        // holds and in the world it was selected from: this road supplies
        // the walk's own state, and the carrier supplies the syntax and the
        // world together.
        let walked = arms.walk(
            PlanFacts {
                guards: ctx.guards.clone(),
                bindings: ctx.bindings.clone(),
                receipt_name: bare.to_string(),
            },
            self,
        )?;
        let mut walked = walked.into_iter();
        let mut accumulated = walked
            .next()
            .expect("an effect label carrier holds at least the arm that minted it");
        for arm in walked {
            accumulated = accumulated.bag_op(
                crate::pipeline::asts::core::expressions::metadata_types::SetOperator::UnionCorresponding,
                arm,
                (),
            );
        }
        Ok(accumulated)
    }

    /// An expression-position BUILT-IN directive call `name!(args)`.
    /// Resolution order: the body's effect-CTE labels and effect mirror
    /// FIRST — a query-local declaration of the name is nearest — then the
    /// built-in's own category.
    pub(super) fn walk_directive_call(
        &mut self,
        builtin: crate::pipeline::asts::effects::DirectiveKind,
        call: &SealedCall,
        arguments: &[DomainExpression],
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let name = call.call().callee.name_text();
        let qualifier = call.call().callee.namespace_fq();
        let bare = bare_name(&name);

        // 1. A query-local declaration of the demanded spelling. Agreement
        //    is the identifier law's, both sides typed: the demand's `!` is
        //    call identity, so the bare spelling keeps the strop bit it was
        //    written with. A qualified demand names another namespace and
        //    is never query-local.
        let demanded_bare = bare_demand_identifier(&call.call().callee.name_identifier());
        let selected = if qualifier.is_none() {
            ctx.world.select_effect_local(&demanded_bare)?
        } else {
            None
        };
        match selected {
            Some(EffectDemand::Label(arms)) => {
                return self.walk_effect_label(&name, arms, &call.call().arguments, ctx);
            }
            Some(EffectDemand::Mirror(scoped)) => {
                return self.invoke_rule(
                    EffectSelection::Scoped(scoped),
                    &call.call().arguments,
                    ctx,
                    false,
                );
            }
            None => {}
        }

        // 2. The built-in's own category.
        let category = builtin.descriptor().category;
        match category {
            DirectiveCategory::Utility if bare == "exit" => {
                require_glob_args(&name, arguments)?;
                let armed = self.plan.exit_armed();
                let v = self.handle_exit(None, ctx)?;
                self.mark_exit_step(Some(ctx), armed)?;
                Ok(v)
            }
            // A built-in has a descriptor, and no descriptor declares the
            // user category: that category is the absence of one.
            DirectiveCategory::User => Err(internal(format!(
                "built-in directive '{}' carries the user category",
                name
            ))),
            // `run_namespace!` is legal in effect
            // bodies — its target's rules already exist when the body is
            // compiled. The demand is an inline sub-invocation of the
            // target namespace's `main!` (pinned by the
            // effects ball's main--24_run_namespace_nested).
            DirectiveCategory::Execution if bare == "run_namespace" => {
                let target_ns = run_target_from_args(&name, arguments)?;
                self.invoke_namespace_main(&target_ns, ctx)
            }
            DirectiveCategory::Dml(_) | DirectiveCategory::Ddl => Err(unsupported(format!(
                "expression-position '{}' is not supported in v0.1 effect bodies; \
                 write the pipe form ('… |> {}(…)')",
                name, name
            ))),
            // doc! is a ratified exception ("annotation only — it writes
            // documentation, never shape"): LEGAL in effect bodies, so the
            // refusal must not cite the general directive refusal for the
            // thing this exception permits.
            // Its lowering is deferred, not ruled out — a scheduling gap.
            // Pinned by the effects ball's rules--50_doc_in_body_deferred.
            DirectiveCategory::Session if bare == "doc" => Err(unsupported(format!(
                "'{}' is not supported in v0.1 effect bodies — EFFECT-ALGEBRA \
                 R9 permits doc! in a body (annotation only); its lowering is \
                 deferred",
                name
            ))),
            DirectiveCategory::Session | DirectiveCategory::Execution => {
                // A belt — consult-time validation already refuses these.
                Err(unsupported(format!(
                    "'{}' cannot execute inside a compiled effect body (EFFECT-ALGEBRA R9)",
                    name
                )))
            }
            // A utility pipe terminal invoked with nothing piped: the
            // descriptor's own policy refusal, the same teaching the entity
            // road gives.
            DirectiveCategory::Utility => Err(effects::pipe_terminal_policy_refusal(&name)),
        }
    }

    /// A pipe operator is PURE — a directive call is a relation-position
    /// call and reaches [`Self::walk_directive_terminal`] from the read
    /// walk, never as an operator. This walk fuses the canonical `returned`
    /// release when the descriptor licenses it, then passes the operator
    /// through around the walked source.
    pub(super) fn walk_pipe(
        &mut self,
        source: Chain,
        operator: PipeOp,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        // OBSERVED-PAYLOAD FUSION: when the
        // immediately-following operator is the EXACT `returned` release
        // (`!>`'s normalization — glob drill, no narrowing, no
        // groundings) and the descriptor PROVES the payload's relational
        // provenance, substitute the originating relation instead of
        // constructing and re-expanding a JSON interior. This is
        // semantic, not just cost: a JSON round trip cannot represent
        // every backend value. Dispatch is by declared provenance —
        // `ReceiptPayload::Input` / `OtherRelation` — never a name list;
        // a future input-returning directive fuses by declaration alone.
        // Produced/arbitrary payloads and any other observation keep the
        // general receipt semantics.
        // The trigger is the FULL canonical release — the two-operator
        // shape `!>` (and the longhand `|> .returned(*)`) normalize to:
        // the glob drill into `returned` followed by the interior-heading
        // projection `(returned.*)`. Fusing on the drill alone would be
        // wrong twice over: the trailing projection would go stale, and
        // the context-KEEPING postfix drill (`receipt.returned(*)`) must
        // keep its receipt context.
        let source = if is_returned_heading_projection(&operator) {
            let drill = source
                .continuations()
                .last()
                .map(Step::form)
                .is_some_and(is_returned_glob_drill);
            if drill {
                let mut inner = source;
                let Some(drill_step) = inner.continuations_mut().pop() else {
                    unreachable!("just matched a drill")
                };
                debug_assert!(matches!(
                    drill_step.form(),
                    Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                        form: crate::pipeline::asts::core::StructuralForm::Drill { .. },
                        ..
                    })
                ));
                match self.try_fuse_released_payload(inner, Access::Unasked, ctx)? {
                    FuseOutcome::Fused(v) => return Ok(v),
                    FuseOutcome::NotApplicable(s) => s.then(drill_step),
                }
            } else {
                source
            }
        } else {
            source
        };
        // Every operator is pure: pass through. But a pure operator's
        // argument domain expressions can still hide a directive in a scalar
        // subquery — that is not lowered on the spine, so refuse it honestly.
        if effects::operator_demands_directive(&operator) {
            return Err(effect_head_predicate_unsupported(
                "a pipe operator argument",
            ));
        }
        let walked_source = self.walk_value(source, &ctx.without_sink())?;
        Ok(make_pipe(walked_source, operator))
    }

    /// A directive TERMINAL and the source flowing into it: the effect
    /// machinery's dispatch over what the directive is. The call is the
    /// relation-position call identity; no operator carrier stands between
    /// the source and the terminal.
    /// A BUILT-IN DIRECTIVE IN RELATION POSITION, with the relation its
    /// descriptor consumes detached under that descriptor's layout. A user
    /// rule never arrives here: its row is admitted whole against its own
    /// declaration by [`Self::invoke_rule`], and there is no descriptor
    /// layout to read it under.
    pub(super) fn walk_directive_terminal(
        &mut self,
        builtin: crate::pipeline::asts::effects::DirectiveKind,
        source: Chain,
        call: SealedCall,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        use crate::pipeline::asts::effects::DirectiveKind as K;

        // A rule designator has its own typed carrier. A bare local name can
        // initially look relation-shaped, while a configured or consulted
        // value is already `Rule`; both enter the ordinary residual judgment
        // below. Dispatch therefore follows the directive identity, never a
        // guess based on the designator's provisional carrier.
        if builtin == K::Assert {
            let property = call
                .call()
                .arguments
                .ho_members()
                .find_map(|member| match member {
                    HoArgument::Rule(rule) | HoArgument::Relation(rule) => Some(rule.clone()),
                    HoArgument::Landed(_)
                    | HoArgument::Value(_)
                    | HoArgument::Landing(_)
                    | HoArgument::Skip => None,
                })
                .ok_or_else(|| internal("assert! has no property designator".to_string()))?;
            let values = call
                .call()
                .arguments
                .ho_members()
                .filter_map(|member| member.scalar_domain().cloned())
                .collect::<Vec<_>>();
            return self.walk_assert_terminal(source, property, &values, receipt, ctx, false);
        }

        if call.call().relations().next().is_none() {
            {
                let name = call.call().callee.name_text();
                let arguments = call
                    .call()
                    .arguments
                    .ho_members()
                    .filter_map(|argument| argument.scalar_domain().cloned())
                    .chain(
                        call.call()
                            .arguments
                            .scalar_members()
                            .iter()
                            .filter_map(|member| member.scalar_domain().cloned()),
                    )
                    .collect::<Vec<_>>();
                let bare = bare_name(&name).to_string();
                match builtin {
                    // DDL directives.
                    K::TempTable | K::TempView | K::Table => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let target = single_name_argument(&name, &arguments)?;
                        let armed = self.plan.exit_armed();
                        let v = self.handle_ddl(walked_source, &bare, &target, ctx)?;
                        self.mark_step(MarkedStepKind::Ddl, &bare, Some(ctx), armed)?;
                        Ok(v)
                    }
                    // stdout! ships and passes through.
                    K::Stdout => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let armed = self.plan.exit_armed();
                        let v = self.handle_stdout(walked_source, ctx)?;
                        self.mark_step(MarkedStepKind::Host, "stdout", Some(ctx), armed)?;
                        Ok(v)
                    }
                    // returning! packages the piped relation in its
                    // receipt's `returned` payload.
                    K::Returning => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        Ok(Self::inline_payload_receipt(walked_source, "returning"))
                    }
                    // Piped exit!: the piped relation is the exit condition.
                    K::Exit => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let armed = self.plan.exit_armed();
                        let v = self.handle_exit(Some(walked_source), ctx)?;
                        self.mark_exit_step(Some(ctx), armed)?;
                        Ok(v)
                    }
                    // abort! is a typed erroneous terminal. The runner tests
                    // the lowered input relation directly; no backend error
                    // is manufactured and no exit latch is reused.
                    K::Abort => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let label = abort_label(&name, &arguments)?;
                        let armed = self.plan.exit_armed();
                        let provenance = compiled_query::AbortProvenance::Authored { label };
                        let (v, probe) = self.handle_abort(walked_source, ctx)?;
                        self.mark_abort_step(probe, provenance, "abort", Some(ctx), armed)?;
                        Ok(v)
                    }
                    // The standalone two-paren form `run_namespace!(ns)(*)`
                    // parses as a one-row anonymous source (carrying the
                    // namespace argument) piped into the terminal. Legal in
                    // bodies, an inline sub-invocation
                    // of the target's main! (effects ball main--24).
                    K::RunNamespace => {
                        let target_ns = run_target_from_source(&name, &source)?;
                        self.invoke_namespace_main(&target_ns, ctx)
                    }
                    K::Run => Err(unsupported(format!(
                        "'{}!' consults and cannot execute inside a compiled \
                         effect body (EFFECT-ALGEBRA R9): consult before the \
                         run, then demand with run_namespace!",
                        bare
                    ))),
                    // Declared identities without a one-group effect-body
                    // realization — the POLICY refusal, never a fallthrough
                    // that mistakes a declared identity for a user rule.
                    K::Consult
                    | K::ConsultTree
                    | K::Reconsult
                    | K::Unconsult
                    | K::Mount
                    | K::MountNew
                    | K::MountTree
                    | K::Unmount
                    | K::Refresh
                    | K::Ground
                    | K::Enlist
                    | K::Delist
                    | K::Alias
                    | K::Expose
                    | K::Doc
                    | K::Imprint
                    | K::ImprintReplace
                    | K::Insert
                    | K::Update
                    | K::Delete
                    | K::ReturningOther
                    | K::Assert => Err(unsupported(format!(
                        "piped directive '{}' is not supported in v0.1 effect bodies",
                        name
                    ))),
                }
            }
        } else {
            // returning_other! — piped input evaluated first
            // (its effects happen), then discarded; the argument returns.
            {
                let name = call.call().callee.name_text();
                // THE TARGET IS A PARAMETER: on this road the piped relation
                // rides the spine, so the group's first relation is the
                // designator the author wrote.
                let argument = call
                    .call()
                    .relations()
                    .next()
                    .cloned()
                    .ok_or_else(|| internal("two-paren effect call has no target relation"))?;
                let access = receipt.clone();
                let bare = bare_name(&name).to_string();
                // The DDL target is a preserved relational
                // DESIGNATOR — a whole-table access, optionally
                // namespace-qualified. Its structure is interpreted
                // deliberately or refused; never silently discarded.
                // The DML target arrives
                // as the same preserved relational DESIGNATOR the DDL path
                // carries; interpreted deliberately or refused — never a
                // string minted by the parser.
                let dml_kind = match Some(builtin.descriptor().category) {
                    Some(crate::pipeline::asts::effects::DirectiveCategory::Dml(verb)) => {
                        Some(verb)
                    }
                    _ => None,
                };
                if let Some(kind) = dml_kind {
                    let (target, target_namespace) = effects::target_designator(
                        &bare,
                        |message| crate::diagnostic::Effect::DmlTargetDesignator { message }.into(),
                        "naming where to write",
                        &argument,
                    )?;
                    let walked_source = self.walk_value(source, &ctx.without_sink())?;
                    let armed = self.plan.exit_armed();
                    let v = self.handle_dml(
                        walked_source,
                        kind,
                        target,
                        target_namespace,
                        argument,
                        call.call().callee.clone(),
                        access,
                        ctx,
                    )?;
                    self.mark_step(MarkedStepKind::Dml, &bare, Some(ctx), armed)?;
                    return Ok(v);
                }
                if matches!(builtin, K::Table | K::TempTable | K::TempView) {
                    require_whole_access(&name, &access)?;
                    let (target, target_namespace) = effects::target_designator(
                        &bare,
                        |message| crate::diagnostic::EffectDdl::TargetDesignator { message }.into(),
                        "naming where to create",
                        &argument,
                    )?;
                    let walked_source = self.walk_value(source, &ctx.without_sink())?;
                    let armed = self.plan.exit_armed();
                    let v = self.handle_ddl_namespaced(
                        walked_source,
                        &bare,
                        &target,
                        target_namespace.as_deref(),
                        ctx,
                    )?;
                    self.mark_step(MarkedStepKind::Ddl, &bare, Some(ctx), armed)?;
                    return Ok(v);
                }
                if builtin != K::ReturningOther {
                    return Err(unsupported(format!(
                        "piped two-paren directive '{}' is not supported in the v0.1 \
                         effect transformer",
                        name
                    )));
                }
                require_whole_access(&name, &access)?;
                // Ordering: the piped input's effects happen first; its
                // value is discarded as data (a sequencing directive). The
                // receipt packages the OTHER relation.
                let _ = self.walk_value(source, &ctx.without_sink())?;
                let walked_argument = self.walk_value(argument, ctx)?;
                Ok(Self::inline_payload_receipt(
                    walked_argument,
                    "returning_other",
                ))
            }
        }
    }

    pub(super) fn walk_assert_terminal(
        &mut self,
        source: Chain,
        property: Chain,
        values: &[DomainExpression],
        receipt: Access,
        ctx: &WalkCtx,
        release: bool,
    ) -> Result<Chain> {
        require_whole_access("assert!", &receipt)?;
        if values.len() > 1 {
            return Err(DelightQLError::from(DirectiveBinding::Arity {
                message: "assert! accepts one property and an optional label".to_string(),
            }));
        }
        let label = match values.first() {
            Some(value) => run_target_from_value(value).ok_or_else(|| {
                DelightQLError::from(DirectiveBinding::Value {
                    message: "assert! label must be a string or bare name".to_string(),
                })
            })?,
            None => format!("assert!#{}", self.plan.step_count()),
        };
        let walked_source = self.walk_value(source, &ctx.without_sink())?;
        let armed = self.plan.exit_armed();
        let provenance = compiled_query::AbortProvenance::Assertion {
            label: label.clone(),
        };
        let (receipt, returned, probe) = self.handle_assert(property, walked_source, label, ctx)?;
        self.mark_abort_step(probe, provenance, "assert", Some(ctx), armed)?;
        Ok(if release { returned } else { receipt })
    }

    pub(super) fn refuse_if_effectful(&self, expr: &Chain) -> Result<()> {
        let invocations = effects::collect_directive_invocations(expr);
        if let Some(inv) = invocations.first() {
            return Err(unsupported(format!(
                "directive '{}' appears in a nested position the v0.1 effect \
                 transformer does not lower",
                inv.name
            )));
        }
        Ok(())
    }
}

pub(super) fn bare_name(name: &str) -> &str {
    name.strip_suffix('!').unwrap_or(name)
}

/// The demanded identifier without its `!`, stropping carried — never
/// reconstructed from characters. The `!` is call identity, not spelling:
/// an effect-CTE label stores the bare subject, so the demand's bare
/// spelling is what agrees with it.
pub(super) fn bare_demand_identifier(
    demanded: &delightql_types::SqlIdentifier,
) -> delightql_types::SqlIdentifier {
    let text = demanded
        .as_str()
        .strip_suffix('!')
        .unwrap_or_else(|| demanded.as_str());
    if demanded.is_stropped() {
        delightql_types::SqlIdentifier::stropped(text)
    } else {
        delightql_types::SqlIdentifier::new(text)
    }
}

pub(super) fn dml_kind_name(kind: &DmlVerb) -> &'static str {
    match kind {
        DmlVerb::Insert => "insert",
        DmlVerb::Update => "update",
        DmlVerb::Delete => "delete",
    }
}

/// A directive demanded inside a predicate subquery under an EFFECT
/// head is LEGAL IN PRINCIPLE (a directive is a
/// relation and composes wherever a relational expression occurs). Its
/// predicate-position lowering is simply not built yet. Refuse it with an honest
/// limitation diagnostic — deliberately NOT a purity refusal: purity refusals
/// govern PURE heads, but the transformer only ever runs on registered EFFECT
/// rules, so that does not apply here (that is exactly why detection under a
/// pure head is closed separately, at consult, by the demand walker). The
/// correlated case is likewise refused for now; both surface this message.
/// Pinned by the effects ball's
/// rules--85/86/87_effecthead_predicate_{in,exists,scalar}.
pub(super) fn effect_head_predicate_unsupported(position: &str) -> DelightQLError {
    DelightQLError::from(Effect::PredicateUnsupported {
        message: format!(
            "a directive is demanded inside {position} under an effect head. A \
             directive composes wherever a relational expression occurs \
             (EFFECT-ALGEBRA E1a corollary), so this is legal in principle — but \
             its predicate-position lowering is not yet supported in v0.1. Demand \
             the directive in a top-level pipeline position instead — an arm, or \
             an effect-CTE ': name!' demanded on the main pipeline — not inside a \
             predicate or argument subquery. (Referencing an effect-CTE as \
             'name!(*)' from within the predicate does NOT help: the reference is \
             itself the demand, E2.)"
        ),
    })
}

/// A demand that declares no scalar parameter takes VALUES from nobody.
/// The access glob is not among these — it enumerates rather than
/// supplying — so an argument standing here is one too many.
pub(super) fn require_glob_args(name: &str, arguments: &[DomainExpression]) -> Result<()> {
    let ok = arguments.is_empty();
    if ok {
        Ok(())
    } else {
        Err(reshaping_argument_list(name))
    }
}

/// THE SAME LAW OVER THE WHOLE ROW: an effect label declares nothing a
/// member could bind, so its demand is bare — no group, or the one glob
/// that spells "whole".
fn require_bare_demand(
    name: &str,
    arguments: &crate::pipeline::asts::core::operators::CallArguments<Unresolved>,
) -> Result<()> {
    use crate::pipeline::asts::core::operators::{CallArguments, ScalarArgument};
    let bare = match arguments {
        CallArguments::None => true,
        CallArguments::Scalar(members) => matches!(
            members.as_slice(),
            [] | [ScalarArgument::Spread(
                crate::pipeline::asts::core::Spread::Glob(_)
            )]
        ),
        CallArguments::HigherOrder(_) => false,
    };
    if bare {
        Ok(())
    } else {
        Err(reshaping_argument_list(name))
    }
}

fn reshaping_argument_list(name: &str) -> DelightQLError {
    unsupported(format!(
        "'{}' is invoked with a reshaping argument list; only the glob form \
         '{}(*)' is supported in v0.1 effect bodies",
        name, name
    ))
}

pub(super) fn require_whole_access(name: &str, spec: &Access) -> Result<()> {
    if spec.is_whole() {
        Ok(())
    } else {
        Err(unsupported(format!(
            "'{}' with a reshaping access spec is not supported in v0.1; use '(*)'",
            name
        )))
    }
}

/// Extract the single bare-name argument of `temp_table!(staged(*))(*)`.
///
/// THE TARGET IS A PARAMETER. One group is receipt access, so a directive
/// written with only one names no target at all — the refusal below says so.
pub(super) fn single_name_argument(name: &str, arguments: &[DomainExpression]) -> Result<String> {
    if arguments.len() == 1 {
        if let DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
            name: table,
            qualifier: None,
            ..
        }))) = &arguments[0]
        {
            return Ok(table.to_string());
        }
    }
    Err(unsupported(format!(
        "'{}' takes exactly one bare object name as its PARAMETER \
         (e.g. '{}(staged)(*)'); one group is receipt access and names no target",
        name, name
    )))
}

/// The namespace argument of a directive-call `run_namespace!(ns)` —
/// a bare/`::`-qualified name (carried as an Lvar with the `::` text
/// intact) or a string literal.
pub(super) fn run_target_from_args(name: &str, arguments: &[DomainExpression]) -> Result<String> {
    if arguments.len() == 1 {
        if let Some(ns) = run_target_from_value(&arguments[0]) {
            return Ok(ns);
        }
    }
    Err(unsupported(format!(
        "'{}' takes exactly one namespace argument (e.g. 'run_namespace!(etl)')",
        name
    )))
}

/// The namespace argument of the two-paren form `run_namespace!(ns)(*)`,
/// which the builder spells as a one-row anonymous source (holding the
/// argument) piped into the terminal.
pub(super) fn run_target_from_source(name: &str, source: &Chain) -> Result<String> {
    if let (GroundForm::Literal(anon), true) =
        (source.head().form(), source.continuations().is_empty())
    {
        let rows = &anon.table.body.rows;
        if rows.len() == 1 {
            let row = rows.first();
            if row.len() == 1 {
                if let Some(ns) = run_target_from_value(&row.0.first().value()) {
                    return Ok(ns);
                }
            }
        }
    }
    Err(unsupported(format!(
        "'{}' takes exactly one namespace argument (e.g. 'run_namespace!(etl)(*)')",
        name
    )))
}

pub(super) fn run_target_from_value(value: &DomainExpression) -> Option<String> {
    match value {
        DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
            name,
            qualifier: None,
            ..
        }))) => Some(name.to_string()),
        DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Ground(
                crate::pipeline::asts::core::literals::LiteralValue::String(s),
            ),
        ) => Some(s.clone()),
        _ => None,
    }
}

/// `abort!(label?)`: the authored label is occurrence prose under the one
/// fixed `authored/abort` identity. Authors mint no identity here.
pub(super) fn abort_label(name: &str, arguments: &[DomainExpression]) -> Result<String> {
    if arguments.len() > 1 {
        return Err(DirectiveBinding::Arity {
            message: format!(
                "{name} expects at most a label: write abort!(\"label\")(*) or abort!()(*)"
            ),
        }
        .into());
    }
    match arguments.first() {
        Some(value) => run_target_from_value(value).ok_or_else(|| {
            DirectiveBinding::Value {
                message: format!("{name}'s label must be a string or bare name"),
            }
            .into()
        }),
        None => Ok(name.to_string()),
    }
}

pub(super) fn make_pipe(source: Chain, operator: PipeOp) -> Chain {
    source.then(Step::authored(Continuation::Pipe {
        operator: operator,
        named: None,
    }))
}

/// A bare glob read of a scratch row the plan allocated, by its receipt.
/// Resolution follows the receipt directly; no character-bearing lookup
/// key exists.
pub(super) fn scratch_read(row: crate::relation::ScratchRow) -> Chain {
    Chain::read(
        Relation::Ground {
            mention: GroundMention::Scratch { row },
            outer: false,
        },
        Access::All,
    )
}

/// A read of a user-named object created by the plan. These characters are
/// authored vocabulary and remain query-local lookup keys.
pub(super) fn named_ground_read(table: &str) -> Chain {
    Chain::read(
        Relation::Ground {
            mention: GroundMention::Named {
                identifier: QualifiedName {
                    namespace_path: crate::pipeline::ast_unresolved::NamespacePath::empty(),
                    name: table.into(),
                },
                alias: None,
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
        },
        Access::All,
    )
}

/// Does this pure value expression carry a signed witness that
/// `lower_witness_union` must lower (top-level, or as a union arm)?
///
/// KEPT LOCAL (not routed through `Chain::fold_tail`): by contract this is a
/// TOP-LEVEL check — the chain's own trailing pipe, or a bag arm. Unlike the
/// tail fold it does NOT descend a member's right-hand chain, so routing it
/// there would over-recurse and change results.
#[stacksafe::stacksafe]
pub(super) fn value_contains_witness(expr: &Chain) -> bool {
    // Top-level-by-contract: a signed witness is recognized only as the
    // chain's own trailing pipe or inside a bag arm. Restrictions, members
    // and ER edges are DELIBERATELY not descended.
    match expr
        .split_last()
        .map(|(step, prefix)| (step.form(), prefix))
    {
        Some((
            Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::SignedWitness,
                ..
            }),
            _,
        )) => true,
        Some((Continuation::BagOp { arm, .. }, prefix)) => {
            value_contains_witness(&prefix.to_chain()) || value_contains_witness(arm)
        }
        _ => false,
    }
}

/// The tail directive of `expr` when that directive's receipt declares NO
/// `returned` payload (DML and DDL terminals today — the descriptor's
/// deliberately preserved absence). Drives the category-error teaching
/// diagnostic for `.returned(*)` / `!>` over such receipts.
/// The invocation a tail leaf ends in: a trailing pipe's call operator, or
/// the head when nothing has consumed it.
pub(super) fn leaf_terminal_call(leaf: &Chain) -> Option<&SealedCall> {
    // A RECEIPT STANDS AFTER ITS DIRECTIVE, so the terminal is what stands
    // under a trailing access — including the mention's own, when the
    // directive heads the chain.
    let mut steps = leaf.steps();
    while let Some((step, rest)) = steps.split_last() {
        if !matches!(step.form(), Continuation::Access { .. }) {
            break;
        }
        steps = rest;
    }
    match steps.last() {
        None => match leaf.head().form() {
            GroundForm::Reference(Relation::FunctorCall { call, .. }) => Some(call),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn tail_payload_free_directive(expr: &Chain) -> Option<String> {
    use crate::pipeline::asts::effects::ReceiptPayload;
    expr.fold_tail(
        &|leaf: &Chain| -> Option<String> {
            let call = leaf_terminal_call(leaf)?;
            let name = call.call().callee.name_text();
            match effects::descriptor_for_reference(&call.call().callee) {
                Some(d) if d.receipt_payload == ReceiptPayload::None => Some(name),
                _ => None,
            }
        },
        &|arms: Vec<Option<String>>| {
            // Every arm must be payload-free for the union's release to be
            // the category error; a mixed union flows to ordinary
            // resolution.
            let names: Vec<String> = arms.into_iter().collect::<Option<_>>()?;
            names.into_iter().next()
        },
    )
}

/// The tail-LEAF kind for consolidation: `(shape, self_sinking)`.
/// DML/DDL terminals write their own receipts into the shared shell
/// (`self_sinking = true`); receipt-era compositional endings — the
/// utility payload producers and nested user directives — have the
/// universal receipt shape and are sunk by the invocation loop
/// (`self_sinking = false`). `None` = not a receipt-producing ending.
pub(super) fn ending_kind(expr: &Chain) -> Option<(Vec<String>, bool)> {
    expr.fold_tail(
        &|leaf: &Chain| -> Option<(Vec<String>, bool)> {
            if let Some(shape) = ending_receipt_leaf(leaf) {
                return Some((shape, true));
            }
            let universal = || {
                Some((
                    vec![
                        "success".to_string(),
                        "operation".to_string(),
                        "returned".to_string(),
                    ],
                    false,
                ))
            };
            let call = leaf_terminal_call(leaf)?;
            // THE DESCRIPTOR DECLARES HOW ITS RECEIPT ENDS A CLAUSE; a user
            // directive has the universal shape.
            match effects::descriptor_for_reference(&call.call().callee) {
                None => universal(),
                Some(descriptor) => match descriptor.ledger {
                    crate::pipeline::asts::effects::LedgerEnding::Universal => universal(),
                    crate::pipeline::asts::effects::LedgerEnding::SelfSinking
                        if call.call().relations().next().is_some() =>
                    {
                        Some((receipt_shape(descriptor), true))
                    }
                    crate::pipeline::asts::effects::LedgerEnding::SelfSinking
                    | crate::pipeline::asts::effects::LedgerEnding::NotAnEnding => None,
                },
            }
        },
        &|arms: Vec<Option<(Vec<String>, bool)>>| {
            let kinds: Vec<(Vec<String>, bool)> = arms.into_iter().collect::<Option<_>>()?;
            let mut merged: Vec<String> = Vec::new();
            let mut self_sinking = true;
            for (shape, sinks) in kinds {
                self_sinking &= sinks;
                for c in shape {
                    if !merged.contains(&c) {
                        merged.push(c);
                    }
                }
            }
            Some((merged, self_sinking))
        },
    )
}

/// A self-sinking terminal's receipt SHAPE, read from its descriptor's
/// declared echoes (descriptor authority): the
/// guaranteed core followed by the ledger-ordered echo names.
pub(super) fn receipt_shape(
    desc: &crate::pipeline::asts::effects::DirectiveDescriptor,
) -> Vec<String> {
    let mut shape = vec!["success".to_string(), "operation".to_string()];
    shape.extend(desc.receipt_echoes.iter().map(|e| e.name.to_string()));
    shape
}

/// Zip a terminal's descriptor-declared echo NAMES with this emission's
/// VALUES, in ledger order: the emitter supplies only
/// values; the names are the descriptor's. An arity mismatch is an
/// internal invariant violation and panics rather than emitting a
/// receipt the declared ledger disowns.
pub(super) fn descriptor_echo_values(name: &str, values: Vec<String>) -> Vec<(String, String)> {
    let desc =
        effects::descriptor(name).unwrap_or_else(|| panic!("no directive descriptor for '{name}'"));
    assert_eq!(
        desc.receipt_echoes.len(),
        values.len(),
        "'{name}': echo values disagree with the descriptor's declared echoes"
    );
    desc.receipt_echoes
        .iter()
        .zip(values)
        .map(|(e, v)| (e.name.to_string(), v))
        .collect()
}

/// The tail-LEAF half of `ending_receipt_columns`: the echo columns of THIS tail
/// node when it is a sinkable DML/DDL terminal, else `None`.
pub(super) fn ending_receipt_leaf(expr: &Chain) -> Option<Vec<String>> {
    // A tail leaf that is not an invocation is not a sinkable terminal; its
    // recursive fields are DELIBERATELY not descended (the tail contract).
    let call = leaf_terminal_call(expr)?;
    let descriptor = effects::descriptor_for_reference(&call.call().callee)?;
    (call.call().relations().next().is_some()
        && descriptor.ledger == crate::pipeline::asts::effects::LedgerEnding::SelfSinking)
        .then(|| receipt_shape(descriptor))
}

/// All bare Ground relation names an expression reads (the hazard
/// detector's input).
///
/// Rides the shared whole-tree closure `AstVisit`: a walker that only
/// descends `Filter.source` and `pipe.source` misses every other
/// query-bearing edge — `Filter.condition`, `correlation`, pipe-OPERATOR
/// argument subqueries, and so on — so a hazardous plan-created view read
/// only inside an IN/EXISTS/scalar predicate would fall out of the
/// candidate set. Its closure COINCIDES with the paired rewrite
/// `rename_ground_reads` (both centralized, both proven complete by
/// `p1_closure_matrix_detection_and_rewrite_agree`).
pub(super) fn collect_ground_names(expr: &Chain) -> HashSet<String> {
    let mut c = GroundNameCollector::default();
    // The collector's hook never fails, so the walk is infallible.
    let _ = walk_visit_relational(&mut c, expr);
    c.out
}

/// The `AstVisit` tenant for ground-read detection.
#[derive(Default)]
struct GroundNameCollector {
    out: HashSet<String>,
}

impl AstVisit<Unresolved> for GroundNameCollector {
    fn enter_relation(&mut self, r: &Relation) -> Result<Descent> {
        if let Relation::Ground {
            mention:
                GroundMention::Named {
                    identifier,
                    mutation_target,
                    passthrough,
                    ..
                },
            ..
        } = r
        {
            if identifier.namespace_path.is_empty() && !mutation_target && !passthrough {
                self.out.insert(identifier.name.to_string());
            }
        }
        Ok(Descent::Continue)
    }
}

/// Rewrite bare Ground reads of `from` into reads of `to` (the
/// snapshot substitution).
///
/// Rides the shared cross-phase spine `AstTransform<Unresolved, Unresolved>`.
/// The default same-phase walk
/// already descends the WHOLE tree — `Filter.condition`, `correlation`, pipe
/// operator arguments, InnerRelation subqueries — so the snapshot name is
/// substituted at EVERY read, not only on the source spine. Its closure
/// COINCIDES with the paired detection
/// `collect_ground_names` (both centralized recursion schemes, both proven
/// complete by `p1_closure_matrix_detection_and_rewrite_agree`).
/// Every read of the named relation reads the receipt instead: the row was
/// paired with the name by the plan that materialized it, and the read
/// stays an authored access under that name.
pub(super) fn rename_ground_reads(expr: Chain, to: crate::relation::NamedScratch) -> Chain {
    let mut r = GroundReadRenamer { to };
    // A same-phase Ground-identifier rewrite never fails.
    r.transform_relational(expr)
        .expect("ground-read rename is infallible")
}

struct GroundReadRenamer {
    to: crate::relation::NamedScratch,
}

impl AstTransform<Unresolved, Unresolved> for GroundReadRenamer {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

    /// THE LEAF THIS RENAME REWRITES is what a ground read names: the
    /// authored mention, at every read the walk reaches.
    fn transform_mention(&mut self, mention: GroundMention) -> Result<GroundMention> {
        match mention {
            GroundMention::Named {
                identifier,
                alias,
                mutation_target,
                passthrough,
            } => {
                let is_rewritten = identifier.namespace_path.is_empty()
                    && !mutation_target
                    && !passthrough
                    && identifier.name.as_str() == self.to.name().as_str();
                if is_rewritten {
                    // The access beside this read is walked in its own right,
                    // so a scalar subquery inside a positional argument takes
                    // part in the same whole-tree rewrite.
                    return Ok(GroundMention::Receipt {
                        receipt: self.to.clone(),
                        alias,
                    });
                }
                Ok(GroundMention::Named {
                    identifier,
                    alias,
                    mutation_target,
                    passthrough,
                })
            }
            other => Ok(other),
        }
    }
}
