// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// REFINER V2 - PRINCIPLED FAR (Flatten-Analyze-Rebuild) ARCHITECTURE
//
// - FJC/FIC/F/Fx/F! predicate classification
// - Laws 1-6 enforcement
// - FAR (Flatten-Analyze-Rebuild) cycle
//
// EXISTS HANDLING:
// - The `+table` syntax creates an InnerExists predicate in the AST
// - InnerExists predicates flow through the FAR cycle as regular predicates
// - They're classified as F (filter) predicates since they filter the source
// - The transformer converts InnerExists to SQL EXISTS subqueries
// - NOT EXISTS uses `-table` syntax and becomes InnerExists with exists=false
//
// The refiner is a single AstTransform<Resolved, Refined> implementation.
// Classification of InnerRelation patterns happens inline in
// transform_relation, which handles operator descent by construction: a
// separate pre-pass classifier is the road back to the classify_operator()
// no-op bug.

mod analyzer;
mod bag;
pub(crate) mod carry;
mod cdt_wj_rewriter;
pub(crate) mod correlation_analyzer;
mod flattener;
mod laws;
mod limit_placement;
pub(crate) mod pattern_classifier;
#[cfg(test)]
mod phase_boundary_tests;
mod rebuilder;
mod settled;
mod types;

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::{walk_transform_relation, AstTransform, FoldAction};
use crate::pipeline::asts::refined::Refined;
use crate::pipeline::asts::resolved::Resolved;
use crate::pipeline::asts::{refined, resolved};

// =============================================================================
// RefinerFold — AstTransform<Resolved, Refined>
// =============================================================================

struct RefinerFold<'a> {
    is_top_level: bool,
    danger_gates: crate::pipeline::danger_gates::DangerGateMap,
    identities: &'a crate::relation::Planning,
}

impl RefinerFold<'_> {
    /// Refine ONE set operand, and report what happened to its relation.
    ///
    /// Hoisting a witness into a join, or binding an outer context onto a
    /// ground read, replaces the relation the step's evidence names. The
    /// authority holds what goes in, reads what comes out of the refinement
    /// it ran, and records the map lowering translates an old port through.
    /// Nothing here pairs an old relation with a new one: there is no pair
    /// to assemble, and no second call that could register the wrong one.
    fn refine_operand(&mut self, expr: resolved::Chain) -> Result<refined::Chain> {
        Ok(self.transform_relational_action(expr)?.into_inner())
    }

    fn is_dml_call(&self, relation: &resolved::Relation) -> bool {
        let resolved::Relation::FunctorCall { call, .. } = relation else {
            return false;
        };
        matches!(
            self.identities.callable_category(call.call().callee),
            Some(crate::names::CallableCategory::Dml(_))
        )
    }
}

impl RefinerFold<'_> {
    /// Refine a chain standing on a bag step.
    ///
    /// Two things happen, in this order and nowhere else. The predicates
    /// standing on the run are settled: each one either names two of the
    /// run's arms — and becomes that pair's correlation, written onto the
    /// step owning the later arm — or stands over the one relation the run
    /// publishes, and remains a filter. Then the outermost continuation is
    /// descended, its operands refined as the chains they are.
    #[stacksafe::stacksafe]
    fn refine_bag_chain(&mut self, expr: resolved::Chain) -> Result<refined::Chain> {
        let expr = self.claim_bag_correlations(expr)?;
        let peeled = expr.peel().map_err(|_| {
            crate::error::DelightQLError::transformation_error(
                "a chain standing on a bag step ends in a step or a predicate",
                "refiner",
            )
        })?;
        match peeled.last().form() {
            resolved::Continuation::Restrict { .. } => {
                peeled.crossing(self, |walk, prefix, form, _| {
                    let resolved::Continuation::Restrict { condition, origin } = form else {
                        unreachable!("the step was just matched as a restriction")
                    };
                    let source = walk.transform_relational_action(prefix)?.into_inner();
                    let condition =
                        rebuilder::refine_predicate_boolean(condition, &walk.identities)?;
                    Ok((
                        source,
                        refined::Continuation::Restrict { condition, origin },
                    ))
                })
            }
            resolved::Continuation::BagOp { .. } => {
                peeled.crossing(self, |walk, prefix, form, carrier| {
                    let resolved::Continuation::BagOp {
                        operator,
                        arm,
                        correlation,
                    } = form
                    else {
                        unreachable!("the step was just matched as a bag operation")
                    };
                    // Minus is minus: the left rows with no corresponding row in
                    // the arm, duplicates preserved and nulls matching nulls.
                    // That is the whole-tuple anti-semijoin, so a BARE minus is
                    // a correlated minus whose predicate is filled in here —
                    // bare and correlated never become two roads, and there is
                    // no `EXCEPT` for the multiset law to be lost in.
                    let correlation = match (correlation, operator) {
                        (None, resolved::SetOperator::MinusCorresponding) => {
                            Some(resolved::BagCorrelation {
                                with_arm: crate::pipeline::asts::vocabulary::ArmIx::from_raw(0),
                                predicate: resolved::CorrPred::Expression(
                                    bag::whole_tuple_correlation(*carrier, &walk.identities)?,
                                ),
                                min_multiplicity: false,
                            })
                        }
                        (correlation, _) => correlation,
                    };
                    // ONE CALL PER OPERAND, through the authority.
                    let left = walk.refine_operand(prefix)?;
                    let arm = walk.refine_operand(arm)?;
                    let correlation = match correlation {
                        Some(correlation) => Some(refined::BagCorrelation {
                            with_arm: correlation.with_arm,
                            // The whole-heading form travels WHOLE. It is not
                            // expanded here and it is not a predicate to refine:
                            // the mode it aligns by is what the lowering reads.
                            predicate: match correlation.predicate {
                                resolved::CorrPred::Expression(predicate) => {
                                    refined::CorrPred::Expression(
                                        rebuilder::refine_predicate_boolean(
                                            predicate,
                                            &walk.identities,
                                        )?,
                                    )
                                }
                                resolved::CorrPred::Whole(whole) => {
                                    refined::CorrPred::Whole(match whole {
                                        resolved::WholeHeading::ByName { left, right } => {
                                            refined::WholeHeading::ByName { left, right }
                                        }
                                        resolved::WholeHeading::ByPosition { left, right } => {
                                            refined::WholeHeading::ByPosition { left, right }
                                        }
                                    })
                                }
                            },
                            min_multiplicity: correlation.min_multiplicity,
                        }),
                        None => None,
                    };
                    Ok((
                        left,
                        refined::Continuation::BagOp {
                            operator,
                            arm,
                            correlation,
                        },
                    ))
                })
            }
            _ => unreachable!("a chain standing on a bag step ends in a step or a predicate"),
        }
    }

    /// Move each condition that relates two of the run's arms onto the step
    /// that owns the pair.
    ///
    /// This is the ONE place the correlation-or-filter question is asked, and
    /// it is asked per CONJUNCT. A correlation is stated one pair at a time,
    /// so `x.k = y.k and y.k = z.k` is two correlations written with one
    /// `and` — exactly what `x.k = y.k, y.k = z.k` is with a comma. Reading
    /// the conjunction whole would see three arms, name no pair, and leave a
    /// condition standing over a heading its references are not in.
    ///
    /// Conjuncts owning the same pair are conjoined again and travel
    /// together; conjuncts owning no pair stay a filter over the finished
    /// relation; a conjunct spanning three arms at once refuses.
    fn claim_bag_correlations(&mut self, mut expr: resolved::Chain) -> Result<resolved::Chain> {
        let Some(last_step) = expr
            .continuations()
            .iter()
            .rposition(|step| matches!(step.form(), resolved::Continuation::BagOp { .. }))
        else {
            unreachable!("only a chain standing on a bag step reaches here")
        };
        if last_step + 1 == expr.continuations().len() {
            return Ok(expr);
        }
        let standing = expr.split_transparent_tail(last_step + 1)?;

        let arms = bag::RunArms::of(&expr)?;
        let run = expr
            .trailing_bag_run()
            .expect("the chain ends in a bag step");
        let min_multiplicity = self
            .danger_gates
            .is_enabled("delightql-danger://semantics/min_multiplicity");

        let mut filters = Vec::with_capacity(standing.len());
        for lifted in standing {
            // A whole-heading correlation arrived as its own comma kind. It
            // names its pair by SCOPE, so it goes straight onto the step that
            // owns the pair — WHOLE, not expanded into a restriction that
            // could then be read as one.
            if let resolved::Transparent::Correlate { whole } = lifted {
                let (earlier, later) = match bag::related_whole(&whole, &arms, &self.identities) {
                    bag::Related::Pair(earlier, later) => (earlier, later),
                    bag::Related::Whole => return Err(bag::refuse_unowned_whole_heading()),
                    bag::Related::Spanning(named) => {
                        return Err(bag::refuse_spanning_conjunct(named))
                    }
                };
                self.claim_pair(
                    &mut expr,
                    run.base,
                    earlier,
                    later,
                    resolved::CorrPred::Whole(whole),
                    min_multiplicity,
                )?;
                continue;
            }
            let resolved::Transparent::Restrict {
                condition: predicate,
                origin,
            } = lifted
            else {
                unreachable!("the opacity rule admits only predicates above a bag step")
            };

            let mut parts = Vec::new();
            bag::conjuncts(predicate, &mut parts);
            let mut claimed: Vec<((usize, usize), Vec<resolved::TruthExpression>)> = Vec::new();
            let mut standing_parts = Vec::new();
            for mut part in parts {
                match bag::related(&part, &arms, &self.identities)? {
                    bag::Related::Pair(earlier, later) => {
                        // The correlation is restated over the arms' OWN
                        // headings, following the contribution record: the
                        // lowering binds it against the two arm sites, and a
                        // pad is never addressable.
                        bag::rebind_to_arms(&mut part, &arms, &self.identities)?;
                        bag::refuse_unqualified_correlation(&part)?;
                        bag::refuse_ambiguous_bare_reference(&part, &arms, &self.identities)?;
                        match claimed
                            .iter_mut()
                            .find(|(pair, _)| *pair == (earlier, later))
                        {
                            Some((_, parts)) => parts.push(part),
                            None => claimed.push(((earlier, later), vec![part])),
                        }
                    }
                    bag::Related::Whole => standing_parts.push(part),
                    bag::Related::Spanning(named) => {
                        return Err(bag::refuse_spanning_conjunct(named))
                    }
                }
            }

            // Nothing was claimed: leave the condition exactly as written, so
            // an ordinary filter over the union is not rebuilt from parts.
            if claimed.is_empty() {
                let predicate =
                    bag::conjoin(standing_parts).expect("a predicate has at least one conjunct");
                filters.push(resolved::Transparent::Restrict {
                    condition: predicate,
                    origin,
                });
                continue;
            }

            for ((earlier, later), parts) in claimed {
                let predicate = bag::conjoin(parts).expect("a claimed pair has a conjunct");
                self.claim_pair(
                    &mut expr,
                    run.base,
                    earlier,
                    later,
                    resolved::CorrPred::Expression(predicate),
                    min_multiplicity,
                )?;
            }

            // Whatever the conjunction also said about the finished relation
            // is kept, in its own right, above the run.
            if let Some(predicate) = bag::conjoin(standing_parts) {
                filters.push(resolved::Transparent::Restrict {
                    condition: predicate,
                    origin,
                });
            }
        }
        // Back on, one at a time: each filter's result is RESTATED from the
        // relation the run publishes rather than carried over from the step
        // it was lifted off.
        for filter in filters {
            expr = expr.transparently(filter);
        }
        Ok(expr)
    }

    /// Write one pair's correlation onto the step that owns it.
    ///
    /// ONE PLACE. The whole-heading form and the ordinary predicate reach
    /// the step by the same road, so "two correlations land on the same
    /// operand" is one refusal rather than one per spelling.
    fn claim_pair(
        &self,
        expr: &mut resolved::Chain,
        base: usize,
        earlier: usize,
        later: usize,
        predicate: resolved::CorrPred,
        min_multiplicity: bool,
    ) -> Result<()> {
        let owner = base + later - 1;
        // A correlation is a constraint on the PAIRING, not on the heading:
        // the arms contribute exactly what they contributed, so the step
        // publishes what it published — which is why the road here reaches
        // the correlation FIELD and has no spelling for anything else.
        let Some(correlation) = expr.bag_correlation_at(owner) else {
            unreachable!("the run's steps are bag steps")
        };
        if correlation.is_some() {
            return Err(DelightQLError::validation_error_categorized(
                "resolution/setop/correlation_owner",
                "two set-operation correlations land on the same operand, so \
                     which one that operand is filtered by is unstated",
                "correlate each operand with one earlier operand: \
                     `x(*) as a ; y(*) as b ; z(*) as c, a.k = b.k, b.k = c.k`",
            ));
        }
        let operator = expr.bag_operator_at(owner).expect("the step is a bag step");
        *expr
            .bag_correlation_at(owner)
            .expect("the step is a bag step") = Some(resolved::BagCorrelation {
            with_arm: crate::pipeline::asts::vocabulary::ArmIx::from_raw(earlier as u16),
            predicate,
            // Only a pair whose rows both reach the result can want the
            // min(m,n) shape; minus keeps its left rows once by law.
            min_multiplicity: min_multiplicity && operator.accumulates_arm_rows(),
        });
        Ok(())
    }
}

impl AstTransform<Resolved, Refined> for RefinerFold<'_> {
    fn fold_correlation_arm(
        &mut self,
        arm: crate::relation::SemanticRelation,
    ) -> Result<crate::relation::SemanticRelation> {
        Ok(arm)
    }

    crate::pipeline::ast_transform::uninhabited_payload_folds!(
        fold_column_ordinal,
        fold_column_range,
        fold_placeholder,
        fold_context_marker,
    );
    fn fold_open_leaf(
        &mut self,
        _: crate::pipeline::asts::core::FormalHole,
    ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
        Err(crate::error::DelightQLError::validation_error_categorized(
            "value/open/unapplied",
            "a composition input stands outside any callable applying it",
            "the position that applies an open body spends its slot",
        ))
    }

    fn fold_cover_callable(&mut self, callable: ()) -> crate::error::Result<()> {
        Ok(callable)
    }

    fn fold_rename_target(
        &mut self,
        target: crate::names::Spelling,
    ) -> crate::error::Result<crate::names::Spelling> {
        Ok(target)
    }
    crate::pipeline::ast_transform::decided_payload_travels_forward!(
        fold_scope(crate::relation::SemanticRelation),
        fold_output(crate::relation::PortId),
        fold_scalar_output(crate::relation::PortId),
        fold_destructure(Vec<crate::pipeline::asts::core::DestructureMapping>),
        fold_drill(crate::pipeline::asts::core::operators::BoundDrill),
        fold_entity(crate::names::CallableId),
        fold_col(crate::pipeline::asts::core::ColumnOccurrence),
        fold_binder(crate::relation::PortId),
    );

    // -------------------------------------------------------------------------
    // transform_relational_action — the routing hub
    // -------------------------------------------------------------------------
    //
    // Every arm returns FoldAction::Replaced because the refiner fully handles
    // each subtree: either via FAR (refine_segment), via explicit recursion
    // (pipes, filters), or via delegation to transform_relation (InnerRelation,
    // ConsultedView). The walk's default recursion is never used directly on
    // Chain nodes.
    //
    // THE GUARDED BOUNDARY. This is the frame the refinement budget counts,
    // and every recursive route through the refiner reaches it — the chain
    // walk, the FAR cycle, and the rebuilder's re-entry alike. The wrapper
    // holds nothing but the frame: the check runs before the body below
    // grows another stack segment or clones another chain, which is the
    // whole point of guarding here rather than inside it.
    fn transform_relational_action(
        &mut self,
        expr: resolved::Chain,
    ) -> Result<FoldAction<refined::Chain>> {
        // The handle is cloned so the frame borrows IT rather than `self`,
        // which the body below needs mutably. A refcount bump per frame is
        // the cost; the clone it stands in front of is a whole chain.
        let identities = self.identities;
        let _frame = identities.refinement().enter()?;
        // THE HUB IS A ROUTER, and the chain travels WHOLE through it. A
        // chain reaching here may come back as the same relation, as one
        // derived from it, or as a statement the FAR cycle built over the
        // operand's own sources — three things, and the road that admits all
        // three is not the road a caller depending on an old-to-new
        // correspondence takes. The operand is read out of the node that
        // arrives, so nothing here names one segment while refining another.
        let refined = identities.authority().refine_segment(expr, |node| {
            Ok(self
                .transform_relational_action_within_budget(node)?
                .into_inner())
        })?;
        Ok(FoldAction::Replaced(refined))
    }

    // -------------------------------------------------------------------------
    // transform_query — refine_query logic through the fold
    // -------------------------------------------------------------------------
    fn transform_query(&mut self, query: resolved::Query) -> Result<refined::Query> {
        self.transform_query_body(query)
    }
}

impl RefinerFold<'_> {
    /// The routing hub's body, one frame of the budget already held.
    fn transform_relational_action_within_budget(
        &mut self,
        expr: resolved::Chain,
    ) -> Result<FoldAction<refined::Chain>> {
        log::debug!(
            "RefinerFold::transform_relational_action: {} continuation(s), is_top_level={}",
            expr.continuations().len(),
            self.is_top_level
        );

        // A bare read that the refiner handles whole, rather than through the
        // FAR cycle: the classification is the relation's own.
        if !expr.has_steps() {
            if let resolved::GroundForm::Reference(rel) = expr.head().form() {
                let handled_whole =
                    matches!(
                        rel,
                        resolved::Relation::InnerRelation { .. }
                            | resolved::Relation::ConsultedView { .. }
                    ) || (matches!(rel, resolved::Relation::FunctorCall { call: _, .. })
                        && self.is_dml_call(rel));
                if handled_whole {
                    // The HEAD travels whole: its payload is refined and what
                    // it publishes rides through the same scope fold every
                    // phase-selected payload uses.
                    let head = expr.head().clone();
                    let resolved::GroundForm::Reference(rel) = head.form().clone() else {
                        unreachable!("just matched a reference head")
                    };
                    let form = refined::GroundForm::Reference(self.transform_relation(rel)?);
                    let refined_head = head.folded(self, form)?;
                    return Ok(FoldAction::Replaced(refined::Chain::ground(refined_head)));
                }
            }
        }

        // A trailing bound or destructure bypasses FAR: a bound is holistic
        // where FAR is pointwise, and a destructure publishes its own scope.
        //
        // The bypass is LOAD-BEARING, not an optimization. Without it either
        // kind reaches the flattener, which stores the whole chain — itself
        // included — as an opaque table for the rebuilder to refine, and
        // refining it arrives back here. Only a restriction may fall through.
        if matches!(
            expr.continuations().last().map(resolved::Step::form),
            Some(resolved::Continuation::Bound { .. } | resolved::Continuation::Destructure { .. })
        ) {
            let peeled = expr.peel().expect("just matched a trailing step");
            return Ok(FoldAction::Replaced(peeled.crossing(
                self,
                |walk, prefix, form, _| {
                    let form = match form {
                        resolved::Continuation::Bound { bound } => {
                            refined::Continuation::Bound { bound }
                        }
                        resolved::Continuation::Destructure {
                            source,
                            pattern,
                            mode,
                            schema,
                        } => refined::Continuation::Destructure {
                            source: Box::new(carry::domain(*source)?),
                            pattern: carry::tree_pattern(pattern)?,
                            mode,
                            schema,
                        },
                        _ => unreachable!("just matched a bound or a destructure"),
                    };
                    let refined_source = walk.transform_relational_action(prefix)?.into_inner();
                    Ok((refined_source, form))
                },
            )?));
        }

        // A trailing run: refine the relation it shapes, then carry the steps
        // across mechanically. The walk's default transform_operator rephases
        // every expression child inside them.
        //
        // A DIMENSION ACCESS BELONGS TO THIS RUN, and the bypass is
        // LOAD-BEARING for it exactly as it is for a bound: without it the
        // access reaches the flattener, which stores the whole chain — itself
        // included — as an opaque table for the rebuilder to refine, and
        // refining it arrives back here.
        //
        // THE PARTITION IS THE MEMBERSHIP: each pop either returns the
        // run-step family or restores the step and ends the run — a chain
        // with no trailing run collects nothing and falls through UNCHANGED
        // to the roads below, so no boolean gate stands beside the
        // partition to disagree with it.
        let expr = match expr.peel_run() {
            Err(expr) => expr,
            Ok(run) => {
                return Ok(FoldAction::Replaced(run.crossing(
                    self,
                    |walk, prefix| Ok(walk.transform_relational_action(prefix)?.into_inner()),
                    |walk, form| {
                        Ok(match form {
                            resolved::Continuation::Pipe {
                                operator,
                                named: (),
                            } => refined::Continuation::Pipe {
                                operator: walk.transform_operator(operator)?,
                                named: (),
                            },
                            resolved::Continuation::Access { access, named: () } => {
                                refined::Continuation::Access {
                                    access: carry::access(access)?,
                                    named: (),
                                }
                            }
                            // The structural steps carry across mechanically —
                            // the walk rephases the expressions they hold, and
                            // their payloads have no relational child to refine.
                            resolved::Continuation::Structural(step) => {
                                refined::Continuation::Structural(
                                    crate::pipeline::ast_transform::walk_transform_structural_step(
                                        walk, step,
                                    )?,
                                )
                            }
                            _ => unreachable!("the run partition returns only run steps"),
                        })
                    },
                )?));
            }
        };

        // A chain standing on a bag step is the bag road's. Its arms are
        // relations in their own right and the predicate above them may be
        // the correlation naming two of them, so the FAR cycle — which
        // pools tables and re-classifies predicates against that pool —
        // never sees one.
        if expr.stands_on_bag_step() {
            return Ok(FoldAction::Replaced(self.refine_bag_chain(expr)?));
        }

        // Everything else — members, predicate restrictions and callable
        // heads — goes through the FAR cycle.
        //
        // FAR treats scalar and predicate subqueries as opaque expressions
        // while it flattens the surrounding segment. Classify every
        // inner-relation pattern before that boundary so a relation nested
        // below IN, EXISTS, or a scalar aggregate cannot bypass the inline
        // InnerRelation arm above.
        // THE NODE BEING REBUILT, taken before anything reshapes it. The
        // FAR cycle stands over this operand's own sources rather than over
        // the operand, so this is the provenance that travels with the
        // flattened segment and says what the rebuild replaces.
        let operand = expr.semantic_relation();
        let classified = pattern_classifier::classify_patterns_via_fold(expr, &self.identities)?;
        let refined = refine_segment(
            classified,
            operand,
            self.is_top_level,
            &self.danger_gates,
            &self.identities,
        )?;
        Ok(FoldAction::Replaced(refined))
    }

    // -------------------------------------------------------------------------
    // transform_relation — inline classification of InnerRelation patterns
    // -------------------------------------------------------------------------
    //
    // This replaces the classify_patterns pre-pass. When encountering an
    // Indeterminate InnerRelation, we classify it on the resolved subquery
    // then convert to refined phase mechanically. The FAR cycle (via the
    // rebuilder) handles real subquery refinement later.
    //
    // For already-classified patterns, walk_transform_inner_relation handles
    // recursion into subqueries and correlation filters.
    fn transform_relation(&mut self, rel: resolved::Relation) -> Result<refined::Relation> {
        match rel {
            resolved::Relation::InnerRelation {
                pattern,
                alias,
                outer,
            } => {
                let refined_pattern = match pattern {
                    resolved::InnerRelationPattern::Indeterminate {
                        identifier,
                        subquery,
                    } => {
                        // Recursively classify nested InnerRelation patterns
                        // in the subquery. This uses the fold's walk to descend
                        // into operators and ConsultedView bodies — fixing the
                        // classify_operator() no-op bug by construction.
                        let classified_subquery = pattern_classifier::classify_patterns_via_fold(
                            *subquery,
                            &self.identities,
                        )?;

                        // Classify the outer pattern on the classified subquery.
                        let classified = pattern_classifier::classify_inner_relation_pattern(
                            identifier,
                            classified_subquery,
                            &self.identities,
                        )?;

                        // The subquery is refined later by the rebuilder
                        // (FAR cycle), when this InnerRelation is reached in
                        // a segment; nothing is decided about it here.
                        carry::inner_relation(classified)?
                    }
                    already_classified => {
                        // The subquery inside was classified by the pre-pass
                        // or by an earlier fold invocation, so this one has
                        // nothing left to decide.
                        carry::inner_relation(already_classified)?
                    }
                };

                Ok(refined::Relation::InnerRelation {
                    pattern: refined_pattern,
                    alias,
                    outer,
                })
            }

            resolved::Relation::ConsultedView { body, outer } => {
                let refined_body = self.transform_query(*body)?;
                Ok(refined::Relation::ConsultedView {
                    body: Box::new(refined_body),
                    outer,
                })
            }

            // Everything else: walk handles it
            other => walk_transform_relation(self, other),
        }
    }

    // -------------------------------------------------------------------------
    // transform_query — refine_query logic through the fold
    // -------------------------------------------------------------------------
    fn transform_query_body(&mut self, query: resolved::Query) -> Result<refined::Query> {
        let resolved::Query { locals, body } = query;
        let ctes = locals.into_ctes();
        let refined_ctes = ctes
            .into_iter()
            .map(|cte| {
                let mut fold = RefinerFold {
                    is_top_level: true,
                    danger_gates: self.danger_gates.clone(),
                    identities: self.identities,
                };
                // The binding crosses WHOLE: its carrier refines every
                // chain it holds and keeps subject and variant.
                cte.folded(&mut fold)
            })
            .collect::<Result<Vec<_>>>()?;

        let mut fold = RefinerFold {
            is_top_level: true,
            danger_gates: self.danger_gates.clone(),
            identities: self.identities,
        };
        let refined_body = fold.transform_relational_action(body)?.into_inner();

        Ok(refined::Query::binding(
            crate::pipeline::asts::core::QueryLocals::spent(refined_ctes),
            refined_body,
        ))
    }
}

// =============================================================================
// Public entry points (unchanged API)
// =============================================================================

/// Main entry point for AST refinement (for Chain)
pub fn refine(
    ast: resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    refine_with_gates(
        ast,
        crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
        identities,
    )
}

/// Refine with danger gate context.
pub fn refine_with_gates(
    ast: resolved::Chain,
    danger_gates: crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    refine_internal(ast, true, danger_gates, identities)
}

/// Internal refine with context tracking
#[stacksafe::stacksafe]
pub(crate) fn refine_internal(
    ast: resolved::Chain,
    is_top_level: bool,
    danger_gates: crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    // Limit placement: insert UDT subquery boundaries where
    // a limit is followed by a row-collapsing operator (aggregation, group,
    // distinct), and fold consecutive limits into a single min-limit.
    let ast = limit_placement::apply(ast, &identities)?;

    let mut fold = RefinerFold {
        is_top_level,
        danger_gates,
        identities,
    };
    fold.transform_relational_action(ast)
        .map(|a| a.into_inner())
}

/// Refine a single segment (no pipes) using the FAR cycle
fn refine_segment(
    ast: resolved::Chain,
    operand: crate::relation::SemanticRelation,
    is_top_level: bool,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    log::debug!(
        "refine_segment: {} continuation(s), is_top_level={}",
        ast.continuations().len(),
        is_top_level
    );

    // Phase 1: Flatten the AST into a flat segment
    let flat_segment = flattener::flatten(ast, operand, identities)?;
    log::debug!(
        "refine_segment: After flatten - {} tables, {} operators",
        flat_segment.tables.len(),
        flat_segment.operators.len()
    );

    // Phase 2: Analyze the segment and classify predicates
    let analyzed_segment = analyzer::analyze(flat_segment, identities)?;
    log::debug!(
        "refine_segment: After analyze - {} predicates",
        analyzed_segment.predicates.len()
    );

    // Phase 3: Rebuild the AST with predicates in proper locations
    log::debug!("refine_segment: Calling rebuilder::rebuild");
    let refined_ast =
        rebuilder::rebuild_internal(analyzed_segment, is_top_level, danger_gates, identities)?;

    Ok(refined_ast)
}

/// Refine a full Query (with CTEs)
pub fn refine_query(
    query: resolved::Query,
    identities: &crate::relation::Planning,
) -> Result<refined::Query> {
    refine_query_with_gates(
        query,
        crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
        identities,
    )
}

/// Refine a full Query with danger gate context.
pub fn refine_query_with_gates(
    query: resolved::Query,
    danger_gates: crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
) -> Result<refined::Query> {
    // Limit placement: pre-process the query AST to insert
    // UDT subquery boundaries where a limit is followed by a row-collapsing
    // operator. Runs once at the top level; the pass recurses into all
    // relational subqueries it encounters.
    let query = apply_limit_placement_to_query(query, &identities)?;

    let mut fold = RefinerFold {
        is_top_level: true,
        danger_gates,
        identities,
    };
    fold.transform_query(query)
}

fn apply_limit_placement_to_query(
    query: resolved::Query,
    identities: &crate::relation::Planning,
) -> Result<resolved::Query> {
    let resolved::Query { locals, body } = query;
    let ctes = locals.into_ctes();
    let new_ctes = ctes
        .into_iter()
        .map(|cte| {
            // A same-phase rewrite of every part, KEEPING subject and
            // variant: where a bound `#<N` lands is not a question about
            // what this binding is or what it stands on.
            cte.map_chains(|chain| limit_placement::apply(chain, identities))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(resolved::Query::binding(
        crate::pipeline::asts::core::QueryLocals::spent(new_ctes),
        limit_placement::apply(body, identities)?,
    ))
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    use crate::pipeline::asts::resolved::{Access, TupleOrdinalClause, TupleOrdinalOperator};

    /// A chain carrying `steps` stacked offset bounds.
    ///
    /// Synthetic depth, bounded and exact: a trailing bound takes the
    /// refiner's bypass, which pops ONE step and re-enters the guarded hub,
    /// so `steps` bounds cost `steps + 1` frames and nothing else varies.
    /// Offsets rather than `#<n` because `limit_placement` folds consecutive
    /// row limits into one and would flatten the ladder.
    fn ladder(steps: usize, registry: &crate::relation::Planning) -> resolved::Chain {
        let scope = crate::relation::any_relation(registry);
        let mut chain = registry
            .authority()
            .ground_read(Access::All, false, scope)
            .expect("a ground read");
        for step in 0..steps {
            chain = chain.transparently(resolved::Transparent::Bound {
                bound: TupleOrdinalClause {
                    operator: TupleOrdinalOperator::GreaterThan,
                    value: step as i64 + 1,
                    offset: None,
                },
            });
        }
        chain
    }

    fn refine_ladder(
        steps: usize,
        budget: usize,
    ) -> (crate::relation::Planning, Result<refined::Chain>) {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        registry.refinement().arm(budget);
        let chain = ladder(steps, &registry);
        let refined = refine(chain, &registry);
        (registry, refined)
    }

    /// The budget is spent by the frame it is measured in, and spending it
    /// is what refuses. Eight steps cost exactly nine frames: nine is
    /// affordable and eight is not, so the guard is neither off by one nor
    /// counting something else.
    #[test]
    fn a_walk_past_the_budget_is_refused_and_one_within_it_is_not() {
        let (_, affordable) = refine_ladder(8, 9);
        assert!(affordable.is_ok(), "nine frames pay for eight steps");

        let (registry, refused) = refine_ladder(8, 8);
        let error = refused.expect_err("eight frames do not");
        assert!(
            error
                .error_uri()
                .contains("operational/resource/refinement-depth"),
            "the refusal carries its own identity, got {}",
            error.error_uri()
        );
        assert_eq!(
            registry.refinement().active(),
            0,
            "every frame the refused walk took was given back"
        );
    }

    /// Raising the allowance within the ceiling admits the SAME walk. A
    /// guard that refused for another reason would refuse both.
    #[test]
    fn raising_the_allowance_admits_the_same_walk() {
        assert!(refine_ladder(20, 12).1.is_err());
        assert!(refine_ladder(20, 64).1.is_ok());
    }

    /// A refinement entered from inside a walk that already holds frames
    /// gets what is LEFT, not a fresh allowance.
    ///
    /// This is the property the rebuilder's re-entry depends on: the budget
    /// belongs to the compilation, and the registry a nested refinement is
    /// handed IS the compilation. The same ladder that fits exactly from the
    /// top must refuse one frame in — if it did not, every nested lap would
    /// renew the allowance and the guard would never stop a cycle that
    /// re-enters.
    #[test]
    fn a_nested_refinement_inherits_the_frames_already_held() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        registry.refinement().arm(9);

        let from_the_top = refine(ladder(8, &registry), &registry);
        assert!(from_the_top.is_ok(), "nine frames pay for eight steps");

        let held = registry.refinement().enter().expect("one frame is free");
        let one_frame_in = refine(ladder(8, &registry), &registry);
        assert!(
            one_frame_in.is_err(),
            "the nested walk must see the frame its caller is standing on"
        );
        drop(held);

        assert_eq!(registry.refinement().active(), 0);
        assert!(
            refine(ladder(8, &registry), &registry).is_ok(),
            "and the compilation is spendable again once the frame is returned"
        );
    }

    /// Two compilations are independent. A refusal is one compilation's
    /// answer, not a mark on the process.
    #[test]
    fn a_refusal_does_not_reach_the_next_compilation() {
        assert!(refine_ladder(8, 4).1.is_err());
        let (_, next) = refine_ladder(8, crate::compiler_limits::REFINEMENT_DEPTH.default_value());
        assert!(next.is_ok(), "the next compilation starts whole");
    }
}
