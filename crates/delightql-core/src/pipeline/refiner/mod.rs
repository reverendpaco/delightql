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
mod correlation_analyzer;
mod flattener;
mod laws;
mod limit_placement;
mod pattern_classifier;
#[cfg(test)]
mod phase_boundary_tests;
mod rebuilder;
mod types;

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::{walk_transform_relation, AstTransform, FoldAction};
use crate::pipeline::asts::refined::Refined;
use crate::pipeline::asts::resolved::Resolved;
use crate::pipeline::asts::{refined, resolved};
use std::rc::Rc;

// =============================================================================
// RefinerFold — AstTransform<Resolved, Refined>
// =============================================================================

struct RefinerFold {
    is_top_level: bool,
    danger_gates: crate::pipeline::danger_gates::DangerGateMap,
    identities: Rc<crate::names::Registry>,
}

impl RefinerFold {
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

impl RefinerFold {
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
        let mut expr = self.claim_bag_correlations(expr)?;
        match expr.continuations.pop() {
            Some(resolved::Continuation::Restrict {
                condition,
                origin,
                cpr_schema,
            }) => {
                let source = self.transform_relational_action(expr)?.into_inner();
                let condition = rebuilder::refine_predicate_boolean(condition, &self.identities)?;
                Ok(source.then(refined::Continuation::Restrict {
                    condition,
                    origin,
                    cpr_schema: self.fold_scope(cpr_schema)?,
                }))
            }
            Some(resolved::Continuation::BagOp {
                operator,
                arm,
                correlation,
                cpr_schema,
            }) => {
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
                                bag::whole_tuple_correlation(
                                    bag::published_scope(&expr),
                                    bag::published_scope(&arm),
                                    &self.identities,
                                )?,
                            ),
                            min_multiplicity: false,
                        })
                    }
                    (correlation, _) => correlation,
                };
                let left = self.transform_relational_action(expr)?.into_inner();
                let arm = self.transform_relational_action(arm)?.into_inner();
                let correlation = match correlation {
                    Some(correlation) => Some(refined::BagCorrelation {
                        with_arm: correlation.with_arm,
                        // The whole-heading form travels WHOLE. It is not
                        // expanded here and it is not a predicate to refine:
                        // the mode it aligns by is what the lowering reads.
                        predicate: match correlation.predicate {
                            resolved::CorrPred::Expression(predicate) => {
                                refined::CorrPred::Expression(rebuilder::refine_predicate_boolean(
                                    predicate,
                                    &self.identities,
                                )?)
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
                let cpr_schema = self.fold_scope(cpr_schema)?;
                Ok(left.bag_op(operator, arm, correlation, cpr_schema))
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
            .continuations
            .iter()
            .rposition(|c| matches!(c, resolved::Continuation::BagOp { .. }))
        else {
            unreachable!("only a chain standing on a bag step reaches here")
        };
        if last_step + 1 == expr.continuations.len() {
            return Ok(expr);
        }
        let standing = expr.continuations.split_off(last_step + 1);

        let arms = bag::RunArms::of(&expr)?;
        let run = expr
            .trailing_bag_run()
            .expect("the chain ends in a bag step");
        let min_multiplicity = self
            .danger_gates
            .is_enabled("delightql-danger://semantics/min_multiplicity");

        let mut filters = Vec::with_capacity(standing.len());
        for continuation in standing {
            // A whole-heading correlation arrived as its own comma kind. It
            // names its pair by SCOPE, so it goes straight onto the step that
            // owns the pair — WHOLE, not expanded into a restriction that
            // could then be read as one.
            if let resolved::Continuation::Correlate { whole, .. } = continuation {
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
            let resolved::Continuation::Restrict {
                condition: predicate,
                origin,
                cpr_schema,
            } = continuation
            else {
                unreachable!("the opacity rule admits only predicates above a bag step")
            };

            let mut parts = Vec::new();
            bag::conjuncts(predicate, &mut parts);
            let mut claimed: Vec<((usize, usize), Vec<resolved::TruthExpression>)> = Vec::new();
            let mut standing_parts = Vec::new();
            for part in parts {
                match bag::related(&part, &arms, &self.identities) {
                    bag::Related::Pair(earlier, later) => {
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
                filters.push(resolved::Continuation::Restrict {
                    condition: predicate,
                    origin,
                    cpr_schema,
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
                filters.push(resolved::Continuation::Restrict {
                    condition: predicate,
                    origin,
                    cpr_schema,
                });
            }
        }
        expr.continuations.extend(filters);
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
        let resolved::Continuation::BagOp {
            operator,
            correlation,
            ..
        } = &mut expr.continuations[base + later - 1]
        else {
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
        *correlation = Some(resolved::BagCorrelation {
            with_arm: crate::pipeline::asts::vocabulary::ArmIx::from_raw(earlier as u16),
            predicate,
            // Only a pair whose rows both reach the result can want the
            // min(m,n) shape; minus keeps its left rows once by law.
            min_multiplicity: min_multiplicity && operator.accumulates_arm_rows(),
        });
        Ok(())
    }
}

impl AstTransform<Resolved, Refined> for RefinerFold {
    fn fold_correlation_arm(
        &mut self,
        arm: crate::names::ScopeId,
    ) -> Result<crate::names::ScopeId> {
        Ok(arm)
    }

    fn fold_ho_landing(&mut self, landing: ()) -> Result<()> {
        Ok(landing)
    }

    crate::pipeline::ast_transform::uninhabited_payload_folds!(
        fold_column_ordinal,
        fold_column_range,
        fold_placeholder,
        fold_context_marker,
    );
    fn fold_open_leaf(
        &mut self,
        leaf: crate::pipeline::asts::vocabulary::Never,
    ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
        match leaf {}
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
        fold_scope(crate::names::ScopeId),
        fold_consulted(crate::names::ScopeId),
        fold_recursion(crate::pipeline::asts::vocabulary::RecursionState),
        fold_cte_subject(crate::names::ScopeId),
        fold_cte_authority(()),
        fold_output(Option<crate::names::ColId>),
        fold_scalar_output(crate::names::ColId),
        fold_destructure(Vec<crate::pipeline::asts::core::DestructureMapping>),
        fold_drill(crate::pipeline::asts::core::operators::BoundDrill),
        fold_entity(crate::names::CallableId),
        fold_col(crate::pipeline::asts::core::ColumnOccurrence),
        fold_binder(crate::names::ColId),
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
        let identities = Rc::clone(&self.identities);
        let _frame = identities.refinement().enter()?;
        self.transform_relational_action_within_budget(expr)
    }

    // -------------------------------------------------------------------------
    // transform_query — refine_query logic through the fold
    // -------------------------------------------------------------------------
    fn transform_query(&mut self, query: resolved::Query) -> Result<refined::Query> {
        self.transform_query_body(query)
    }
}

impl RefinerFold {
    /// The routing hub's body, one frame of the budget already held.
    fn transform_relational_action_within_budget(
        &mut self,
        expr: resolved::Chain,
    ) -> Result<FoldAction<refined::Chain>> {
        log::debug!(
            "RefinerFold::transform_relational_action: {} continuation(s), is_top_level={}",
            expr.continuations.len(),
            self.is_top_level
        );

        // A bare read that the refiner handles whole, rather than through the
        // FAR cycle: the classification is the relation's own.
        if !expr.has_steps() {
            if let resolved::Grelex::Reference(rel) = &expr.head {
                let handled_whole =
                    matches!(
                        rel,
                        resolved::Relation::InnerRelation { .. }
                            | resolved::Relation::ConsultedView { .. }
                    ) || (matches!(rel, resolved::Relation::FunctorCall { call: _, .. })
                        && self.is_dml_call(rel));
                if handled_whole {
                    let resolved::Grelex::Reference(rel) = expr.head else {
                        unreachable!("just matched a reference head")
                    };
                    let refined = self.transform_relation(rel)?;
                    return Ok(FoldAction::Replaced(refined::Chain::relation(refined)));
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
            expr.continuations.last(),
            Some(resolved::Continuation::Bound { .. } | resolved::Continuation::Destructure { .. })
        ) {
            let mut expr = expr;
            let step = expr.continuations.pop().expect("just matched");
            let refined_source = self.transform_relational_action(expr)?.into_inner();
            let step = match step {
                resolved::Continuation::Bound { bound, cpr_schema } => {
                    refined::Continuation::Bound {
                        bound,
                        cpr_schema: self.fold_scope(cpr_schema)?,
                    }
                }
                resolved::Continuation::Destructure {
                    source,
                    pattern,
                    mode,
                    schema,
                    cpr_schema,
                } => refined::Continuation::Destructure {
                    source: Box::new(carry::domain(*source)?),
                    pattern: carry::tree_pattern(pattern)?,
                    mode,
                    schema,
                    cpr_schema: self.fold_scope(cpr_schema)?,
                },
                _ => unreachable!("just matched a bound or a destructure"),
            };
            return Ok(FoldAction::Replaced(refined_source.then(step)));
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
        let mut expr = expr;
        {
            use crate::pipeline::asts::core::expressions::chain::RunStep;
            let mut segments: Vec<RunStep<crate::pipeline::asts::core::Resolved>> = Vec::new();
            while let Some(step) = expr.pop_run_step() {
                segments.push(step);
            }
            if !segments.is_empty() {
                segments.reverse();
                let mut refined = self.transform_relational_action(expr)?.into_inner();
                for segment in segments {
                    refined = match segment {
                        RunStep::Pipe {
                            operator,
                            named: (),
                            cpr_schema,
                        } => refined.then(refined::Continuation::Pipe {
                            operator: self.transform_operator(operator)?,
                            named: (),
                            cpr_schema: self.fold_scope(cpr_schema)?,
                        }),
                        RunStep::Access { access, cpr_schema } => {
                            refined.then(refined::Continuation::Access {
                                access: carry::access(access)?,
                                cpr_schema: self.fold_scope(cpr_schema)?,
                            })
                        }
                        RunStep::Structural(step) => {
                            // The structural steps carry across mechanically —
                            // the walk rephases the expressions they hold, and
                            // their payloads have no relational child to refine.
                            refined.then(refined::Continuation::Structural(
                                crate::pipeline::ast_transform::walk_transform_structural_step(
                                    self, step,
                                )?,
                            ))
                        }
                    };
                }
                return Ok(FoldAction::Replaced(refined));
            }
        }

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
        let classified = pattern_classifier::classify_patterns_via_fold(expr, &self.identities)?;
        let refined = refine_segment(
            classified,
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
                preminted_scope,
                alias,
                outer,
                cpr_schema,
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
                    preminted_scope,
                    alias,
                    outer,
                    cpr_schema: self.fold_scope(cpr_schema)?,
                })
            }

            resolved::Relation::ConsultedView {
                body,
                scoped,
                outer,
            } => {
                let refined_body = self.transform_query(*body)?;
                Ok(refined::Relation::ConsultedView {
                    body: Box::new(refined_body),
                    scoped: self.fold_consulted(scoped)?,
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
        let resolved::Query { cfes: (), ctes, body } = query;
        let refined_ctes = ctes
            .into_iter()
            .map(|cte| {
                let mut fold = RefinerFold {
                    is_top_level: true,
                    danger_gates: self.danger_gates.clone(),
                    identities: Rc::clone(&self.identities),
                };
                Ok(refined::CteBinding {
                    expression: fold
                        .transform_relational_action(cte.expression)?
                        .into_inner(),
                    subject: cte.subject,
                    authority: cte.authority,
                    recursion: self.fold_recursion(cte.recursion)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut fold = RefinerFold {
            is_top_level: true,
            danger_gates: self.danger_gates.clone(),
            identities: Rc::clone(&self.identities),
        };
        let refined_body = fold.transform_relational_action(body)?.into_inner();

        Ok(refined::Query {
            cfes: (),
            ctes: refined_ctes,
            body: refined_body,
        })
    }
}

// =============================================================================
// Public entry points (unchanged API)
// =============================================================================

/// Main entry point for AST refinement (for Chain)
pub fn refine(
    ast: resolved::Chain,
    identities: Rc<crate::names::Registry>,
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
    identities: Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    refine_internal(ast, true, danger_gates, identities)
}

/// Internal refine with context tracking
#[stacksafe::stacksafe]
pub(crate) fn refine_internal(
    ast: resolved::Chain,
    is_top_level: bool,
    danger_gates: crate::pipeline::danger_gates::DangerGateMap,
    identities: Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    // Limit placement: insert UDT subquery boundaries where
    // a limit is followed by a row-collapsing operator (aggregation, group,
    // distinct), and fold consecutive limits into a single min-limit.
    let ast = limit_placement::apply(ast)?;

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
    is_top_level: bool,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    log::debug!(
        "refine_segment: {} continuation(s), is_top_level={}",
        ast.continuations.len(),
        is_top_level
    );

    // Phase 1: Flatten the AST into a flat segment
    let flat_segment = flattener::flatten(ast, Rc::clone(identities))?;
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
    identities: Rc<crate::names::Registry>,
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
    identities: Rc<crate::names::Registry>,
) -> Result<refined::Query> {
    // Limit placement: pre-process the query AST to insert
    // UDT subquery boundaries where a limit is followed by a row-collapsing
    // operator. Runs once at the top level; the pass recurses into all
    // relational subqueries it encounters.
    let query = apply_limit_placement_to_query(query)?;

    let mut fold = RefinerFold {
        is_top_level: true,
        danger_gates,
        identities,
    };
    fold.transform_query(query)
}

fn apply_limit_placement_to_query(query: resolved::Query) -> Result<resolved::Query> {
    let resolved::Query { cfes: (), ctes, body } = query;
    let new_ctes = ctes
        .into_iter()
        .map(|cte| {
            Ok(resolved::CteBinding {
                expression: limit_placement::apply(cte.expression)?,
                subject: cte.subject,
                authority: cte.authority,
                recursion: cte.recursion,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(resolved::Query {
        cfes: (),
        ctes: new_ctes,
        body: limit_placement::apply(body)?,
    })
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    use crate::names::Registry;
    use crate::pipeline::asts::resolved::{
        Access, Relation, TupleOrdinalClause, TupleOrdinalOperator,
    };

    /// A chain carrying `steps` stacked offset bounds.
    ///
    /// Synthetic depth, bounded and exact: a trailing bound takes the
    /// refiner's bypass, which pops ONE step and re-enters the guarded hub,
    /// so `steps` bounds cost `steps + 1` frames and nothing else varies.
    /// Offsets rather than `#<n` because `limit_placement` folds consecutive
    /// row limits into one and would flatten the ladder.
    fn ladder(steps: usize, registry: &Registry) -> resolved::Chain {
        let scope = registry.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::User(registry.intern("t", false)),
            None,
        );
        let mut chain = Relation::ground_read(Access::All, false, scope);
        for step in 0..steps {
            chain = chain.then(resolved::Continuation::Bound {
                bound: TupleOrdinalClause {
                    operator: TupleOrdinalOperator::GreaterThan,
                    value: step as i64 + 1,
                    offset: None,
                },
                cpr_schema: scope,
            });
        }
        chain
    }

    fn refine_ladder(steps: usize, budget: usize) -> (Rc<Registry>, Result<refined::Chain>) {
        let registry = Rc::new(Registry::new(&[]));
        registry.refinement().arm(budget);
        let chain = ladder(steps, &registry);
        let refined = refine(chain, Rc::clone(&registry));
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
        let registry = Rc::new(Registry::new(&[]));
        registry.refinement().arm(9);

        let from_the_top = refine(ladder(8, &registry), Rc::clone(&registry));
        assert!(from_the_top.is_ok(), "nine frames pay for eight steps");

        let held = registry.refinement().enter().expect("one frame is free");
        let one_frame_in = refine(ladder(8, &registry), Rc::clone(&registry));
        assert!(
            one_frame_in.is_err(),
            "the nested walk must see the frame its caller is standing on"
        );
        drop(held);

        assert_eq!(registry.refinement().active(), 0);
        assert!(
            refine(ladder(8, &registry), Rc::clone(&registry)).is_ok(),
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
