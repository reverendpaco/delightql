// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! ResolverFold — the resolver as an AstTransform<Unresolved, Resolved>.
//!
//! All recursive calls within resolve_relational_impl go through
//! `self.resolve_relational()` / `self.resolve_child()` — never a
//! temporary fold via the free function wrapper.
//!
//! Scope stack (`push_scope`/`pop_scope`) manages outer_context and grounding
//! at recursion boundaries. Expression-level hooks (transform_sigma,
//! transform_operator) read from `self.available` / `self.in_correlation`.
//!
//! The free function in mod.rs remains for callers outside this file
//! (relation_resolver, predicates, subqueries, etc.).
use super::ResolvedRelation;
use crate::pipeline::asts::core::literals::column_ordinal_text;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use delightql_types::SqlIdentifier;

use super::unification::ColumnReference;
use super::{DmlPipeKind, ResolutionConfig};
use crate::names::DmlVerb;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::operators::JoinType;
use crate::pipeline::asts::core::phases::{Resolved, Unresolved};
use crate::pipeline::asts::core::Chain;
use crate::pipeline::asts::core::ProbeAddressing;
use crate::pipeline::asts::core::ValueRow;
use crate::pipeline::asts::core::{
    Existence, Membership, MembershipSource, RelationalMembership, SigmaApplication,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::{ast_resolved, ast_unresolved};
use delightql_types::error::{DelightQLError, Result};

/// The resolver as an AstTransform<Unresolved, Resolved>.
///
/// Holds the ResolverCore, config, and the lexical position. The transform
/// trait's relational door answers with a body alone, so what answers over
/// a relation leaves only through `resolve_relational`, never through the
/// generic walk.
pub(crate) struct ResolverFold<'reg, 'db> {
    /// The durable resolver core: catalog, built-ins, planning. No lexical
    /// map lives here.
    pub core: &'reg mut crate::resolution::ResolverCore<'db>,
    /// THE ONE LEXICAL WORLD this fold resolves in. A use world at the
    /// prompt; a body world — constructed only by the definition-use
    /// authority — inside an opened definition.
    pub env: &'reg mut crate::defuse::environment::Environment,
    pub config: ResolutionConfig,
    /// THE LEXICAL POSITION: the relations under the reader's finger and
    /// the enclosing fold's position behind them. Every authored lookup is
    /// its judgment; no vector of ports or relations stands beside it.
    pub(crate) lexical: super::Position<'reg>,
    /// What the correlated condition being resolved did with its names.
    /// Accumulated across the whole condition, because whether one reference
    /// reaching outward is a correlation or a mistake depends on what the
    /// OTHER references did.
    pub(super) correlation_witness: super::Witness,
    /// THE CELL THE COVER IS APPLYING. Set only while a cover (or a
    /// deferred callable-argument body) resolves its body for one cell:
    /// an open leaf met during that resolution becomes this expression,
    /// which is how the applying position spends the leaf BEFORE any
    /// closed resolved tree is minted. `None` everywhere else, where a
    /// leaf refuses instead.
    pub(crate) cover_cell: Option<crate::pipeline::asts::resolved::DomainExpression>,
    /// Whether we're in a correlation context (for deferred validation).
    pub(super) in_correlation: bool,
    /// Pivot IN values for operator resolution.
    pivot_in_values: super::PivotInWitnesses,
    /// The self-name of the access whose INTERIOR this fold resolves —
    /// the alias if authored, the access name otherwise. Inside the
    /// parens the access is one relation under that name whatever stage
    /// its interior has reached, so spine stages keep it answering.
    /// Output columns from the last operator resolution (sidecar like last_bubbled).
    last_operator_output: Option<Vec<crate::relation::PortId>>,
    /// The exact semantic result when the operator family has crossed the
    /// authority boundary.
    /// The run's exact input while the generic AST-transform hook resolves
    /// one operator.
    operator_input: Option<crate::relation::SemanticRelation>,
    /// THE CALLER ROW AT A CALLABLE IN JOIN POSITION, and what became of
    /// it. Higher-order construction may absorb it; an ordinary callable
    /// leaves it standing for the enclosing join to recover. The carrier
    /// says which, so no flag beside it can say otherwise.
    pub(super) ho_caller_row: crate::pipeline::resolver::CallerRow,
    /// Hygienic semantic positions a definition body must publish because
    /// they carry a closed value across this use.
    pub(crate) crossing_carriers: Vec<crate::relation::PortId>,
    /// THE WINDOW OBLIGATION OF A WINDOWED WRAPPER USE. Armed by the
    /// instantiation road while a windowed use of a consulted value
    /// definition opens its body; the FIRST reducing absorber built during
    /// that resolution — an engine aggregate or an unknown target callable
    /// — takes the resolved spec, which is how the position's grade flows
    /// INWARD through the wrapper (a wrapper over an unknown target call
    /// is grade-polymorphic per use). A body that offers no absorber, or
    /// more than one, refuses when the instantiation closes.
    pub(crate) window_obligation: Option<WindowObligation>,
    /// THE GRADE OF THE VALUE POSITION BEING RESOLVED. Reduction-slot
    /// members resolve under `Reducing`; every call's ARGUMENTS resolve
    /// under `RowWise` (an absorber's interior is per-row); a consulted
    /// definition's BODY inherits the position's grade, which is how the
    /// expectation reaches every nested callable through a wrapper.
    pub(crate) position_grade: crate::defuse::bound_use::CallableGrade,
}

/// One windowed-use obligation: the caller-scope-resolved spec, whether an
/// absorber has taken it, and whether a SECOND absorber appeared (the
/// window rides one function; two is a refusal, never a silent choice).
pub(crate) struct WindowObligation {
    pub(crate) spec: ast_resolved::WindowSpec,
    pub(crate) taken: bool,
    pub(crate) extra: bool,
}

/// Whether a resolved arm IS a truth witness: its chain ends in the
/// witness structural step, directly or inside the derived table its
/// access became.
pub(super) fn chain_is_truth_witness(chain: &ast_resolved::Chain) -> bool {
    if let Some(ast_resolved::Continuation::Structural(step)) =
        chain.continuations().last().map(ast_resolved::Step::form)
    {
        if matches!(
            step.form,
            ast_resolved::StructuralForm::Witness { .. }
                | ast_resolved::StructuralForm::SignedWitness
        ) {
            return true;
        }
    }
    if chain.continuations().is_empty() {
        if let ast_resolved::GroundForm::Reference(ast_resolved::Relation::InnerRelation {
            pattern:
                ast_resolved::InnerRelationPattern::Indeterminate { subquery, .. }
                | ast_resolved::InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. },
            ..
        }) = chain.head().form()
        {
            return chain_is_truth_witness(subquery);
        }
    }
    false
}

fn validate_witness_membership(
    probe: &ast_unresolved::Probe,
    rows: &crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::ValueRow>,
) -> Result<()> {
    for header in probe.values() {
        let name = match header {
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            ) => continue,
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn { name, .. },
            ))) => name,
            _ => {
                return Err(DelightQLError::validation_error_categorized(
                    "resolution/anon/witness_shape",
                    "every witness header must be a ground value or a unifying lvar",
                    "drop the witness marker for relational function headers",
                ))
            }
        };
        let repeated = rows.iter().any(|row| {
            row.values().any(|candidate| {
                matches!(candidate,
                    ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn { name: candidate_name, .. })))
                        if SqlIdentifier::str_eq(candidate_name.as_str(), name)
                )
            })
        });
        if repeated {
            return Err(DelightQLError::validation_error_categorized(
                "resolution/anon/header_row_lvar",
                format!(
                    "lvar '{}' appears both as a header and in the data rows of the same anonymous table",
                    name
                ),
                "the header is the probe and a row lvar is a candidate — probing a column against itself is vacuously true; drop the self-candidate or rename the header",
            ));
        }
    }
    Ok(())
}

impl<'reg, 'db> ResolverFold<'reg, 'db> {
    /// A fold at a ROOT position: the prompt, or a closed world such as a
    /// definition body or a relation actual, with no row behind it.
    pub fn new(
        core: &'reg mut crate::resolution::ResolverCore<'db>,
        env: &'reg mut crate::defuse::environment::Environment,
        config: ResolutionConfig,
    ) -> Self {
        Self::at(core, env, config, super::Position::root())
    }

    /// A fold ENCLOSED by another's position: an interior expression sees
    /// the row it is correlated to through that borrow and nothing else.
    pub(crate) fn enclosed(
        core: &'reg mut crate::resolution::ResolverCore<'db>,
        env: &'reg mut crate::defuse::environment::Environment,
        config: ResolutionConfig,
        outer: &'reg super::Position<'reg>,
    ) -> Self {
        Self::at(core, env, config, super::Position::enclosed_by(outer))
    }

    fn at(
        core: &'reg mut crate::resolution::ResolverCore<'db>,
        env: &'reg mut crate::defuse::environment::Environment,
        config: ResolutionConfig,
        lexical: super::Position<'reg>,
    ) -> Self {
        Self {
            core,
            env,
            config,
            lexical,
            correlation_witness: Default::default(),
            cover_cell: None,
            in_correlation: false,
            pivot_in_values: std::collections::HashMap::new(),
            last_operator_output: None,
            operator_input: None,
            ho_caller_row: crate::pipeline::resolver::CallerRow::Absent,
            crossing_carriers: Vec::new(),
            window_obligation: None,
            position_grade: crate::defuse::bound_use::CallableGrade::RowWise,
        }
    }

    /// A CHILD FOLD over the same core and the SAME world, ENCLOSED by this
    /// fold's lexical position: the road every interior resolution takes.
    /// It sees the row it is correlated to through a borrow of this
    /// position — nothing lexical is copied, and this fold cannot move its
    /// frames while the child lives. The formals and the position's grade
    /// flow into it.
    pub(crate) fn child(&mut self) -> ResolverFold<'_, 'db> {
        let mut child = ResolverFold::enclosed(
            &mut *self.core,
            &mut *self.env,
            self.config.clone(),
            &self.lexical,
        );
        child.position_grade = self.position_grade;
        child
    }

    /// A CHILD FOLD at a ROOT position — a closed world that may read
    /// nothing of the row it was written beside.
    pub(crate) fn child_closed(&mut self) -> ResolverFold<'_, 'db> {
        let mut child = ResolverFold::new(&mut *self.core, &mut *self.env, self.config.clone());
        child.position_grade = self.position_grade;
        child
    }

    /// Resolve an INTERIOR expression — a subquery, an inner relation, a
    /// probe — in a child fold enclosed by this position.
    pub(crate) fn resolve_interior(
        &mut self,
        expr: ast_unresolved::Chain,
    ) -> Result<ResolvedRelation> {
        use super::{CorrelatingRun, OwnedCorrelatingRun};
        // LOOKING LEFT REACHES THE ENCLOSING ROW. A dequalifying access on the
        // interior's OWN head has nothing to its left inside, so the lvar it
        // renames onto is the outer one and the step is a correlation. Any other
        // position keeps its claimant: a member's dequalify is the join's USING,
        // and the pipe carrier correlates on its own road.
        // BOTH SPELLINGS OF THE RUN. `.(cols)` names the shared columns and `.*`
        // asks for every one there is; they are one step with two spellings, so
        // one reaching this road and the other not is the run answering
        // differently for the same query.
        // The row a USING correlation looks left into is every position in
        // view here — the frames this fold stands over and what encloses
        // them — read before the interior opens.
        let row = self.lexical.ports_in_view(&self.core.identities)?;
        let correlating = self
            .lexical
            .encloses_a_row()
            .then_some(&row)
            .and_then(|outer| {
                if !matches!(
                    expr.head().form(),
                    ast_unresolved::GroundForm::Reference(ast_unresolved::Relation::Ground { .. })
                ) {
                    return None;
                }
                match expr.head_access()? {
                    ast_unresolved::Access::Dequalify(columns) => {
                        Some((OwnedCorrelatingRun::Named(columns.clone()), outer.to_vec()))
                    }
                    ast_unresolved::Access::DequalifyAll => {
                        Some((OwnedCorrelatingRun::All, outer.to_vec()))
                    }
                    ast_unresolved::Access::Unasked
                    | ast_unresolved::Access::All
                    | ast_unresolved::Access::Slots(_) => None,
                }
            });
        let mut expr = expr;
        if correlating.is_some() {
            if let Some(ast_unresolved::Continuation::Access { access, .. }) = expr
                .continuations_mut()
                .first_mut()
                .map(|step| step.form_mut())
            {
                *access = ast_unresolved::Access::All;
            }
        }

        let mut child = self.child();
        let resolved = child.resolve_relational(expr)?;
        match correlating {
            // A USING correlation drops rows and republishes nothing: the
            // filters are computed against the base the carrier peels, and
            // the carrier keeps its own relation throughout.
            Some((run, outer)) => {
                let identities = child.core.identities;
                resolved.restricted_at_base(&identities, |base| match run.borrow() {
                    CorrelatingRun::Named(columns) => {
                        super::resolving::build_using_correlation_filters(
                            columns,
                            &outer,
                            base,
                            &identities,
                        )
                    }
                    CorrelatingRun::All => super::resolving::build_using_all_correlation_filters(
                        &outer,
                        base,
                        &identities,
                    ),
                })
            }
            None => Ok(resolved),
        }
    }

    /// THE RESOLVER'S OWN DOOR. It answers with the carrier, so nothing
    /// between the resolution and its consumer holds the relation beside a
    /// scope — which the walk's `transform_relational` cannot do, because
    /// the AST transform's shape is a chain and a chain alone.
    pub fn resolve_relational(&mut self, expr: ast_unresolved::Chain) -> Result<ResolvedRelation> {
        self.resolve_relational_impl(expr)
    }

    /// Core relational resolution logic. What encloses the chain is the
    /// fold's lexical position — no parameter carries it.
    #[stacksafe::stacksafe]
    pub(super) fn resolve_relational_impl(
        &mut self,
        expr: ast_unresolved::Chain,
    ) -> Result<ResolvedRelation> {
        // The fold reads the chain from the OUTSIDE in: the last
        // continuation is the one whose operand is everything before it,
        // which IS the chain minus that continuation.
        // The base of the fold is the chain's READ: the head together with
        // the access its own parens asked for. Resolving a mention needs
        // both — which dimensions the read publishes is what the access
        // says — so the two are never handed to this authority apart.
        // The base of the fold is the chain's READ: the head together with
        // the access its own parens asked for. Both reach one authority.
        let mut expr = expr;
        let Some(last) = expr.pop_step() else {
            let (head, access, _) = expr.split_head_access();
            return match head.into_form() {
                ast_unresolved::GroundForm::Reference(rel) => {
                    self.resolve_relation_impl(rel, access)
                }
                ast_unresolved::GroundForm::Literal(anon) => self.resolve_anon_table_impl(anon),
            };
        };
        match last.into_form() {
            // A dimension access standing on the relation the chain has
            // built. It resolves in the SAME run the pipes do — the
            // available columns, qualifier scope, liminal classification and
            // DML shape a pipe segment sees are what an access sees, and
            // splitting the run is what would give them two contexts.
            step @ ast_unresolved::Continuation::Access { .. } => {
                expr.continuations_mut()
                    .push(ast_unresolved::Step::authored(step));
                self.r_resolve_pipe(expr)
            }
            ast_unresolved::Continuation::Restrict {
                condition, origin, ..
            } => self.r_resolve_filter(expr, condition, origin),

            // A whole-heading correlation names two ARMS. Resolution answers
            // each spelling with the scope that arm published; which PAIR of
            // the run they are is the refiner's question, not this one's.
            ast_unresolved::Continuation::Correlate { whole, .. } => {
                let resolved = self.resolve_relational(expr)?;
                // A correlation arm is answered against the SOURCE's own
                // frontier — the arms of the run it stands on — exactly as
                // a predicate's qualified reference is.
                self.lexical.enter(resolved, super::Reach::Local);
                let owner = |fold: &Self, spelling: &delightql_types::SqlIdentifier| {
                    fold.lexical
                        .correlation_owner(spelling, &fold.core.identities)
                };
                let whole = match whole {
                    ast_unresolved::WholeHeading::ByName { left, right } => {
                        ast_resolved::WholeHeading::ByName {
                            left: owner(self, &left)?,
                            right: owner(self, &right)?,
                        }
                    }
                    ast_unresolved::WholeHeading::ByPosition { left, right } => {
                        ast_resolved::WholeHeading::ByPosition {
                            left: owner(self, &left)?,
                            right: owner(self, &right)?,
                        }
                    }
                };
                let resolved = self.lexical.leave();
                Ok(resolved.transparently(ast_resolved::Transparent::Correlate { whole }))
            }

            ast_unresolved::Continuation::Bound { bound, .. } => self.r_resolve_bound(expr, bound),

            ast_unresolved::Continuation::Destructure {
                source,
                pattern,
                mode,
                ..
            } => self.r_resolve_destructure(expr, *source, pattern, mode),

            ast_unresolved::Continuation::Member {
                rhs,
                correlation,
                join_type,
                ..
            } => {
                if direct_dml_terminal(&expr)? || direct_dml_terminal(&rhs)? {
                    return Err(dml_multi_terminal_error());
                }
                self.r_resolve_join(expr, rhs, correlation, join_type)
            }

            // The trailing pipes resolve as one run: the chain already holds
            // them flat, so there is no pipe-spine recursion to eliminate.
            // The structural forms — ordering, reposition, meta, the
            // witnesses, drill and narrowing — are steps of the same run.
            operator @ (ast_unresolved::Continuation::Pipe { .. }
            | ast_unresolved::Continuation::Structural(_)) => {
                expr.continuations_mut()
                    .push(ast_unresolved::Step::authored(operator));
                self.r_resolve_pipe(expr)
            }

            ast_unresolved::Continuation::BagOp { operator, arm, .. } => {
                if direct_dml_terminal(&expr)? || direct_dml_terminal(&arm)? {
                    return Err(dml_multi_terminal_error());
                }
                self.r_resolve_set_op(operator, expr, arm)
            }

            ast_unresolved::Continuation::ErJoin(step) if step.transitive => self
                .r_resolve_er_transitive(
                    expr,
                    step.rhs,
                    step.left_spelling,
                    step.right_spelling,
                    step.context,
                ),

            ast_unresolved::Continuation::ErJoin(step) => {
                // A direct edge run: the head plus every `&` step, read as
                // the pair sequence the resolver expands.
                expr.continuations_mut()
                    .push(ast_unresolved::Step::authored(
                        ast_unresolved::Continuation::ErJoin(step),
                    ));
                if !matches!(expr.head().form(), ast_unresolved::GroundForm::Reference(_)) {
                    return Err(er_operand_error());
                }
                let (head, steps) = expr.split_read();
                let mut relations = vec![head];
                let mut term_spellings = Vec::new();
                let mut contexts = Vec::new();
                for continuation in steps {
                    let ast_unresolved::Continuation::ErJoin(step) = continuation.into_form()
                    else {
                        return Err(er_operand_error());
                    };
                    if term_spellings.is_empty() {
                        term_spellings.push(step.left_spelling);
                    }
                    term_spellings.push(step.right_spelling);
                    contexts.push(step.context);
                    relations.push(step.rhs);
                }
                self.r_resolve_er_join_chain(relations, term_spellings, contexts)
            }
        }
    }

    // ── Extracted match-arm methods ─────────────────────────────────────

    fn r_resolve_er_join_chain(
        &mut self,
        relations: Vec<ast_unresolved::Chain>,
        term_spellings: Vec<String>,
        contexts: Vec<Option<String>>,
    ) -> Result<ResolvedRelation> {
        let context = super::er_chain_context(&contexts)?;

        // The published schema is the pair schema (GROUNDING-AND-MENTION):
        // a direct call publishes its WRITTEN terms' exports — helpers and
        // computed body columns never cross the entity boundary.
        let published: Vec<String> = relations
            .iter()
            .map(|rel| super::er_endpoint(rel).0)
            .collect();

        Ok(super::expand_er_join_chain(
            relations,
            &term_spellings,
            &context,
            self,
            Some(published),
        )?)
    }

    fn r_resolve_er_transitive(
        &mut self,
        left: ast_unresolved::Chain,
        right: ast_unresolved::Chain,
        left_spelling: String,
        right_spelling: String,
        context: Option<String>,
    ) -> Result<ResolvedRelation> {
        let context = super::er_chain_context(std::slice::from_ref(&context))?;

        Ok(super::expand_er_transitive_join(
            left,
            right,
            &left_spelling,
            &right_spelling,
            &context,
            self,
        )?)
    }

    /// One bag STEP: the chain-so-far against one arm. A three-arm
    /// expression reaches here twice, and each visit settles exactly the
    /// pair it sees — the heading a step publishes is the one its own two
    /// operands make.
    fn r_resolve_set_op(
        &mut self,
        operator: ast_unresolved::SetOperator,
        left: ast_unresolved::Chain,
        arm: ast_unresolved::Chain,
    ) -> Result<ResolvedRelation> {
        let left = self.resolve_relational(left)?;
        let arm = self.resolve_relational(arm)?;
        // ONE DERIVATION, owned by the act that consumes both arms.
        ResolvedRelation::bagged(left, arm, operator, &self.core.identities)
    }

    fn r_resolve_filter(
        &mut self,
        source: ast_unresolved::Chain,
        condition: ast_unresolved::TruthExpression,
        origin: ast_resolved::FilterOrigin,
    ) -> Result<ResolvedRelation> {
        // Check for EXISTS in the condition and handle through registry
        {
            if let ast_unresolved::TruthExpression::Existence(Existence {
                relation: subquery,
                polarity,
                addressing:
                    ProbeAddressing {
                        identifier: _,
                        using_columns,
                    },
            }) = &condition
            {
                // The correlation context and the source that survives are one
                // resolution. Resolving the source a second time to build the
                // context mints a parallel set of occurrences, and the
                // subquery's outer references bind to the set that is then
                // thrown away — leaving them owned by a scope no FROM entry
                // establishes.
                let resolved_source = self.resolve_relational(source.clone())?;
                let source_scope = resolved_source.semantic_relation();
                let available_columns =
                    crate::relation::published_ports(&self.core.identities, &source_scope)?;

                // THE INTERIOR STANDS INSIDE THE SOURCE ROW. Interdependent
                // EXISTS (`+orders(...), +order_items(...), +products(,
                // order_items.x = products.y)`) addresses sibling witnesses
                // BY NAME, so each sibling enters the frame as a relation —
                // its ports answer bare references and the relation itself
                // answers `orders.id`. They are read off the source resolved
                // just above — the one the statement will contain — for the
                // same reason the source is resolved once.
                let resolved_subquery = {
                    let subquery_expr = *subquery.clone();
                    self.lexical
                        .enter(resolved_source.with_exists_witnesses(), super::Reach::Row);
                    // Config swap for EXISTS: validate_in_correlation = true
                    let exists_config = ResolutionConfig {
                        validate_in_correlation: true,
                        ..self.config.clone()
                    };
                    let saved_config = std::mem::replace(&mut self.config, exists_config);
                    // An EXISTS interior is resolved ENCLOSED by the source
                    // row and correlated by the synthesis below; the
                    // interior entrance's own USING correlation is not its
                    // road.
                    let result = self.child().resolve_relational(subquery_expr);
                    self.config = saved_config;
                    let resolved = result.map(ResolvedRelation::into_body);
                    (self.lexical.leave(), resolved)
                };
                let (resolved_source, resolved_subquery) = resolved_subquery;
                let resolved_subquery = resolved_subquery?;

                // Synthesize correlation predicates from USING columns
                let final_subquery = super::resolving::synthesize_using_correlation(
                    resolved_subquery,
                    using_columns,
                    &available_columns,
                    &self.core.identities,
                )?;

                // Create resolved EXISTS condition
                let resolved_condition = ast_resolved::TruthExpression::Existence(Existence {
                    polarity: *polarity,
                    relation: Box::new(final_subquery),
                    addressing: (),
                });

                return Ok(
                    resolved_source.transparently(ast_resolved::Transparent::Restrict {
                        condition: resolved_condition,
                        origin,
                    }),
                );
            }
        }

        let resolved_source = self.resolve_relational(source)?;

        // THE CONDITION CONSTRAINS THE SOURCE ROW: it resolves over the
        // source as a row frame, reaching the enclosing row for a name the
        // source does not publish. Whether it stands in a correlation is
        // whether anything encloses that frame — unless validate_in_
        // correlation is set (EXISTS subqueries, where the full column set
        // is known and validation is safe).
        self.lexical.enter(resolved_source, super::Reach::Row);
        self.in_correlation = self.lexical.has_enclosing() && !self.config.validate_in_correlation;
        let saved_witness = std::mem::take(&mut self.correlation_witness);
        let resolved_condition = self.transform_boolean(condition);
        let witness = std::mem::replace(&mut self.correlation_witness, saved_witness);
        let resolved_source = self.lexical.leave();
        let resolved_condition = resolved_condition?;
        // A condition attached to an interior relation whose every name was
        // answered by the ENCLOSING row constrains nothing about the relation
        // it is attached to: the subquery is not correlated, and the predicate
        // it appears to carry is decided outside it. Silently that reads as a
        // plausible number — `country = users.country` over a relation
        // publishing only `n` becomes `users.country = users.country`.
        //
        // PROVISIONAL: whether this refuses, and behind which danger gate,
        // is still an open question; this keeps the shape loud until it is
        // answered rather than letting a wrong answer become a baseline.
        if witness.escaped && !witness.anchored {
            return Err(DelightQLError::validation_error_categorized(
                "resolution/correlation/uncorrelated_predicate",
                "every name in this condition was answered by the enclosing relation,                  so it constrains nothing about the relation it is attached to — an                  interior relation reads the heading its source PUBLISHES, and a name                  absent from that heading reaches outward",
                "attach the condition where the columns it names are published — inside                  the argument rather than on the call's result — or move it out of the                  interior relation, which is where a condition about the enclosing row                  belongs",
            ));
        }

        // A restriction drops rows and touches no heading, so what it
        // publishes is what its source published.
        Ok(
            resolved_source.transparently(ast_resolved::Transparent::Restrict {
                condition: resolved_condition,
                origin,
            }),
        )
    }

    fn r_resolve_join(
        &mut self,
        left: ast_unresolved::Chain,
        right: ast_unresolved::Chain,
        correlation: Option<ast_unresolved::MemberCorrelation>,
        join_type: Option<JoinType>,
    ) -> Result<ResolvedRelation> {
        // A relational call may consume the exact relation already standing
        // to its left. Keep that affine carrier whole: construction either
        // spends it or ordinary join assembly takes it back.
        let right_is_tvf = matches!(
            right.as_read_relation(),
            Some(ast_unresolved::Relation::FunctorCall { call: _, .. })
        );
        let resolved_left = self.resolve_relational(left)?;

        // THE RIGHT MEMBER IS LEXICALLY INSIDE THE JOIN'S LEFT ROW. The
        // left carrier itself is the frame the right resolves under — so
        // `o.id` is answered by the authored `o` occurrence, never by
        // recovering an owner from a candidate column — and it is the one
        // relation a callable on the right may take as its caller row.
        // Entered here, left below by the road that assembles the join or
        // absorbed by the call that consumed it.
        self.lexical.enter(resolved_left, super::Reach::Row);
        if right_is_tvf {
            self.ho_caller_row = crate::pipeline::resolver::CallerRow::Framed;
        }
        // THE LEFT OPERAND COMES BACK FROM THE FRAME, exactly once, to the
        // road that assembles the join.
        let take_left = |fold: &mut Self| fold.lexical.leave();

        // Check if right side uses positional patterns and needs unification
        let right_anon = match (right.head().form(), right.continuations().is_empty()) {
            (ast_unresolved::GroundForm::Literal(anon), true) => Some(anon.clone()),
            _ => None,
        };
        let join = if right_is_tvf {
            let resolved = self.resolve_relational(right)?;
            // THE ROW SAYS WHAT BECAME OF IT. A call that absorbed the
            // left member already stands for it, so assembling a join
            // here would read it twice.
            match std::mem::replace(
                &mut self.ho_caller_row,
                crate::pipeline::resolver::CallerRow::Absent,
            ) {
                crate::pipeline::resolver::CallerRow::Framed => {
                    ResolvedRelation::joining(take_left(self), resolved)
                }
                crate::pipeline::resolver::CallerRow::Absorbed => return Ok(resolved),
                crate::pipeline::resolver::CallerRow::Absent => {
                    return Err(DelightQLError::transformation_error(
                        "a callable left its resolved construction row unaccounted for",
                        "join construction",
                    ))
                }
            }
        } else if let Some(ast_unresolved::AnonRelation {
            table,
            alias: anon_alias,
            ..
        }) = right_anon
        {
            let column_headers = table.body.header.as_ref().map(|header| {
                header
                    .iter()
                    .map(|item| {
                        item.term()
                            .expect("a tabular header slot has a domain term")
                    })
                    .collect::<Vec<_>>()
            });
            // Handle anonymous table unification
            let resolved = self.resolve_relational(right.clone())?;

            // Whether the headers unify, and whether they make a
            // membership test rather than a relation, is decided by the
            // join itself — over both headings it owns and what answers
            // over the left one.
            match ResolvedRelation::joining(take_left(self), resolved).unifying_anonymously(
                column_headers.as_deref(),
                anon_alias.as_ref(),
                &self.core.identities,
            )? {
                super::AnonRouting::Membership(restricted) => return Ok(restricted),
                super::AnonRouting::Join(join) => join,
            }
        } else if let Some(rel) = right.as_read_relation().cloned() {
            let right_access = right.head_access().cloned();
            match (&rel, right_access.as_ref()) {
                (
                    ast_unresolved::Relation::Ground {
                        mention:
                            ast_unresolved::GroundMention::Named {
                                identifier, alias, ..
                            },
                        outer,
                    },
                    Some(ast_unresolved::Access::Slots(patterns)),
                ) => {
                    // Use the SAME pattern resolver that single tables use!
                    let table_name = &identifier.name;
                    // Get table schema — check CTEs first, then database.
                    // BOTH branches yield rich ColumnMetadata: squeezing a
                    // CTE's columns through ColumnInfo (the narrow
                    // database-boundary type) would strip every value fact
                    // the thin type cannot hold — the interior heading of a
                    // staged tree dying BY TYPE, with nullability
                    // hardcoded true. Value facts are conserved (the
                    // carrying law); only identity is rebuilt below.
                    let maybe_table_columns: Option<(
                        crate::relation::SemanticRelation,
                        Vec<crate::relation::PortId>,
                    )> = if let Some(crate::defuse::environment::QueryLocalSelection::Relation(
                        cte_schema,
                    )) = self.env.select_query_local(
                        table_name,
                        crate::pipeline::asts::core::QueryLocalDemand::Relation,
                        None,
                    )? {
                        let cte_schema = cte_schema.relation();
                        Some((
                            cte_schema,
                            crate::relation::published_ports(&self.core.identities, &cte_schema)?,
                        ))
                    } else {
                        let resolved_database = if !identifier.namespace_path.is_empty() {
                            // The one lookup authority answers qualified
                            // names too; this position judges the closed
                            // answer and never searches again.
                            let (answer, _serve) = self.env.relation_qualified(
                                self.core,
                                &identifier.namespace_path,
                                table_name,
                                false,
                            )?;
                            match answer {
                                crate::defuse::environment::RelationAnswer::DatabaseEntity(
                                    entity,
                                )
                                | crate::defuse::environment::RelationAnswer::MaterializedRelation(
                                    entity,
                                ) => {
                                    let crate::resolution::EntityDefinition::RelationSchema(
                                        schema,
                                    ) = entity.definition;
                                    Some((schema, entity.canonical_name, entity.backend_schema))
                                }
                                _ => None,
                            }
                        } else {
                            match self.env.relation(self.core, table_name, alias.as_ref())? {
                                crate::defuse::environment::RelationAnswer::DatabaseEntity(entity)
                                | crate::defuse::environment::RelationAnswer::MaterializedRelation(
                                    entity,
                                ) => {
                                    let crate::resolution::EntityDefinition::RelationSchema(schema) =
                                        entity.definition;
                                    Some((schema, entity.canonical_name, entity.backend_schema))
                                }
                                _ => None,
                            }
                        };

                        if let Some((schema, canonical, backend_schema)) = resolved_database {
                            super::relation_resolver::bind_physical_relation(
                                schema,
                                canonical.as_ref(),
                                backend_schema.as_deref(),
                                &self.core.identities,
                            )?;
                            Some((
                                schema,
                                crate::relation::published_ports(&self.core.identities, &schema)?,
                            ))
                        } else {
                            None
                        }
                    };

                    if let Some((source_relation, table_columns)) = maybe_table_columns {
                        // CTE or database table — use existing mini-pipeline

                        // THE ONE ARGUMENTATIVE OPERATION, over the relation
                        // the lookup answered with: the slot row is judged
                        // and applied there, and the read comes back whole
                        // for the join that owns the left row.
                        let _ = table_columns;
                        let owner = match &alias {
                            Some(alias) => super::PatternOwner::Authored(alias.clone()),
                            None => super::PatternOwner::Unqualified,
                        };
                        let read = ResolvedRelation::patterned(
                            super::PatternOperand::Read {
                                scope: source_relation,
                                outer: *outer,
                            },
                            &ast_unresolved::Access::Slots(patterns.clone()),
                            owner,
                            self,
                        )?;
                        ResolvedRelation::joining_pattern(
                            take_left(self),
                            read,
                            &self.core.identities,
                        )?
                    } else {
                        // Not CTE or database — likely a consulted entity.
                        // Route through the full resolver which handles consulted
                        // entities (views, facts) and applies positional patterns.
                        // The READ goes whole: rebuilding the relation without
                        // the access it was read under hands the resolver a
                        // mention nobody parameterized.
                        let resolved = self.resolve_relational(right.clone())?;

                        // No name-derived join condition: the pattern's own
                        // resolution recorded each binding's exact reuse
                        // while the live bare interface was in hand, and the
                        // join step consumes that record. A second, name-only
                        // derivation here is what made one spelling behave
                        // differently per source kind.
                        ResolvedRelation::joining(take_left(self), resolved)
                    }
                }
                (
                    ast_unresolved::Relation::Ground { .. },
                    Some(ast_unresolved::Access::Dequalify(using_cols)),
                ) => {
                    // Dequalify on consulted views (or any non-positional entity):
                    // resolve the entity, then create USING join condition from the
                    // specified columns.
                    let using_cols = using_cols.clone();
                    let resolved = self.resolve_relational(right)?;
                    ResolvedRelation::joining(take_left(self), resolved)
                        .dequalifying(&using_cols, &self.core.identities)?
                }
                (
                    ast_unresolved::Relation::Ground { .. },
                    Some(ast_unresolved::Access::DequalifyAll),
                ) => {
                    // DequalifyAll: resolve the right side, then compute
                    // shared columns between left and right as USING columns.
                    let resolved = self.resolve_relational(right)?;
                    ResolvedRelation::joining(take_left(self), resolved)
                        .dequalifying_all(&self.core.identities)?
                }
                _ => {
                    let resolved = self.resolve_relational(right)?;
                    ResolvedRelation::joining(take_left(self), resolved)
                }
            }
        } else {
            let resolved = self.resolve_relational(right)?;
            ResolvedRelation::joining(take_left(self), resolved)
        };

        // An authored member correlation is a CONDITION — a correspondence
        // is read off the access that directs it, so `Correspond` has no
        // inhabitant here and no arm to write. The deferral and the needs
        // it bubbles are ONE act on the member itself.
        let join = if let Some(ast_unresolved::MemberCorrelation::Condition(cond)) = correlation {
            join.deferring_authored_condition(cond, self)?
        } else {
            join
        };
        // THE OPERANDS' EXACT INTERFACES, for the judgments this road makes
        // before the join act derives its own correspondence from them.
        let left_scope = join.left().semantic_relation();
        let right_scope = join.right().semantic_relation();
        // A JOIN NEEDS BOTH HEADINGS. Correlation, implicit unification and
        // the concatenated interface are all decided from the operands'
        // dimensions; an operand that publishes none has nothing to decide
        // them from, and a statement built anyway would name columns no
        // target has.
        for operand in [&left_scope, &right_scope] {
            if self
                .core
                .identities
                .authority()
                .interface(operand)?
                .is_opaque()
            {
                return Err(crate::pipeline::resolver::opaque_reference_refusal());
            }
        }
        // TWO LIVE SCOPES NEVER SHARE A NAME, judged at activation — the
        // arms become co-addressable HERE, before glob, ordinal,
        // qualification, or metadata can select or merge answers. The ruled
        // exception is the acknowledged danger gate. Only an AUTHORED
        // environment is judged: instantiated bodies and compiler-built
        // queries are replays. The judgment is the frontier's: it alone
        // knows which relations become addressable together.
        let policy = if self
            .config
            .danger_gates
            .is_enabled("delightql-danger://scope/duplicate")
        {
            crate::names::DuplicateScopePolicy::Acknowledged
        } else {
            crate::names::DuplicateScopePolicy::Refuse
        };
        // A truth-witness arm (`+`/`\+`) answers to the relation it probes
        // so a correlation can address it, but the existence overlap is a
        // TRUTH: the witness is not a live row-space relation and enters no
        // duplicate judgment, here or in any enclosing composition.
        for side in [join.left(), join.right()] {
            if side.is_truth_witness() {
                self.core
                    .identities
                    .mark_truth_witness(side.semantic_relation().scope());
            }
        }
        if self.config.authored_environment {
            join.admitting_distinct_names(policy, &self.core.identities)?;
        }

        // Create the join. ONE DESCRIPTION: the variant says both what the
        // step is and what law its result comes from, and the left operand
        // is the chain's own rather than a second relation stated here.
        join.joined(join_type, &self.core.identities)
    }

    /// Detect `ns::(*).liminal(…)` — a `liminal` interior drill whose source
    /// is a catalog functor (a Ground in sys::meta whose name ends in `::`,
    /// the wrapper naming convention of `register_catalog_wrapper`) — and
    /// build the wrapper query that carries the namespace's liminal
    /// ledger as a fourth tree-group column beside entities/namespaces/name
    /// (EFFECT-ALGEBRA §8, THE LIMINAL RELATION). Returns the pieces
    /// r_resolve_view_query needs; None falls through to the ordinary
    /// path (not a liminal drill, not a catalog functor, or no such
    /// namespace — the stock "Table not found" then reports as today).
    fn liminal_catalog_expansion(
        &self,
        base: &ast_unresolved::Chain,
        first_drill: Option<&crate::pipeline::asts::core::operators::AuthoredDrill>,
    ) -> Result<
        Option<(
            delightql_types::SqlIdentifier,
            ast_unresolved::Query,
            ast_unresolved::Access,
            Option<delightql_types::SqlIdentifier>,
            bool,
        )>,
    > {
        let Some(drill) = first_drill else {
            return Ok(None);
        };
        if drill.column != "liminal" {
            return Ok(None);
        }
        let (
            Some(ast_unresolved::Relation::Ground {
                mention:
                    ast_unresolved::GroundMention::Named {
                        identifier, alias, ..
                    },
                outer,
                ..
            }),
            Some(access),
        ) = (base.as_read_relation(), base.head_access())
        else {
            return Ok(None);
        };
        let ns_parts: Vec<&str> = identifier
            .namespace_path
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        if ns_parts != ["sys", "meta"] {
            return Ok(None);
        }
        let wrapper_name = identifier.name.as_str();
        let Some(ns_typed) = wrapper_name.strip_suffix("::") else {
            return Ok(None);
        };
        let Some(system) = self.core.database.system else {
            return Ok(None);
        };
        let Some((ns_fq, echo_columns)) = system.liminal_echo_columns(ns_typed)? else {
            return Ok(None);
        };
        let query = liminal_wrapper_query(&ns_fq, &echo_columns, &self.core.identities)?;
        Ok(Some((
            identifier.name.clone(),
            query,
            access.clone(),
            alias.clone(),
            *outer,
        )))
    }

    /// The bubbled needs a pipe boundary must still find among the
    /// source's columns: an unqualified name the active formal frame
    /// binds is a PARAMETER reference, answered by the caller-resolved
    /// actual where the expression itself resolves.
    fn needs_beyond_frame(
        &self,
        needs: &[super::unification::ColumnReference],
    ) -> Vec<super::unification::ColumnReference> {
        needs
            .iter()
            .filter(|need| {
                !matches!(
                    need,
                    super::unification::ColumnReference::Named { name, qualifier: None, .. }
                        if self.env.covers_value_formal(name)
                )
            })
            .cloned()
            .collect()
    }

    fn r_resolve_pipe(&mut self, expr: ast_unresolved::Chain) -> Result<ResolvedRelation> {
        // The trailing run, in source order, and the relation it shapes.
        //
        // ONE RUN. An access and a pipe operator are different steps but the
        // same walk: the available columns, qualifier scope, liminal
        // classification, DML shape and descriptor roles a pipe segment sees
        // are what an access sees, and collecting them separately would give
        // one of them a context the other never had.
        let mut base = expr;
        let mut segments: Vec<ast_unresolved::RunForm> = Vec::new();
        // THE PARTITION IS THE MEMBERSHIP: each pop either returns the
        // run-step family or restores the step and ends the run — no
        // second list, no reachable panic. `pop_run_step` never crosses
        // the head span: the leading continuations inside it are the
        // HEAD'S OWN READ, never run steps.
        while let Some(step) = base.pop_run_step() {
            segments.push(step.into_form());
        }
        segments.reverse();

        let pivot_in_values;
        // THE RUN'S STATE IS ONE VALUE: the relation standing here and
        // what answers over it. Each step's crossing consumes it and
        // answers with the next, so nothing in this loop holds a crossed
        // relation beside a scope the crossing did not derive.
        let mut standing;

        // THE LIMINAL RELATION (EFFECT-ALGEBRA §8): `ns::(*).liminal(*)`
        // drills into the namespace's liminal ledger beside `entities` and
        // `namespaces`. The stored catalog-wrapper definition carries only
        // (entities, namespaces, name) — the ledger's presented schema is the
        // corresponding union of THAT NAMESPACE's receipts, knowable only at
        // resolve time — so when the drill's column is `liminal` and its
        // source is a catalog functor, the source expands from a SYNTHESIZED
        // wrapper body (generator ⨯ ledger tree group carrying this
        // namespace's echo columns) through the ordinary consulted-view road.
        // Bare `ns::(*)` reads and entities/namespaces drills never enter
        // here and keep the stored definition byte-for-byte (pinned by
        // scratch_home--02, home_namespace--01, nesting--10). Pinned by
        // effects/liminal--43, --45,
        // `liminal_drill_presents_the_corresponding_union`.
        let first_drill = segments.first().and_then(|step| match step {
            ast_unresolved::RunForm::Structural(ast_unresolved::StructuralStep {
                form: ast_unresolved::StructuralForm::Drill { drill },
                ..
            }) => Some(drill),
            _ => None,
        });
        let liminal_expansion = self.liminal_catalog_expansion(&base, first_drill)?;

        {
            // Resolve the base expression through registry.
            // If base is Pipe(HoView, ...), recursion handles the expansion.
            let resolved_base = match liminal_expansion {
                Some((view_name, query, access, alias, outer)) => {
                    let resolved_query = crate::defuse::bound_use::resolve_synthesized_body(
                        self,
                        "sys::meta",
                        &view_name,
                        query,
                    )?;
                    super::relation_resolver::finish_view_access(
                        view_name,
                        crate::relation::form::DefinitionKind::View,
                        resolved_query,
                        access,
                        alias,
                        outer,
                        self,
                    )?
                }
                None => self.resolve_relational(base)?,
            };
            standing = resolved_base;

            // THE PIVOT'S KEYS ARE READ AFTER RESOLUTION, AND ONLY THERE. A
            // name and an ordinal are the same column occurrence by now, which
            // is what lets one answer serve both addressings; before
            // resolution an ordinal has no name and the question cannot be
            // asked at all. Reading it twice and merging gave the two
            // addressings two different laws — the earlier answer entered
            // `Or` and `Not`, where an `IN` constrains nothing exhaustively,
            // so a named key produced pivot columns that the other arm of the
            // `or` could add rows outside of, while the same query written
            // positionally refused.
            pivot_in_values = standing.pivot_keys(&self.core.identities);
        }

        // Iterate the run bottom-up (innermost step first)
        for step in segments {
            // Narrowing a knowable object literal is a provable mistake —
            // refuse while the anon source is still in hand.
            if let ast_unresolved::RunForm::Structural(ast_unresolved::StructuralStep {
                form: ast_unresolved::StructuralForm::Narrow { nest, .. },
                ..
            }) = &step
            {
                if let crate::pipeline::asts::core::Reference::Named(
                    crate::pipeline::asts::core::NamedReference(authored),
                ) = nest
                {
                    standing.refusing_knowable_object_narrowing(
                        authored.name.as_str(),
                        &self.core.identities,
                    )?;
                }
            }

            // Check for unresolved columns before pipe (scope barrier)
            if let Some(first_unresolved) = standing.owes().first() {
                let qual_str = match first_unresolved {
                    ColumnReference::Named {
                        name, qualifier, ..
                    } => qualifier
                        .as_ref()
                        .map(|q| format!("{}.{}", q, name))
                        .unwrap_or_else(|| name.to_string()),
                    ColumnReference::Ordinal {
                        position, reverse, ..
                    } => column_ordinal_text(*position, *reverse),
                };

                return Err(DelightQLError::ColumnNotFoundError {
                    column: qual_str,
                    context:
                        "Column reference before pipe operator cannot be resolved (scope barrier)"
                            .to_string(),
                });
            }

            let available_columns = crate::relation::published_ports(
                &self.core.identities,
                &standing.semantic_relation(),
            )?;

            // USING→correlation intercept
            if let ast_unresolved::RunForm::Access {
                access: ast_unresolved::Access::Dequalify(ref columns),
                ..
            } = step
            {
                if self.lexical.encloses_a_row() {
                    // A correlation reworks the relation and republishes
                    // nothing, so what answers over it travels through
                    // rather than being chosen again here.
                    // A USING correlation drops rows and republishes
                    // nothing: the filters are computed against the base
                    // the carrier peels, and the carrier keeps its own
                    // relation throughout.
                    let identities = self.core.identities;
                    let outer = self.lexical.ports_in_view(&identities)?;
                    standing = standing.restricted_at_base(&identities, |base| {
                        super::resolving::build_using_correlation_filters(
                            columns,
                            &outer,
                            base,
                            &identities,
                        )
                    })?;
                    continue;
                }
            }

            // THE STEP, RESOLVED AND SEALED. The authored node goes in
            // whole: its member, its payload and the answer written at its
            // position come off that one node, and the closure below
            // receives only the payload. Nothing here assembles the three.
            //
            // THE FORM BORROWS ITS INPUT through the position: the standing
            // relation is entered as the frame the form resolves over and
            // left again, by value, for the crossing that consumes it.
            // A SLOT ROW IN RUN POSITION — `… as u(a, b)`, or a row over what
            // a head published — is the one argumentative operation over the
            // standing relation: it consumes the carrier whole and publishes
            // the row's own interface, under the authored owner when one was
            // written.
            let step = match step {
                ast_unresolved::RunForm::Access {
                    access: access @ ast_unresolved::Access::Slots(_),
                    named,
                } => {
                    let owner = match named {
                        Some(name) => super::PatternOwner::Authored(name),
                        None => super::PatternOwner::Unqualified,
                    };
                    standing = ResolvedRelation::patterned(
                        super::PatternOperand::Standing(standing),
                        &access,
                        owner,
                        self,
                    )?
                    .restricted_by_its_own_constraints(&self.core.identities)?;
                    continue;
                }
                step => step,
            };
            let input = standing.semantic_relation();
            self.lexical.enter(standing, super::Reach::Stage);
            let resolved_step = super::pipe_form::ResolvedStep::of(step, |body| match body {
                super::pipe_form::StepBody::Access(access) => {
                    let (staged, _output) =
                        super::resolving::operators::schema_ops::resolve_access(
                            super::relation_resolver::resolve_schema_free_access(&access)?,
                            &available_columns,
                            input,
                            &self.core.identities,
                        )?;
                    Ok(staged)
                }
                super::pipe_form::StepBody::Pipe(operator) => {
                    // Bubble the operator to collect column needs
                    let (unresolved_operator, operator_bubbled) =
                        super::bubbling::bubble_unary_operator(operator, self)?;

                    // Validate that all operator needs can be satisfied.
                    // A need answered by the ACTIVE FORMAL FRAME is not a
                    // column of the source: a definition body's parameter
                    // reference is satisfied by the caller-resolved actual
                    // at the reference's own resolution, so it never stands
                    // as an unmet column here.
                    let unmet = self.needs_beyond_frame(&operator_bubbled);
                    if !unmet.is_empty() {
                        self.lexical.resolve_all(
                            unmet,
                            &self.core.identities,
                            "in pipe operator",
                        )?;
                    }

                    // Resolve the operator at the pipe boundary over the
                    // frame it stands on
                    self.pivot_in_values = pivot_in_values.clone();
                    self.operator_input = Some(input);
                    let staged = self.resolve_pipe_stage(unresolved_operator)?;
                    let _output = self
                        .last_operator_output
                        .take()
                        .expect("a resolved stage records the positions it publishes");
                    Ok(staged)
                }
                super::pipe_form::StepBody::Structural(step) => {
                    // Only an ordering carries expressions whose needs are
                    // validated against the source; the other structural
                    // steps address by name at their own resolution.
                    if let ast_unresolved::StructuralForm::Ordering { specs, .. } = &step.form {
                        let bubbled = super::bubbling::bubble_ordering_specs(specs, self)?;
                        let unmet = self.needs_beyond_frame(&bubbled);
                        if !unmet.is_empty() {
                            self.lexical.resolve_all(
                                unmet,
                                &self.core.identities,
                                "in pipe operator",
                            )?;
                        }
                    }
                    self.pivot_in_values = pivot_in_values.clone();
                    let (staged, _output) =
                        self.resolve_structural_step(step, &available_columns, input)?;
                    Ok(staged)
                }
            });
            let consumed = self.lexical.leave();
            let resolved_step = resolved_step?;

            // THE ONE CROSSING, and the only producer of what stands after
            // a step. The standing relation goes in by value and its
            // frontier dies there, so this loop cannot carry a spent
            // qualifier set forward — there is no copy of it to carry.
            standing = super::pipe_form::cross(consumed, resolved_step, &self.core.identities)?;
        }

        Ok(standing)
    }

    /// One structural step of the run, resolved with the same available
    /// columns the operators see. Each kind's judgment lives in its own
    /// resolving function; this match routes the EXACT structural family
    /// exhaustively — there is no other continuation to receive.
    fn resolve_structural_step(
        &mut self,
        step: ast_unresolved::StructuralStep,
        available: &[crate::relation::PortId],
        input: crate::relation::SemanticRelation,
    ) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
        use super::resolving::operators::{ordering, schema_ops};
        let (step, output) = match step.form {
            ast_unresolved::StructuralForm::Ordering { specs, bound } => {
                let (specs, _) = ordering::resolve_tuple_ordering_via_fold(self, specs, available)?;
                // AN ORDERING IS A PIPE FORM: it republishes its operand's
                // whole heading through the stage export, dequalified. What
                // stands after it is the stage's own publication, so the
                // ports the bind minted are the ports later steps resolve
                // against. The bound it consumed goes in WITH it: one act,
                // derived once, and the authority stamps the by-position
                // fact on what that act publishes.
                let (step, output) = self.core.identities.authority().bind(
                    crate::relation::pending::Pending::Ordering {
                        input,
                        specs,
                        bound,
                    },
                )?;
                Ok((step, output))
            }
            ast_unresolved::StructuralForm::Reposition { moves } => {
                schema_ops::resolve_reposition(self, moves, available, input)
            }
            ast_unresolved::StructuralForm::Meta => {
                schema_ops::resolve_meta_ize(input, &self.core.identities)
            }
            ast_unresolved::StructuralForm::Witness { polarity } => {
                schema_ops::resolve_witness(input, polarity, &self.core.identities)
            }
            ast_unresolved::StructuralForm::SignedWitness => {
                schema_ops::resolve_signed_witness(input, &self.core.identities)
            }
            ast_unresolved::StructuralForm::Drill { drill } => {
                let nest = self.addressed_nest(&crate::pipeline::asts::core::AuthoredColumn {
                    name: delightql_types::SqlIdentifier::new(drill.column.clone()),
                    qualifier: None,
                    namespace_path: crate::pipeline::asts::core::NamespacePath::empty(),
                })?;
                schema_ops::resolve_interior_drill_down(
                    drill.column,
                    nest.column,
                    drill.glob,
                    drill.columns,
                    drill.groundings,
                    input,
                    &self.core.identities,
                )
            }
            ast_unresolved::StructuralForm::Narrow { nest, pattern, .. } => {
                let crate::pipeline::asts::core::Reference::Named(
                    crate::pipeline::asts::core::NamedReference(authored),
                ) = &nest
                else {
                    return Err(DelightQLError::validation_error(
                        "a narrowing addresses its nest by name".to_string(),
                        "write the column's name",
                    ));
                };
                let spelled = authored.name.to_string();
                let nest = self.addressed_nest(authored)?;
                schema_ops::resolve_narrowing_destructure(
                    nest,
                    &spelled,
                    pattern,
                    input,
                    &self.core.identities,
                )
            }
        }?;
        Ok((step, output))
    }

    /// The mutation contract: who may be mutated, and what shapes may stand
    /// between the source and the mutation.
    ///
    /// ONE road. A mutation is written two ways and reaches this function by
    /// one of them, because the other becomes it before resolution starts.
    /// The `!!` evidence, the target agreement, and the shaping laws are read
    /// here and nowhere else — a second copy is how a source once reached SQL
    /// with its marker unexamined.
    fn enforce_mutation_contract(
        &mut self,
        kind: DmlVerb,
        target: &str,
        marks: &[(crate::names::ScopeId, crate::names::Spelling)],
        pipe_ops: &[DmlPipeKind],
        available_columns: &[crate::relation::PortId],
    ) -> Result<()> {
        let marked_names = || {
            marks
                .iter()
                .map(|(_, relation)| {
                    let mut text = String::new();
                    self.core
                        .identities
                        .write(*relation, &mut crate::names::Teaching(&mut text));
                    text
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        if marks.len() > 1 {
            return Err(DelightQLError::validation_error_categorized(
                "dml/marker/multiple",
                format!(
                    "DML source has !! on multiple relations: {}",
                    marked_names()
                ),
                "Only one relation can be marked with !! — the mutation target must be unambiguous",
            ));
        }

        match kind {
            DmlVerb::Insert => {
                if !marks.is_empty() {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/marker/forbidden",
                            format!("insert! source must not have !! marker (found on: {})", marked_names()),
                            "Remove !! from the source relation — insert reads from source, it does not mutate it".to_string(),
                        ));
                }
            }
            DmlVerb::Update | DmlVerb::Delete => {
                let kind_name = match kind {
                    DmlVerb::Update => "update!",
                    DmlVerb::Delete => "delete!",
                    _ => unreachable!(),
                };
                let Some((_, marked)) = marks.first() else {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/marker/missing",
                            format!("{} requires !! on the source relation that will be mutated", kind_name),
                            format!("Mark the source with !!: {}!!(*)  — this makes the mutation target explicit", target),
                        ));
                };
                // Both sides name a relation, and the identifier law folds
                // them the same way — so the comparison is of names as this
                // language means them, not of characters.
                let written = self.core.identities.intern(target, false);
                if self.core.identities.canonical(*marked)
                    != self.core.identities.canonical(written)
                {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/marker/mismatch",
                            format!("!! source table '{}' does not match {} target '{}'", marked_names(), kind_name, target),
                            format!("The !! marker must be on the same table as the DML target: {}!!(*)  |> {}({}(*))", target, kind_name, target),
                        ));
                }
            }
        }

        match kind {
            DmlVerb::Update => {
                let has_transform = pipe_ops
                    .iter()
                    .any(|op| matches!(op, DmlPipeKind::Transform));
                if !has_transform {
                    let has_non_filter_ops = pipe_ops.iter().any(|op| {
                        matches!(
                            op,
                            DmlPipeKind::ProjectOut
                                | DmlPipeKind::Rename
                                | DmlPipeKind::TupleOrdering
                                | DmlPipeKind::General
                                | DmlPipeKind::Group
                        )
                    });
                    if has_non_filter_ops {
                        return Err(DelightQLError::validation_error_categorized(
                                "dml/shape/update_no_transform",
                                "update! requires a Transform ($$) to specify column assignments — embed (+), project-out (-), rename (*), ordering (#), and projection do not produce SET clauses",
                                "Use $$(new_value as column_name) before update! to specify what to change",
                            ));
                    }
                } else {
                    let has_aggregate = pipe_ops.iter().any(|op| matches!(op, DmlPipeKind::Group));
                    if has_aggregate {
                        return Err(DelightQLError::validation_error_categorized(
                                "dml/source/aggregate",
                                "Cannot aggregate/group data before update! — aggregation changes the row identity, making it impossible to map results back to source rows",
                                "Remove the aggregate/group-by pipe before the DML operation",
                            ));
                    }
                    let transform_count = pipe_ops
                        .iter()
                        .filter(|op| matches!(op, DmlPipeKind::Transform))
                        .count();
                    if transform_count > 1 {
                        return Err(DelightQLError::validation_error_categorized(
                                "dml/shape/update_no_transform",
                                "update! requires exactly one Transform ($$) — multiple covers produce ambiguous SET clauses",
                                "Combine the transforms into a single $$(expr1 as col1, expr2 as col2) before update!",
                            ));
                    }
                    let has_ordering = pipe_ops
                        .iter()
                        .any(|op| matches!(op, DmlPipeKind::TupleOrdering));
                    if has_ordering {
                        return Err(DelightQLError::validation_error_categorized(
                                "dml/shape/update_no_transform",
                                "Ordering (#) before update! is meaningless — UPDATE does not preserve row order",
                                "Remove the ordering pipe from the DML pipeline",
                            ));
                    }
                }
            }
            DmlVerb::Delete => {
                let has_transform = pipe_ops
                    .iter()
                    .any(|op| matches!(op, DmlPipeKind::Transform));
                if has_transform {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/shape/delete_with_cover",
                            "delete! discards column data — a Transform ($$) before it is wasted",
                            "Remove the Transform before delete! — only filters affect which rows are deleted",
                        ));
                }
                // A shape operator before delete! is not waste by
                // construction: the rows that die are the ones the
                // source still identifies, and a projection DOWN to
                // the target's own columns is what makes that match
                // well-formed over a join. What is wasted — and
                // refused — is a shape that stops the source
                // publishing a column the target has, because the
                // match is then over a heading the target does not
                // share.
                let has_shape_ops = pipe_ops.iter().any(|op| {
                    matches!(
                        op,
                        DmlPipeKind::ProjectOut | DmlPipeKind::Rename | DmlPipeKind::General
                    )
                });
                if has_shape_ops {
                    let carried: Vec<_> = available_columns
                        .iter()
                        .filter_map(|column| self.core.identities.published_sym(column.column()))
                        .collect();
                    let dropped: Vec<String> = self
                        .core
                        .database
                        .schema()
                        .get_table_columns(None, target)?
                        .into_iter()
                        .flatten()
                        .filter_map(|column| {
                            let spelling = self.core.identities.intern(&column.name, false);
                            let name = self.core.identities.canonical(spelling);
                            (!carried.contains(&name)).then_some(column.name.to_string())
                        })
                        .collect();
                    if !dropped.is_empty() {
                        return Err(DelightQLError::validation_error_categorized(
                            "dml/shape/delete_with_cover",
                            format!(
                                "the source of this delete! no longer publishes \
                                     [{}], columns of '{target}' — the rows to delete \
                                     are identified by the target's whole heading, so a \
                                     shape that drops one of its columns identifies no rows",
                                dropped.join(", ")
                            ),
                            "keep the target's columns through the shaping pipes, or \
                                 drop the shaping and filter instead",
                        ));
                    }
                }
                let has_aggregate = pipe_ops.iter().any(|op| matches!(op, DmlPipeKind::Group));
                if has_aggregate {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/source/aggregate",
                            "Cannot aggregate/group data before delete! — aggregation changes the row identity",
                            "Remove the aggregate/group-by pipe before the DML operation",
                        ));
                }
            }
            DmlVerb::Insert => {
                // Insert is more permissive — projections, transforms, etc. are valid
                // for shaping the data before insertion. Aggregates are suspicious but
                // not necessarily wrong (e.g., insert aggregated results into a summary table).
            }
        }
        Ok(())
    }

    /// Operator resolution — delegates to `resolving::resolve_operator_via_fold`.
    pub(super) fn resolve_operator_impl(
        &mut self,
        operator: ast_unresolved::PipeOp,
        available: &[crate::relation::PortId],
        input: crate::relation::SemanticRelation,
        pivot_in_values: &super::PivotInWitnesses,
    ) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
        super::resolving::resolve_operator_via_fold(
            self,
            operator,
            available,
            input,
            pivot_in_values,
        )
    }

    /// ONE PIPE STAGE, resolved. What comes back is the NODE — the
    /// operation and what it publishes, written by the authority in one act
    /// — so nothing between here and the chain it lands on can pair a
    /// payload with another relation.
    ///
    /// NOT the walk's `transform_operator`. That one answers with a payload
    /// alone, which is exactly what a resolved stage cannot be: the pipe
    /// road here is the only one that resolves an operator, and it answers
    /// with the step.
    fn resolve_pipe_stage(&mut self, o: ast_unresolved::PipeOp) -> Result<ast_resolved::Step> {
        let available = self.lexical.local_ports(&self.core.identities)?;
        let pivot = self.pivot_in_values.clone();
        let input = self
            .operator_input
            .take()
            .expect("a pipe run supplies its exact semantic input");
        let (step, output_columns) = self.resolve_operator_impl(o, &available, input, &pivot)?;
        self.last_operator_output = Some(output_columns);
        Ok(step)
    }

    /// Resolve a DML call IN RELATION POSITION — the one invocation shape a
    /// mutation has. The call carries its target first and its source second
    /// (the descriptor's layout); the source resolves as the ordinary chain
    /// it is, THE MUTATION CONTRACT is enforced against that resolved source,
    /// and the target resolves through the physical mutation lookup — a
    /// normal relation access mints an output occurrence for its visible
    /// heading; a mutation target must instead name the catalog-owned scope
    /// that the SQL statement will delete, update, or insert into. The
    /// resolved chain is the call at the head with its receipt access beside
    /// it; no operator carrier stands between the source and the terminal.
    /// THE NEST A STRUCTURAL STEP OPENS, addressed through the frontier:
    /// the one occurrence the authored spelling names in the frame
    /// standing here, or the refusal it earned.
    fn addressed_nest(
        &mut self,
        authored: &crate::pipeline::asts::core::AuthoredColumn,
    ) -> Result<crate::pipeline::asts::core::ColumnOccurrence> {
        use super::unification::UnificationResult;
        let mut witness = super::Witness::default();
        let reference = ColumnReference::Named {
            name: authored.name.clone(),
            qualifier: authored.qualifier.clone(),
        };
        match self
            .lexical
            .address(reference, false, &mut witness, &self.core.identities)?
        {
            UnificationResult::Resolved(occurrence) => Ok(occurrence),
            UnificationResult::Unresolved(column) => Err(DelightQLError::column_not_found_error(
                column,
                "as the nest a structural step opens",
            )),
            UnificationResult::Ambiguous { column, tables } => {
                Err(DelightQLError::validation_error_categorized(
                    "resolution/ambiguous",
                    format!(
                        "Column '{column}' is ambiguous as a nest. Could refer to: {}",
                        tables.join(", ")
                    ),
                    "the nest a structural step opens",
                ))
            }
            UnificationResult::Opaque => Err(super::opaque_reference_refusal()),
            UnificationResult::Refused(refusal) => Err(refusal.into_error()),
        }
    }

    pub(super) fn resolve_dml_call(
        &mut self,
        call: ast_unresolved::SealedCall,
        access: Option<ast_unresolved::Access>,
    ) -> Result<ResolvedRelation> {
        let effect = call.is_effect();
        let (call, source) = split_dml_source(call)?;
        let call = call.into_inner();
        let reference = Some(&call.call().callee).ok_or_else(|| {
            DelightQLError::parse_error("a DML call has no written operation identity")
        })?;
        let operation = reference.name_text();
        let verb = match crate::pipeline::asts::effects::descriptor_for_reference(reference)
            .map(|descriptor| descriptor.category)
            .unwrap_or(crate::pipeline::asts::effects::DirectiveCategory::User)
        {
            crate::pipeline::asts::effects::DirectiveCategory::Dml(verb) => verb,
            _ => unreachable!("non-DML call reached DML resolver"),
        };

        // THE SHAPES between the source read and the terminal — every
        // trailing run step of the source chain, outermost first, so an
        // ordering or a reshaping before a mutation is judged whatever
        // carrier it rides.
        let contract_target = call
            .call()
            .relations()
            .next()
            .and_then(extract_unresolved_base_ground_name)
            .unwrap_or_else(|| "target".to_string());
        let dml_pipe_ops = classify_dml_source_shapes(&source);

        // The source resolves as the ordinary relational chain it is; its
        // own trailing run resolves on the shared run road.
        let resolved_source = self.resolve_relational(source)?;
        if let Some(first_unresolved) = resolved_source.owes().first() {
            let qual_str = match first_unresolved {
                ColumnReference::Named {
                    name, qualifier, ..
                } => qualifier
                    .as_ref()
                    .map(|q| format!("{}.{}", q, name))
                    .unwrap_or_else(|| name.to_string()),
                ColumnReference::Ordinal {
                    position, reverse, ..
                } => column_ordinal_text(*position, *reverse),
            };
            return Err(DelightQLError::ColumnNotFoundError {
                column: qual_str,
                context: "Column reference before pipe operator cannot be resolved (scope barrier)"
                    .to_string(),
            });
        }
        let available_columns = crate::relation::published_ports(
            &self.core.identities,
            &resolved_source.semantic_relation(),
        )?;

        // THE MUTATION CONTRACT, in the one place it is enforced: the `!!`
        // evidence is read off the relation the mutation is about to receive.
        let marks = self
            .core
            .identities
            .authority()
            .mutation_marks(&resolved_source.semantic_relation())?;
        self.enforce_mutation_contract(
            verb,
            &contract_target,
            &marks,
            &dml_pipe_ops,
            &available_columns,
        )?;
        let callable_name = self.core.identities.intern(&operation, false);
        let callable_namespace = reference
            .namespace_texts()
            .into_iter()
            .map(|part| self.core.identities.intern(&part, false))
            .collect();
        // DML classification is minted once into the registry and carried by
        // the callable identity through resolution and lowering. It is applied
        // to the RESOLVED call below: a classification written onto the
        // authored call would be a resolution decision sitting in a tree that
        // has not been resolved.
        let dml_callee = self.core.identities.mint_callable(
            callable_name,
            callable_namespace,
            crate::names::CallableCategory::Dml(verb),
        );
        // THE TARGET IS THE FIRST RELATION FORMAL: the descriptor's layout
        // for every mutation verb puts the destination first and the source
        // after it, so the position answers what the deleted role mark used
        // to say.
        let target_relation = call
            .call()
            .relations()
            .next()
            .cloned()
            .ok_or_else(|| DelightQLError::parse_error("DML call has no target relation"))?;
        let bare = operation.strip_suffix('!').unwrap_or(operation.as_ref());
        let (target, target_namespace) = crate::pipeline::asts::effects::target_designator(
            bare,
            "effect/dml/target_designator",
            "naming where to write",
            &target_relation,
        )?;

        if let Some(system) = self.core.database.system {
            let scope = self.env.reach().root_fq().to_string();
            if let Some((owner, kind)) =
                system.effect_target_owner(&target, target_namespace.as_deref(), &scope)?
            {
                if kind == "system" {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/target/engine_owned",
                        format!(
                            "DML target '{target}' resolves into the engine-owned namespace \
                             '{owner}': programs cannot mutate system relations — query it, \
                             never write it"
                        ),
                        "engine-owned namespace",
                    ));
                }
            }
        }

        let (target_schema, canonical, backend_schema) = if let Some(namespace) = target_namespace {
            let path =
                ast_unresolved::NamespacePath::from_fq_string(&namespace).map_err(|error| {
                    DelightQLError::validation_error(
                        format!("Invalid DML target namespace: {error}"),
                        "Use a valid namespace path",
                    )
                })?;
            use crate::defuse::environment::RelationAnswer;
            use crate::resolution::EntityDefinition;
            // The one lookup authority owns the qualified target answer —
            // a lexical refusal and a true provider miss arrive as distinct
            // closed outcomes, and this position only judges them.
            let (answer, _serve) = self.env.relation_qualified(
                self.core,
                &path,
                &delightql_types::SqlIdentifier::new(target.clone()),
                false,
            )?;
            let info = match answer {
                RelationAnswer::DatabaseEntity(info)
                | RelationAnswer::MaterializedRelation(info) => info,
                RelationAnswer::DataHole { name, world } => {
                    return Err(crate::defuse::environment::lookup::unbound_data_hole(
                        &name, &world,
                    ))
                }
                RelationAnswer::Ambiguous(message) => {
                    return Err(DelightQLError::validation_error(
                        message,
                        "Ambiguous DML target resolution",
                    ))
                }
                _ => {
                    return Err(DelightQLError::TableNotFoundError {
                        table_name: target,
                        context: "DML target was not found in its namespace".to_string(),
                    });
                }
            };
            let EntityDefinition::RelationSchema(schema) = info.definition;
            (schema, info.canonical_name, info.backend_schema)
        } else {
            use crate::defuse::environment::RelationAnswer;
            use crate::resolution::EntityDefinition;
            let resolved = self.env.relation(
                self.core,
                &delightql_types::SqlIdentifier::new(target.clone()),
                None,
            )?;
            let info = match resolved {
                RelationAnswer::DatabaseEntity(info)
                | RelationAnswer::MaterializedRelation(info) => info,
                // A FREE DATA NAME of a declaration whose world no ground!
                // bound: the grounding teaching answers, not a claim that
                // the table is missing — the caller's session may well hold
                // one, and that is exactly what a body cannot reach.
                RelationAnswer::DataHole { name, world } => {
                    return Err(crate::defuse::environment::lookup::unbound_data_hole(
                        &name, &world,
                    ))
                }
                _ => {
                    return Err(DelightQLError::validation_error(
                        format!("DML target '{target}' is not a physical table"),
                        "DML targets must resolve to database tables",
                    ))
                }
            };
            let EntityDefinition::RelationSchema(schema) = info.definition;
            (schema, info.canonical_name, info.backend_schema)
        };
        let target_scope = target_schema;
        let target_spelling = self.core.identities.intern(&target, false);
        let target_entity = self
            .core
            .identities
            .authority()
            .entity(&target_scope)?
            .unwrap_or_else(|| self.core.identities.mint_entity(target_spelling));
        let canonical = canonical.as_ref().map(|name| {
            self.core
                .identities
                .intern(name.as_str(), name.is_stropped())
        });
        let backend_schema = backend_schema
            .as_deref()
            .map(|name| self.core.identities.intern(name, false));
        self.core
            .identities
            .bind_entity_physical(target_entity, canonical, backend_schema);

        // THE CALL'S ARGUMENTS RESOLVE OVER THE SOURCE ROW it is about to
        // consume: the source is the frame, entered here and left before
        // the terminal is assembled over it.
        self.lexical.enter(resolved_source, super::Reach::Stage);
        let resolved_call = self.resolve_functor_call(call);
        let resolved_source = self.lexical.leave();
        let mut resolved_call = resolved_call?;
        resolved_call.callee = dml_callee;
        let target = self.core.identities.authority().ground_read(
            ast_resolved::Access::All,
            false,
            target_scope,
        )?;
        resolved_call
            .call_mut()
            .arguments
            .replace_first_relation(target.clone());
        // The source rides the call in its own formal position — after the
        // target, per the descriptor's layout — so the lowering reads
        // [target, source] off the one call.
        let source_relation = resolved_source.semantic_relation();
        insert_dml_source_argument(&mut resolved_call, resolved_source.into_body());

        // A mutation's source is a plan-lifetime relation. Its semantic
        // storage identity and complete interface are fixed here, before SQL
        // lowering decides the temporary table's physical slots.
        let terminal_scope =
            self.core
                .identities
                .authority()
                .derive(crate::relation::RelForm::Scratch(
                    crate::relation::form::ScratchSpec::holding(
                        crate::relation::form::ScratchWhy::DmlSource,
                        None,
                        &source_relation,
                    ),
                ))?;
        let terminal_columns =
            crate::relation::published_ports(&self.core.identities, &terminal_scope)?;

        let mut resolved_chain =
            ast_resolved::Chain::ground(self.core.identities.authority().reading(
                crate::relation::builder::ReadHead::Call {
                    call: ast_resolved::SealedCall::from_inner(resolved_call, effect),
                    alias: (),
                    published: terminal_scope,
                },
            )?);

        // THE RECEIPT IS THE MUTATION'S OWN: the access on what the mutation
        // publishes, standing beside the call exactly as it was written.
        if let Some(access) = access {
            let (staged, _access_output) = super::resolving::operators::schema_ops::resolve_access(
                super::relation_resolver::resolve_schema_free_access(&access)?,
                &terminal_columns,
                terminal_scope,
                &self.core.identities,
            )?;
            resolved_chain = self
                .core
                .identities
                .authority()
                .reland(resolved_chain, staged)?;
        }

        Ok(ResolvedRelation::answering_for_itself(resolved_chain))
    }

    /// Relation dispatch.
    /// Matches on the Relation variant and delegates to the appropriate helper in
    /// `relation_resolver`. The helpers remain as free functions; only the
    /// dispatch is absorbed so `self.core` / `self.config` are threaded
    /// implicitly.
    #[stacksafe::stacksafe]
    fn resolve_anon_table_impl(
        &mut self,
        anon: ast_unresolved::AnonRelation,
    ) -> Result<ResolvedRelation> {
        super::relation_resolver::resolve_anonymous(anon, self)
    }

    /// Resolve the chain's READ: the head relation, and the access its own
    /// parens asked for.
    ///
    /// The two arrive together because a mention's published heading is what
    /// the access decides — the caller pattern's binders, the dequalifying
    /// merge, the activation of an inchoate source. Handing the relation on
    /// without it would leave a read nobody parameterized.
    fn resolve_relation_impl(
        &mut self,
        rel: ast_unresolved::Relation,
        access: Option<ast_unresolved::Access>,
    ) -> Result<ResolvedRelation> {
        match &rel {
            // Which road a ground read takes is the MENTION's question: a
            // plan read is already addressed, so no spelling lookup runs.
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Scratch { .. },
                ..
            } => super::relation_resolver::resolve_scratch_read(rel, read_access(access)?, self),
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Receipt { .. },
                ..
            } => super::relation_resolver::resolve_receipt_read(rel, read_access(access)?, self),
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Structural { .. },
                ..
            } => {
                super::relation_resolver::resolve_structural_scope(rel, read_access(access)?, self)
            }
            ast_unresolved::Relation::Ground { .. } => {
                super::relation_resolver::resolve_ground(rel, read_access(access)?, self)
            }
            ast_unresolved::Relation::FunctorCall { call, alias, .. } => {
                // The builder has already substituted a piped relation into
                // the call's table arguments. Resolve that call in place:
                // application provenance is diagnostic data only, so no
                // placeholder or synthetic application pipe is rebuilt here.
                let is_dml = Some(&call.call().callee).is_some_and(|reference| {
                    matches!(
                        crate::pipeline::asts::effects::descriptor_for_reference(reference)
                            .map(|descriptor| descriptor.category)
                            .unwrap_or(crate::pipeline::asts::effects::DirectiveCategory::User),
                        crate::pipeline::asts::effects::DirectiveCategory::Dml(_)
                    )
                });
                if is_dml && call.call().relations().nth(1).is_some() {
                    // ONE INVOCATION SHAPE. A mutation is the relation-
                    // position call the builder and the effect machinery
                    // both construct — [target, source] in the descriptor's
                    // layout — and it resolves IN PLACE: the mutation
                    // contract has one place to be enforced and the lowerer
                    // one shape to consume. A second road is what let a
                    // source reach SQL with its `!!` evidence and its shape
                    // unexamined.
                    //
                    // THE RECEIPT IS A RELATION: a name authored on the
                    // call names the receipt it produced, exactly as a name
                    // on any other landed result does.
                    let receipt = self.resolve_dml_call(call.clone(), access)?;
                    return match alias {
                        Some(alias) => {
                            let spelling = self
                                .core
                                .identities
                                .intern(alias.as_str(), alias.is_stropped());
                            receipt.aliased(spelling, &self.core.identities)
                        }
                        None => Ok(receipt),
                    };
                }
                let mut caller_row = std::mem::replace(
                    &mut self.ho_caller_row,
                    crate::pipeline::resolver::CallerRow::Absent,
                );
                // A caller-authored argument correlates by the caller's
                // scope names: the fold's own position holds them, and this
                // resolution may be standing inside a scalar subquery whose
                // enclosing position holds more.
                let outcome = super::relation_resolver::resolve_functor_call(
                    call.clone().into_inner(),
                    alias.clone(),
                    read_access(access)?,
                    self,
                    &mut caller_row,
                );
                self.ho_caller_row = caller_row;
                let outcome = outcome?;
                // A LANDED CALL IS A PIPE FORM: the outcome answers that
                // itself and crosses if it must, so this road never holds
                // the call's relation beside a scope the crossing did not
                // derive.
                let standing = outcome.crossed_if_landed(&self.core.identities)?;
                if !standing.head_reads_a_call() {
                    // Higher-order expansion consumes the call carrier and
                    // returns the expanded relation directly. It is still
                    // one invocation road; only an ordinary unresolved TVF
                    // remains a callable relation after resolution.
                    return Ok(standing);
                }
                // The access stands where the call road put it: after the
                // call, on what the call publishes. The head travels WHOLE:
                // there is no rebuilding it from parts, so nothing here can
                // pair the call with another relation — and the rebuild
                // republishes nothing, so what answers over the relation
                // travels through with it rather than being chosen again.
                let standing = standing.republished(|chain| {
                    let (head, resolved_access, steps) = chain.split_head_access();
                    let mut rebuilt = ast_resolved::Chain::ground(head);
                    if let Some(access) = resolved_access {
                        rebuilt = self
                            .core
                            .identities
                            .authority()
                            .read_asking(rebuilt, access)?;
                    }
                    self.core.identities.authority().reland_all(rebuilt, steps)
                })?;
                Ok(standing)
            }
            ast_unresolved::Relation::InnerRelation { .. } => {
                super::relation_resolver::resolve_inner_relation(rel, self)
            }
            // Matched on a REFERENCE: rustc's uninhabited-variant elision is
            // by value only, so the consulted form still needs an arm even
            // though `Unresolved::Consulted` cannot be built.
            ast_unresolved::Relation::ConsultedView { .. } => {
                unreachable!("resolved relation reached relation resolution")
            }
        }
    }

    pub(super) fn resolve_functor_call(
        &mut self,
        call: ast_unresolved::FunctorCall,
    ) -> Result<ast_resolved::FunctorCall> {
        // A curried callee's leading positions take CODE: what stands
        // there is handed to the formal, not invoked here.
        let code_positions =
            crate::defuse::callable::curried_code_positions(&call.callee, self.core, self.env);
        self.resolve_call_row_wise(call, code_positions)
    }

    /// A CLOSED TARGET CALLABLE'S CALL at its invocation site: the callee
    /// was judged and closed in the caller, so no definition of THIS
    /// world may read its spelling — no catalog probe decides a code
    /// position here, and none exists.
    pub(crate) fn resolve_target_call(
        &mut self,
        call: ast_unresolved::FunctorCall,
    ) -> Result<ast_resolved::FunctorCall> {
        self.resolve_call_row_wise(call, 0)
    }

    fn resolve_call_row_wise(
        &mut self,
        call: ast_unresolved::FunctorCall,
        code_positions: usize,
    ) -> Result<ast_resolved::FunctorCall> {
        // A CALL'S ARGUMENTS ARE ROW-WISE POSITIONS whatever grade the
        // call itself stands in: an absorber's interior is per-row, and a
        // reducing obligation never distributes INTO an argument row.
        let prior_grade = std::mem::replace(
            &mut self.position_grade,
            crate::defuse::bound_use::CallableGrade::RowWise,
        );
        let outcome = self.resolve_call_members(call, code_positions);
        self.position_grade = prior_grade;
        outcome
    }

    fn resolve_call_members(
        &mut self,
        call: ast_unresolved::FunctorCall,
        code_positions: usize,
    ) -> Result<ast_resolved::FunctorCall> {
        let is_cast = call.callee.namespace_texts().is_empty() && call.callee.name_text() == "cast";
        let expected_cast_type = |type_name: &str| {
            const CAST_TYPES: &[&str] = &["integer", "real", "text", "numeric", "boolean"];
            if CAST_TYPES.contains(&type_name) {
                Ok(())
            } else {
                Err(DelightQLError::validation_error_categorized(
                    "cast",
                    format!(
                        "cast: unknown type '{}'. Types: {} (date/timestamp and parameterized types are not yet supported)",
                        type_name,
                        CAST_TYPES.join("|")
                    ),
                    "cast resolution",
                ))
            }
        };
        let callee = call.callee.written_call_identity(&self.core.identities);
        use crate::pipeline::asts::core::operators::{CallArguments, HoArgument, ScalarArgument};
        if is_cast && call.arguments.scalar_members().len() != 2 {
            return Err(DelightQLError::validation_error_categorized(
                "cast",
                format!(
                    "cast: expects exactly 2 arguments: cast:(expr, type), got {}",
                    call.arguments.scalar_members().len()
                ),
                "cast resolution",
            ));
        }
        let arguments = match call.arguments {
            CallArguments::None => CallArguments::None,
            CallArguments::HigherOrder(part) => {
                CallArguments::HigherOrder(crate::pipeline::asts::core::operators::HoPart::of(
                    part.into_members().try_map(|argument| {
                        Ok::<_, DelightQLError>(match argument {
                            HoArgument::Relation(relation) => {
                                HoArgument::Relation(self.transform_relational(relation)?)
                            }
                            HoArgument::Rule(rule) => {
                                HoArgument::Rule(self.transform_relational(rule)?)
                            }
                            // The kind travels with the member: a landed
                            // relation crosses the phase landed.
                            HoArgument::Landed(relation) => {
                                HoArgument::Landed(self.transform_relational(relation)?)
                            }
                            HoArgument::Value(value) => HoArgument::Value(
                                crate::pipeline::ast_transform::transform_argument_value(
                                    self, value,
                                )?,
                            ),
                            // A landing reaching this fold is one no pipe
                            // applied an invocation to; the fold refuses it.
                            HoArgument::Landing(landing) => {
                                HoArgument::Landing(self.fold_placeholder(landing)?)
                            }
                            HoArgument::Skip => HoArgument::Skip,
                        })
                    })?,
                ))
            }
            CallArguments::Scalar(members) => {
                let mut resolved_members = Vec::with_capacity(members.len());
                for (index, member) in members.into_iter().enumerate() {
                    // A MEMBER STANDING IN A CODE POSITION IS A MENTION,
                    // however spelled: `upper:()` written as a value there
                    // is the callable the position declares, resolved as
                    // the form it is and never invoked here.
                    let member = if index < code_positions {
                        match member {
                            ScalarArgument::Value(ast_unresolved::ArgumentValue {
                                value:
                                    ast_unresolved::DomainExpression::Application(
                                        ast_unresolved::FunctionApplication::Standard(mention),
                                    ),
                                ..
                            }) => ScalarArgument::Callable(
                                crate::pipeline::asts::core::Callable::Functor(mention),
                            ),
                            other => other,
                        }
                    } else {
                        member
                    };
                    let resolved = match member {
                        // The star is the RESOLVED reading of a bare glob; an
                        // authored argument row has none to carry across.
                        ScalarArgument::Star => {
                            return Err(DelightQLError::transformation_error(
                                "a whole-operand star reached resolution already resolved: the \
                                 bare glob it reads is spent here, not before",
                                "ho_argument",
                            ))
                        }
                        ScalarArgument::Value(ast_unresolved::ArgumentValue {
                            distinct,
                            value: domain,
                        }) => {
                            if is_cast && index == 1 {
                                let type_name = match &domain {
                                    ast_unresolved::DomainExpression::Application(ast_unresolved::FunctionApplication::Ground(crate::pipeline::asts::core::literals::LiteralValue::Symbol(
                                            symbol,
                                        ))) => symbol.clone(),
                                    ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
                                        qualifier: None, ..
                                    }))) => {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "cast",
                                            "cast: a bare name is use; the type is a tag — write cast:(x, ::integer). Types: integer|real|text|numeric|boolean",
                                            "cast resolution",
                                        ));
                                    }
                                    ast_unresolved::DomainExpression::Application(ast_unresolved::FunctionApplication::Ground(_)) => {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "cast",
                                            "cast: takes a type symbol, not a string — write cast:(x, ::integer). Types: integer|real|text|numeric|boolean",
                                            "cast resolution",
                                        ));
                                    }
                                    _ => {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "cast",
                                            "cast: second argument must be a type symbol. Types: integer|real|text|numeric|boolean",
                                            "cast resolution",
                                        ));
                                    }
                                };
                                expected_cast_type(&type_name)?;
                                ScalarArgument::plain(ast_resolved::DomainExpression::Application(
                                    ast_resolved::FunctionApplication::Ground(
                                        crate::pipeline::asts::core::literals::LiteralValue::String(
                                            type_name,
                                        ),
                                    ),
                                ))
                            } else {
                                ScalarArgument::Value(ast_resolved::ArgumentValue {
                                    distinct,
                                    value: self.transform_domain(domain)?,
                                })
                            }
                        }
                        // A CALLABLE RESOLVES AS THE FORM IT IS. Its slot is the
                        // callee's to supply, so resolution reaches its body and
                        // leaves the slot standing.
                        ScalarArgument::Callable(callable) => {
                            ScalarArgument::Callable(self.transform_callable(callable)?)
                        }
                        // AN ENUMERATION IS SPENT WHERE ITS CONTAINER RESOLVES IT,
                        // and an argument row is such a container: FN.35 lists it
                        // among the enumerating positions, and an addressing spread
                        // expands there into the columns it addresses — the
                        // ENCLOSING relation's, which is the same heading every
                        // other container in this fold reads.
                        //
                        // The bare glob is the one that does not address. It NAMES
                        // the whole of what the position offers — the mark `t(*)`
                        // writes — so it has no several columns to become, and
                        // resolves to the star instead.
                        ScalarArgument::Spread(crate::pipeline::asts::core::Spread::Glob(
                            crate::pipeline::asts::core::Glob {
                                qualifier: None, ..
                            },
                        )) => ScalarArgument::Star,
                        ScalarArgument::Spread(spread) => {
                            let available = self.lexical.local_ports(&self.core.identities)?;
                            // Width and arity are judged AFTER the position
                            // expands — by the callee's own authority — so nothing
                            // here refuses on a count. What can refuse is the
                            // addressing itself: a qualifier naming no live scope,
                            // a pattern matching nothing, a span outside the
                            // heading, each at the authority that owns it.
                            let expanded =
                                super::resolving::domain_expressions::projection::expand_spread(
                                    self, &spread, &available, false,
                                )?;
                            for value in expanded {
                                resolved_members.push(ScalarArgument::plain(value));
                            }
                            continue;
                        }
                        // A `..` reaching this fold selects the context mode
                        // of a callee that instantiates none; the fold
                        // refuses it.
                        ScalarArgument::Context(marker) => {
                            ScalarArgument::Context(self.fold_context_marker(marker)?)
                        }
                    };
                    resolved_members.push(resolved);
                }
                CallArguments::Scalar(resolved_members)
            }
        };
        Ok(ast_resolved::FunctorCall {
            callee,
            arguments,
            marks: call.marks,
        })
    }

    /// THE WINDOW RIDES THE WINDOW FUNCTION, both directions, judged from the
    /// FINAL argument row.
    ///
    /// A window builtin computes over a window, so one standing bare has
    /// nothing to compute over; a scalar function computes per row and takes
    /// no window. Which function a name means can depend on the row —
    /// `max:(x)` is the aggregate and `max:(x, 2)` is the scalar overload —
    /// and the overload is read from the one authority the lowering picks its
    /// render form from, so the two cannot disagree.
    ///
    /// An unknown callee passes through unmapped: engine semantics apply.
    /// THE TARGET-PROVIDER LAW for a callee no definition road answered.
    /// A QUALIFIED callee names DelightQL's own world, so a miss refuses
    /// instead of falling through to the open target provider; the reserved
    /// virtual provider `sys::target` explicitly selects the target. An
    /// unqualified name the engine does not know is judged against the
    /// complete enlisted DQL candidate set first: a wrong-kind family must
    /// not silently become a target call. Reached by the ordinary road at
    /// its last arm, and by the caller closing a target code actual — the
    /// same judgment, made where the callee is closed.
    pub(crate) fn judge_target_callee(
        &self,
        callee: &crate::pipeline::asts::vocabulary::Ref,
    ) -> Result<()> {
        if let Some(fq) = callee.namespace_fq() {
            if fq != "sys::target" {
                return Err(DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::RESOLUTION_CALLABLE_UNKNOWN,
                    format!(
                        "no DQL callable '{}' exists in namespace '{}'. A \
                         qualified name states where the callable lives in \
                         DelightQL's world, so a miss refuses; to call the \
                         target engine's own function write \
                         sys::target.{}:(…)",
                        callee.name_text(),
                        fq,
                        callee.name_text(),
                    ),
                    "unknown qualified callable",
                ));
            }
        } else if !self.core.built_in.is_known_function(&callee.name_text()) {
            // ONLY A TRUE DQL MISS REACHES THE OPEN TARGET PROVIDER. A
            // capable definition reaching this judgment means the call
            // carries a window the value-definition road does not serve —
            // a consulted body computes per row and takes no window (its
            // grade is the position's; a window disagrees).
            let name_ident = callee.name_identifier();
            if let crate::defuse::bound_use::CallablePresence::WrongKind(provenance) =
                crate::defuse::bound_use::callable_presence(self.core, self.env, &name_ident)?
            {
                return Err(DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::RESOLUTION_CALLABLE_UNKNOWN,
                    format!(
                        "no DQL callable '{}' exists, but the name is \
                         taken in DelightQL's world ({provenance}). A \
                         defined name never falls through to the target \
                         engine; to call the engine's own function write \
                         sys::target.{}:(…)",
                        callee.name_text(),
                        callee.name_text(),
                    ),
                    "a taken name is not an unknown callable",
                ));
            }
        }
        Ok(())
    }

    fn judge_window(
        &self,
        name: &str,
        windowed: bool,
        call: &ast_resolved::FunctorCall,
    ) -> Result<()> {
        let builtin = &self.core.built_in;
        if !windowed {
            if builtin.window_signature(name).is_some() {
                return Err(DelightQLError::validation_error_categorized(
                    "window/needs_window",
                    format!(
                        "'{name}' is a window function and computes over a window; \
                         standing bare it has nothing to compute over"
                    ),
                    format!("write the spec inside the call's parens: `{name}:(… <~ #(…))`"),
                ));
            }
            return Ok(());
        }
        // THE FINAL ROW: what the call actually hands the function. The
        // window builtins' argument bounds read it too — one row, one
        // reading, for both questions.
        let supplied = call.arguments.scalar_members().len();
        super::grounding::judge_window_row(self, name, supplied)?;
        if builtin.window_signature(name).is_some() {
            return Ok(());
        }
        let scalar_overload = crate::names::Intrinsic::scalar_overload(name, supplied).is_some();
        let aggregates = builtin.is_aggregate(name) && !scalar_overload;
        if !aggregates && (builtin.is_known_function(name) || scalar_overload) {
            return Err(DelightQLError::validation_error_categorized(
                "window/not_a_window",
                format!(
                    "the window rides the window function itself, and '{name}' is a \
                     scalar function — it computes per row and takes no window"
                ),
                format!(
                    "spell the windowed call inside the argument: \
                     `{name}:(lag:(x <~ #(…)), …)`"
                ),
            ));
        }
        Ok(())
    }

    /// The application, resolved whole: the call plus the scalar context the
    /// value position gave it.
    ///
    /// THE WINDOW IS JUDGED HERE because this is where the FINAL argument row
    /// exists. An authored row is not that row — one addressing spread is a
    /// single authored member and several resolved arguments — so a judgment
    /// made before this point reads a width the call does not have. Every
    /// resolved standard application is built here, authored or rebuilt, so
    /// the judgment cannot be reached around.
    pub(super) fn resolve_standard_application(
        &mut self,
        application: ast_unresolved::StandardApplication,
    ) -> Result<ast_resolved::StandardApplication> {
        let windowed = application.window.is_some();
        let callee = application.call().callee.clone();
        let call = self.resolve_functor_call(application.call.into_inner())?;
        let window = application
            .window
            .map(|window| self.resolve_window_spec(window))
            .transpose()?;
        let guard = application
            .guard
            .map(|condition| self.transform_boolean(*condition).map(Box::new))
            .transpose()?;
        self.finish_application(
            &callee.name_text(),
            callee.namespace_fq().as_deref(),
            windowed,
            call,
            window,
            guard,
        )
    }

    /// THE ONE WINDOW-SIGNATURE AUTHORITY over a resolved application —
    /// reached by every authored application, and by a closed TARGET code
    /// actual's invocation, whose callee was closed in the caller and whose
    /// arguments resolved at the site. Every resolved standard application
    /// is built here, so the judgment cannot be reached around.
    pub(crate) fn finish_application(
        &mut self,
        name: &str,
        namespace_fq: Option<&str>,
        windowed: bool,
        call: ast_resolved::FunctorCall,
        mut window: Option<ast_resolved::WindowSpec>,
        guard: Option<Box<ast_resolved::TruthExpression>>,
    ) -> Result<ast_resolved::StandardApplication> {
        // JUDGED LAST, so every refusal the application's own parts can make
        // is made first: an argument's, the window's own expressions', the
        // guard's. A qualified callee is not a built-in.
        if namespace_fq.is_none() {
            self.judge_window(name, windowed, &call)?;
        }
        // A WINDOWED WRAPPER USE'S OBLIGATION lands here: while a windowed
        // use of a consulted value definition resolves its body, the FIRST
        // reducing absorber built — an engine aggregate or an unknown
        // target callable (sys::target included) — takes the caller's
        // resolved spec. This is the grade flowing INWARD: the wrapper is
        // grade-polymorphic per use, and the window rides the function
        // that can carry it.
        if window.is_none() && self.window_obligation.is_some() {
            let absorber = match namespace_fq {
                Some(fq) => fq == "sys::target",
                None => {
                    let supplied = call.arguments.scalar_members().len();
                    let scalar_overload =
                        crate::names::Intrinsic::scalar_overload(name, supplied).is_some();
                    let builtin = &self.core.built_in;
                    (builtin.is_aggregate(name) && !scalar_overload)
                        || !builtin.is_known_function(name)
                }
            };
            if absorber {
                let obligation = self
                    .window_obligation
                    .as_mut()
                    .expect("presence checked above");
                if obligation.taken {
                    obligation.extra = true;
                } else {
                    obligation.taken = true;
                    window = Some(obligation.spec.clone());
                }
            }
        }
        Ok(ast_resolved::StandardApplication {
            call: crate::pipeline::asts::core::PureCall::from_inner(call),
            guard,
            window,
        })
    }

    /// Resolve one authored window spec in the CURRENT scope — the caller's
    /// scope for a windowed call's own spec, and for a windowed wrapper
    /// use, whose spec resolves before the body opens.
    pub(crate) fn resolve_window_spec(
        &mut self,
        window: crate::pipeline::asts::core::WindowSpec<crate::pipeline::asts::core::Unresolved>,
    ) -> Result<ast_resolved::WindowSpec> {
        Ok(ast_resolved::WindowSpec {
            partition: window
                .partition
                .into_iter()
                .map(|expr| self.transform_domain(expr))
                .collect::<Result<Vec<_>>>()?,
            ordering: window
                .ordering
                .into_iter()
                .map(|spec| {
                    Ok(ast_resolved::OrderingSpec {
                        column: self.transform_domain(spec.column)?,
                        direction: spec.direction,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            frame: window
                .frame
                .map(|frame| {
                    super::resolving::functions::resolve_window_frame_via_fold(self, frame)
                })
                .transpose()?,
        })
    }
}

impl<'reg, 'db> AstTransform<Unresolved, Resolved> for ResolverFold<'reg, 'db> {
    crate::pipeline::ast_transform::position_is_resolved_against_a_heading!();
    fn fold_entity(
        &mut self,
        entity: crate::pipeline::asts::vocabulary::Ref,
    ) -> crate::error::Result<crate::names::CallableId> {
        Ok(entity.written_call_identity(&self.core.identities))
    }
    crate::pipeline::ast_transform::column_is_bound_where_it_is_resolved!();
    crate::pipeline::ast_transform::binder_is_bound_where_the_pattern_is_resolved!();
    crate::pipeline::ast_transform::a_landing_is_consumed_where_the_pipe_is_applied!();
    crate::pipeline::ast_transform::a_context_marker_is_consumed_where_the_call_instantiates!();
    crate::pipeline::ast_transform::scope_is_minted_where_it_is_resolved!();
    crate::pipeline::ast_transform::minted_where_it_is_decided!(
        fold_output -> crate::relation::PortId: "an expression's output port",
        fold_scalar_output -> crate::relation::PortId: "a scalarized relation's column",
        fold_destructure -> Vec<crate::pipeline::asts::core::DestructureMapping>: "a destructuring pattern's columns",
    );
    fn fold_open_leaf(
        &mut self,
        _: crate::pipeline::asts::core::DomainHole,
    ) -> crate::error::Result<crate::pipeline::asts::core::FormalHole> {
        Err(crate::error::DelightQLError::validation_error_categorized(
            "value/open/unapplied",
            "a composition input stands outside any callable applying it",
            "the position that applies an open body spends its slot",
        ))
    }

    fn fold_cover_callable(
        &mut self,
        _: crate::pipeline::asts::core::Callable<crate::pipeline::asts::core::Unresolved>,
    ) -> crate::error::Result<()> {
        Err(crate::error::DelightQLError::transformation_error(
            "a cover's callable is applied where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    fn fold_rename_target(
        &mut self,
        _: crate::pipeline::asts::core::NameTarget,
    ) -> crate::error::Result<crate::names::Spelling> {
        Err(crate::error::DelightQLError::transformation_error(
            "a rename target is expanded where the rename resolves, and this fold is not that place",
            "phase_payload",
        ))
    }
    fn fold_drill(
        &mut self,
        _: crate::pipeline::asts::core::operators::AuthoredDrill,
    ) -> crate::error::Result<crate::pipeline::asts::core::operators::BoundDrill> {
        Err(crate::error::DelightQLError::transformation_error(
            "an interior drill binds where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    fn transform_relational(&mut self, e: Chain<Unresolved>) -> Result<Chain<Resolved>> {
        // THE WALK TAKES THE BODY. A generic transform answers with a
        // chain and has nowhere to put a lexical scope, so the scope ends
        // here rather than travelling in a sidecar the walk could pair
        // with some other chain. A caller that needs it uses the
        // resolver's own door, `resolve_relational`.
        Ok(self.resolve_relational_impl(e)?.into_body())
    }

    /// Stack-safe: this is the walk that a nested expression descends once
    /// per level, and it is the one that overflowed. Its relational sibling
    /// and the anonymous-table road already carry the same guard; a
    /// parenthesis ladder reaches none of them and reached this.
    #[stacksafe::stacksafe]
    fn transform_domain(
        &mut self,
        expr: ast_unresolved::DomainExpression,
    ) -> Result<ast_resolved::DomainExpression> {
        use crate::pipeline::ast_transform::walk_transform_domain;

        // THE APPLYING POSITION SPENDS THE LEAF. While a cover applies its
        // body for one cell, the leaf becomes that cell — here, before any
        // resolved tree is minted — so a closed phase never carries one.
        // Outside an applying position the leaf refuses instead.
        if let ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Open(hole),
        ) = &expr
        {
            return match (hole, &self.cover_cell) {
                (crate::pipeline::asts::core::DomainHole::CompositionInput, Some(cell)) => {
                    Ok(cell.clone())
                }
                _ => Err(crate::error::DelightQLError::validation_error_categorized(
                    "value/open/unapplied",
                    "a composition input stands outside any callable applying it",
                    "the position that applies an open body spends its slot",
                )),
            };
        }

        // A FORMAL IS SPENT, NOT LOOKED UP: inside an open instantiation, a
        // bare name that is one of the definition's formals stands for the
        // argument as the CALLER resolved it. No scope is consulted, so a
        // probe or relation the body opens cannot capture the name.
        if let ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
            authored,
        ))) = &expr
        {
            if authored.qualifier.is_none() && authored.namespace_path.is_empty() {
                if let Some(resolved) = self.env.formal_value(&authored.name) {
                    return self.spend_formal(resolved);
                }
            }
        }

        match expr {
            // A CONSULTED VALUE DEFINITION IS SPENT AT ITS CALL SITE, before
            // ordinary closed resolution: the formals substitute and what
            // remains is closed code in the caller's position.
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Standard(application),
            ) => {
                if let Some(inlined) = crate::defuse::callable::inline_cfe_call(self, &application)?
                {
                    return Ok(inlined);
                }
                // The window judgment is `resolve_standard_application`'s,
                // where the final argument row exists; this arm only spends
                // the consulted-definition road before it.
                walk_transform_domain(
                    self,
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Standard(application),
                    ),
                )
            }
            // StringTemplate at domain level → concat chain (returns DomainExpression, not Function)
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Template(template),
            ) => {
                let mut resolved_parts: Vec<ast_resolved::ValueTemplatePart> = Vec::new();
                for part in template.into_parts() {
                    match part {
                        ast_unresolved::ValueTemplatePart::Text(text) => {
                            resolved_parts.push(ast_resolved::ValueTemplatePart::Text(text));
                        }
                        ast_unresolved::ValueTemplatePart::Interpolation(expr) => {
                            let resolved_expr = self.transform_domain(*expr)?;
                            resolved_parts.push(ast_resolved::ValueTemplatePart::Interpolation(
                                Box::new(resolved_expr),
                            ));
                        }
                    }
                }
                Ok(super::string_templates::build_concat_chain(resolved_parts))
            }

            // Simple expressions — column validation, ordinal resolution, literal conversion
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn { .. },
            )))
            | ast_unresolved::DomainExpression::Reference(Reference::Ordinal(_))
            | ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            )
            | ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Open(_),
            ) => super::resolving::domain_expressions::simple::resolve_simple_expr(
                expr,
                &self.lexical,
                self.in_correlation,
                &mut self.correlation_witness,
                &self.core.identities,
            ),

            // Everything else: walk handles structural descent
            // Function(non-StringTemplate) → transform_function
            // Predicate → transform_boolean
            // Nested value forms → recursive transform_domain
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        pred: ast_unresolved::TruthExpression,
    ) -> Result<ast_resolved::TruthExpression> {
        use crate::pipeline::ast_transform::walk_transform_boolean;

        match pred {
            // Literal membership stays a membership; lowering owns the
            // doctrine.
            ast_unresolved::TruthExpression::Membership(Membership {
                probe,
                rows,
                negated,
                source,
            }) => {
                if source == MembershipSource::WitnessAnon {
                    validate_witness_membership(&probe, &rows)?;
                }
                let probe = crate::pipeline::ast_transform::transform_probe(self, probe).map_err(
                    |error| {
                        if source == MembershipSource::WitnessAnon {
                            DelightQLError::validation_error_categorized(
                                "resolution/anon/witness_shape",
                                "a witness anonymous table (+_ or \\+_) is a membership test: every header must be a ground value or an lvar that unifies with a column in scope",
                                "a header that unifies with nothing would declare a fresh column, and a membership test has no columns to declare",
                            )
                        } else {
                            error
                        }
                    },
                )?;
                let rows = rows.try_map(|row| -> Result<_> {
                    Ok(ValueRow(row.0.try_map(|e| self.transform_domain(e))?))
                })?;
                Ok(ast_resolved::TruthExpression::Membership(Membership {
                    probe,
                    rows,
                    negated,
                    source,
                }))
            }

            // InRelational → fresh subquery resolution via registry
            ast_unresolved::TruthExpression::RelationalMembership(RelationalMembership {
                probe,
                relation: subquery,
                addressing: ProbeAddressing { identifier, .. },
                negated,
            }) => {
                let resolved_value = crate::pipeline::ast_transform::transform_probe(self, probe)?;
                let resolved_subquery = self.resolve_interior(*subquery)?.into_body();
                // Arity law: N tested expressions require exactly N produced
                // columns — a mismatch is a compile-time refusal, never a
                // backend "sub-select returns N columns" surprise.
                // The probe SAYS its width; nothing re-derives it from a
                // tuple value's shape.
                let left_arity = resolved_value.width();
                {
                    let scope = resolved_subquery.semantic_relation();
                    let right_arity =
                        crate::relation::published_ports(&self.core.identities, &scope)?.len();
                    if right_arity != left_arity {
                        return Err(crate::error::DelightQLError::validation_error_categorized(
                            "membership/arity",
                            format!(
                                "relational '{}' arity mismatch: the left side tests {} expression(s) but '{}' produces {} column(s)",
                                if negated { "not in" } else { "in" },
                                left_arity,
                                identifier.name,
                                right_arity
                            ),
                            "project the relation to the tested width, e.g. R(|> (col))",
                        ));
                    }
                }
                Ok(ast_resolved::TruthExpression::RelationalMembership(
                    RelationalMembership {
                        probe: resolved_value,
                        relation: Box::new(resolved_subquery),
                        negated,
                        addressing: (),
                    },
                ))
            }

            // InnerExists → fresh subquery resolution + USING correlation
            ast_unresolved::TruthExpression::Existence(Existence {
                polarity,
                relation: subquery,
                addressing:
                    ProbeAddressing {
                        identifier: _,
                        using_columns,
                    },
            }) => {
                let resolved_subquery = self.resolve_interior(*subquery)?.into_body();
                let row = self.lexical.ports_in_view(&self.core.identities)?;
                let final_subquery = super::resolving::predicates::synthesize_using_correlation(
                    resolved_subquery,
                    &using_columns,
                    &row,
                    &self.core.identities,
                )?;
                Ok(ast_resolved::TruthExpression::Existence(Existence {
                    polarity,
                    relation: Box::new(final_subquery),
                    addressing: (),
                }))
            }

            ast_unresolved::TruthExpression::Sigma(SigmaApplication {
                proof: crate::pipeline::asts::core::NamedProof::Call(call),
                polarity,
            }) => self.resolve_sigma_application(call, polarity),

            // Everything else: the walk handles structural descent.
            other => walk_transform_boolean(self, other),
        }
    }

    fn transform_function(
        &mut self,
        func: ast_unresolved::FunctionApplication,
    ) -> Result<ast_resolved::FunctionApplication> {
        use crate::pipeline::ast_transform::walk_transform_function;

        use super::resolving::domain_expressions::mode::{resolve_mode_call, Picked};

        match func {
            // A CALLEE THAT DECLARES A MODE ANSWERS THROUGH THE DECLARATION,
            // whatever its width and whichever spelling reached it. An
            // ordinary call comes straight back and takes the ordinary road.
            ast_unresolved::FunctionApplication::Standard(application) => {
                match resolve_mode_call(self, application.clone(), Picked::Whole)? {
                    Some(resolved) => Ok(resolved),
                    None => {
                        // A QUALIFIED CALLEE NAMES DELIGHTQL'S OWN WORLD.
                        // Every definition road — value definitions, and the
                        // declared-mode road just above — has had its
                        // chance, so a still-qualified callee here is a
                        // MISS, and a qualified miss refuses instead of
                        // falling through to the open target provider. Only
                        // an unqualified miss takes the default target
                        // transpilation, caveat emptor. The reserved
                        // virtual provider `sys::target` explicitly selects
                        // the target and bypasses DQL shadowing: its
                        // qualifier is virtual and the call proceeds as the
                        // target call it names.
                        self.judge_target_callee(&application.call().callee)?;
                        Ok(ast_resolved::FunctionApplication::Standard(
                            self.resolve_standard_application(application)?,
                        ))
                    }
                }
            }
            ast_unresolved::FunctionApplication::FieldSelect(select) => {
                let picked = Picked::Named(select.field);
                Ok(resolve_mode_call(self, select.application, picked)?
                    .expect("a named pick is answered or refused, never returned"))
            }
            // A RELATION BECOMES A VALUE in a fresh resolution context, and
            // the degree judgment is taken there, once.
            ast_unresolved::FunctionApplication::Scalarized(relation) => {
                Ok(ast_resolved::FunctionApplication::Scalarized(
                    super::resolving::domain_expressions::subqueries::resolve_scalar_relation_via_fold(
                        self, relation,
                    )?,
                ))
            }
            // A TEMPLATE RESOLVES WHERE IT STANDS. In value position
            // `transform_domain` reaches it first and builds the concat chain
            // the target spells; in cover position it is a `Callable::String`
            // and never arrives here at all.
            ast_unresolved::FunctionApplication::Template(template) => {
                let parts = template
                    .into_parts()
                    .into_iter()
                    .map(|part| match part {
                        ast_unresolved::ValueTemplatePart::Text(text) => {
                            Ok(ast_resolved::ValueTemplatePart::Text(text))
                        }
                        ast_unresolved::ValueTemplatePart::Interpolation(value) => {
                            Ok(ast_resolved::ValueTemplatePart::Interpolation(Box::new(
                                self.transform_domain(*value)?,
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(ast_resolved::FunctionApplication::Template(
                    ast_resolved::ValueTemplate::interpolating(parts)
                        .expect("a phase change preserves the interpolation invariant"),
                ))
            }

            // A constructed value: record spreads expand against the
            // available columns, and every other member resolves in place.
            ast_unresolved::FunctionApplication::Enclyph(enclyph) => {
                super::resolving::functions::resolve_function_enclyph_via_fold(self, enclyph)
            }

            // JsonPath: structural descent, then the wrong-aim check — pathing
            // reaches INTO a value, so a column whose declared type is plainly
            // scalar (INTEGER, REAL, BOOLEAN, dates) has no insides to reach
            // into. Without this check the failure is target-dependent and
            // silent-or-leaky (sqlite: 'malformed JSON' at runtime for text
            // that doesn't parse, silent NULLs for numbers that do). TEXT
            // stays permissive — documents live in TEXT columns — as do JSON,
            // NONE, and undeclared columns. Part of the DIALECT/interior-values
            // wrong-aim matrix; promise-ladder rung 1 preconditions.
            ast_unresolved::FunctionApplication::JsonAccess(_) => {
                let resolved = walk_transform_function(self, func)?;
                if let ast_resolved::FunctionApplication::JsonAccess(access) = &resolved {
                    let source = &access.source;
                    let wrong_aim = match source.as_ref() {
                        ast_resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence { column, .. }),
                        )) => scalar_declaration_for(column.column(), &self.core.identities)
                            .map(|decl| ("this column".to_string(), decl)),
                        _ => None,
                    };
                    if let Some((subject, decl)) = wrong_aim {
                        return Err(DelightQLError::ValidationError {
                            message: format!(
                                "cannot path into {subject}: it is declared {decl} — a \
                                     plain scalar has no insides to reach into. Pathing \
                                     ('col:{{.field}}' / 'col:[0]') expects a compound \
                                     value: something you built with {{...}}/[...], a \
                                     tree-group, or a document column (TEXT)."
                            ),
                            context: "resolver::json_path".to_string(),
                            subcategory: Some(crate::uri_registry::subcat::COMPOUND_SCALAR_COLUMN),
                        });
                    }
                }
                Ok(resolved)
            }

            // Everything else: walk handles structural descent
            // (infix, lambda, case, clause selection).
            other => walk_transform_function(self, other),
        }
    }
}

impl ResolverFold<'_, '_> {
    /// `#<n` / `#>n` — the authored row bound.
    ///
    /// The bound touches no heading, so it publishes its source's scope; the
    /// by-position fact is stamped HERE, where the bound is written, so every
    /// scope minted above it inherits it and no later reader goes looking for
    /// a LIMIT.
    fn r_resolve_bound(
        &mut self,
        source: ast_unresolved::Chain,
        bound: crate::pipeline::asts::core::TupleOrdinalClause,
    ) -> Result<ResolvedRelation> {
        let resolved_source = self.resolve_relational(source)?;
        let source_schema = resolved_source.semantic_relation();
        self.core
            .identities
            .authority()
            .mark_row_bounded(&source_schema)?;
        Ok(resolved_source.transparently(ast_resolved::Transparent::Bound { bound }))
    }

    /// `col ~= {…}` / `col ~= ~> {…}` — the destructure expansion.
    ///
    /// Unlike a restriction this ADDS columns, so it mints its own output
    /// scope and republishes the source's heading through it.
    fn r_resolve_destructure(
        &mut self,
        source: ast_unresolved::Chain,
        source_expr: ast_unresolved::DomainExpression,
        pattern: ast_unresolved::TreePattern,
        mode: crate::pipeline::asts::core::DestructureMode,
    ) -> Result<ResolvedRelation> {
        let resolved_source = self.resolve_relational(source)?;
        let source_schema = resolved_source.semantic_relation();
        // The document expression is read over the source alone.
        self.lexical.enter(resolved_source, super::Reach::Local);
        let resolved_source_expr = self.transform_domain(source_expr);
        let resolved_source = self.lexical.leave();
        let resolved_source_expr = resolved_source_expr?;
        super::resolving::predicates::validate_unresolved_pattern_for_mode(&pattern, &mode)?;
        super::resolving::predicates::validate_no_sibling_explosions(&pattern)?;
        super::resolving::predicates::validate_distinct_bindings(&pattern)?;
        let _ = source_schema;
        let identities = self.core.identities;
        resolved_source.destructured(resolved_source_expr, mode, pattern, &identities)
    }

    /// `+p(a, b)` in truth position, resolved.
    ///
    /// The classification order is load-bearing and pinned by
    /// `sigma_guard_scope_tests` and `enlisted_guard_classification_tests`:
    /// sigma rules first, then tables/facts/consulted views, then the bin
    /// cartridge's own predicates. The first three EXPAND into an ordinary
    /// truth expression; only the fall-through survives as an application.
    /// Both outcomes are the same carrier, so no arm rewraps a truth it just
    /// produced and no caller unwraps one.
    /// A metadata level: the key is a REFERENCE into the enclosing relation,
    /// so it binds like any other, and the levels under it chain the same way
    /// the surface writes them.
    pub(in crate::pipeline::resolver) fn resolve_metadata_group(
        &mut self,
        group: ast_unresolved::MetadataGroup,
    ) -> Result<ast_resolved::MetadataGroup> {
        use super::unification::{ColumnReference, UnificationResult};
        use crate::pipeline::asts::core::MetadataTarget;

        let mut witness = super::Witness::default();
        let result = self.lexical.address(
            ColumnReference::Named {
                name: group.key.name.clone(),
                qualifier: group.key.qualifier.clone(),
            },
            false,
            &mut witness,
            &self.core.identities,
        )?;
        let key = match result {
            UnificationResult::Resolved(occurrence) => occurrence.column,
            UnificationResult::Unresolved(column) => {
                return Err(DelightQLError::column_not_found_error(
                    column,
                    "in metadata tree group key",
                ))
            }
            UnificationResult::Opaque => {
                return Err(crate::pipeline::resolver::opaque_reference_refusal())
            }
            UnificationResult::Refused(refusal) => return Err(refusal.into_error()),
            UnificationResult::Ambiguous { column, tables } => {
                return Err(DelightQLError::validation_error_categorized(
                    "resolution/ambiguous",
                    format!(
                        "Ambiguous metadata tree-group key '{column}' in scopes: {}",
                        tables.join(", ")
                    ),
                    "qualify the metadata tree-group key",
                ))
            }
        };
        let target = match group.target {
            MetadataTarget::Enclyph(enclyph) => MetadataTarget::Enclyph(
                super::resolving::functions::resolve_enclyph_via_fold(self, enclyph)?,
            ),
            MetadataTarget::Group(nested) => {
                MetadataTarget::Group(Box::new(self.resolve_metadata_group(*nested)?))
            }
        };
        // WHAT THE TARGET DOES decides what each key holds. A target whose
        // every constructed member reduces SUMMARIZES its group — one object
        // per key. A target of plain members collects the group's rows — an
        // array per key. A mix would need an implicit aggregation for the
        // plain member, and there is none, ever.
        let summary = match &target {
            MetadataTarget::Enclyph(crate::pipeline::asts::core::Enclyph::Record(record)) => {
                let mut reduces = 0usize;
                let mut plain = None;
                for member in record.members.iter() {
                    match member {
                        ast_resolved::RecordMember::Keyed { key, value } => {
                            if self.reduces_its_group(value) {
                                reduces += 1;
                            } else {
                                plain = Some(key.clone());
                            }
                        }
                        ast_resolved::RecordMember::SelfKeyed(_)
                        | ast_resolved::RecordMember::Induced { .. }
                        | ast_resolved::RecordMember::Metadata { .. }
                        | ast_resolved::RecordMember::Spread(_) => plain = None.or(plain),
                    }
                }
                if reduces > 0 {
                    if let Some(plain) = plain {
                        return Err(DelightQLError::validation_error_categorized(
                            "constraint/implicit_aggregation",
                            format!(
                                "the group has many rows and the member '{plain}' has one \
                                 slot: a value with one answer per row cannot stand beside \
                                 a reduction, and there is no implicit aggregation, ever"
                            ),
                            "write the reduction, e.g. `\"k\": sum:(expr)`",
                        ));
                    }
                    true
                } else {
                    false
                }
            }
            _ => false,
        };
        Ok(ast_resolved::MetadataGroup {
            key: ColumnOccurrence::engine(key),
            target,
            cte_requirements: None,
            summary,
        })
    }

    /// Whether a resolved value REDUCES the group it stands in: an
    /// application of an aggregate function. The registry's descriptor
    /// answers by the callee's own name — the one place aggregate-ness is
    /// recorded.
    fn reduces_its_group(&self, value: &ast_resolved::DomainExpression) -> bool {
        let ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Standard(application),
        ) = value
        else {
            return false;
        };
        if application.window.is_some() {
            return false;
        }
        let mut name = String::new();
        self.core
            .identities
            .write_function_name(
                application.call().callee,
                &mut crate::names::sink::Teaching(&mut name),
            )
            .is_ok()
            && self.core.built_in.is_aggregate(&name)
    }

    fn resolve_sigma_application(
        &mut self,
        call: ast_unresolved::PureCall,
        polarity: crate::pipeline::asts::core::Polarity,
    ) -> Result<ast_resolved::TruthExpression> {
        let reference = call.call().callee.clone();
        let arguments = call
            .call()
            .arguments
            .scalar_members()
            .iter()
            .filter_map(|member| match member {
                crate::pipeline::asts::core::operators::ScalarArgument::Value(value) => {
                    Some(value.value.clone())
                }
                crate::pipeline::asts::core::operators::ScalarArgument::Spread(_)
                | crate::pipeline::asts::core::operators::ScalarArgument::Callable(_)
                | crate::pipeline::asts::core::operators::ScalarArgument::Star
                | crate::pipeline::asts::core::operators::ScalarArgument::Context(_) => None,
            })
            .collect::<Vec<_>>();
        let functor = reference.name_text().to_string();
        let functor_stropped = reference.name_identifier().is_stropped();
        let namespace = reference.namespace_texts();
        // QUALIFIED sigma citation (`+HL.h(v)`, `+a::b.h(v)`): the
        // qualifier resolves through the SAME alias- and
        // scope-aware door as qualified relations
        // (`lookup_entity`: exact fq, session alias at the
        // prompt, the OWNING namespace's local alias inside a
        // definition, §IV plain-qualifier expansion) — dropping
        // the qualifier and resolving the bare functor would
        // silently reach the wrong rule (or none, dying at SQL
        // generation). A sigma-rule hit expands here; any other
        // outcome falls through to the unqualified probes below,
        // so an unqualified citation's resolution is unaffected.
        if !namespace.is_empty() {
            let fq = namespace.join("::");
            let arguments = match crate::defuse::bound_use::use_sigma_qualified(
                self,
                &functor,
                functor_stropped,
                &fq,
                arguments,
            )? {
                crate::defuse::bound_use::SigmaQualified::Expanded(expanded) => {
                    return Ok(self.sigma_proof(expanded, polarity));
                }
                // The qualifier selected a BIN sigma predicate
                // (`+std::prelude.sql_eq(l, r)`, or an alias of that
                // namespace): the served entity is the selected identity,
                // whatever a nearer definition shadows, and THAT identity —
                // not the authored qualifier — is the callee lowering reads.
                crate::defuse::bound_use::SigmaQualified::ServedBin {
                    selected,
                    arguments,
                } => {
                    let callee = selected.callee(&self.core.identities);
                    return self.resolve_bin_sigma(call, callee, arguments, polarity);
                }
                crate::defuse::bound_use::SigmaQualified::NotSigma(arguments) => arguments,
            };
            // Not a sigma rule and not a served predicate: cite it as an
            // inner-exists with the qualifier STAMPED on the inner
            // reference — the qualified-relation machinery (aliases,
            // exposure, §IV expansion, and its loud refusals) resolves it.
            // A qualified citation always names a namespace entity; it
            // never falls through to the universal bin search.
            return super::resolving::predicates::expand_table_as_sigma(
                self, &functor, namespace, arguments, polarity,
            );
        }
        // Check if functor matches a consulted sigma predicate
        // (entity_type = 9). Scope first: inside a declaration world
        // (effect bodies, view bodies, HO expansions), a sigma rule
        // reachable from the declaring namespace (same file, or
        // enlisted into it) wins over one enlisted
        // into main — mirroring the relation path's
        // reach-scoped lookup (`Environment::relation`).
        // Without the scoped probe, a SAME-FILE sigma guard fell
        // through to the bin-rewrite path and died at SQL
        // generation ("Unknown predicate rewrite"), and a
        // same-named enlisted sigma silently shadowed the scope's
        // own rule. Sigma stays FIRST in the classification order
        // (before the table probes and the bin fall-through).
        // Pinned by sigma_guard_scope_tests.
        // STRICT definition independence: inside a definition the
        // sigma lookup is scoped to the OWNING namespace — itself
        // + its own edges — with NO session fallback; at the
        // prompt (None) the scope is `home`. A scoped-probe-then-
        // session-fallback two-step is a caller-leak: a file's
        // rule could silently find a sigma through whatever the
        // CALLER happened to have enlisted.
        // THE COMPLETE CANDIDATE SET, judged once. An existence test can
        // be answered by a sigma rule OR by a relation (table, fact,
        // view), so both faces are enumerated BEFORE either answers: a
        // sigma silently shadowing a same-named table was the collision
        // the checker exists for, and probe order must not decide it.
        let arguments = match crate::defuse::bound_use::use_sigma_enlisted(
            self,
            &functor,
            functor_stropped,
            arguments,
        )? {
            crate::defuse::bound_use::SigmaEnlisted::Expanded(expanded) => {
                return Ok(self.sigma_proof(expanded, polarity));
            }
            crate::defuse::bound_use::SigmaEnlisted::RelationAnswers(arguments) => {
                return super::resolving::predicates::expand_table_as_sigma(
                    self,
                    &functor,
                    Vec::new(),
                    arguments,
                    polarity,
                );
            }
            crate::defuse::bound_use::SigmaEnlisted::Neither(arguments) => arguments,
        };

        // Neither face answered: the universally visible bin sigma
        // predicates (`like`, `between`, `sql_eq`, …) stand, under the
        // bare name the author wrote.
        let callee = call
            .call()
            .callee
            .written_call_identity(&self.core.identities);
        self.resolve_bin_sigma(call, callee, arguments, polarity)
    }

    /// A BIN sigma predicate's application: the arguments resolve in the
    /// enclosing clause, and the call is built around the callee the CALLER
    /// answered with — the bare universal name, or the identity a qualified
    /// selection found — so nothing downstream recovers it from spelling.
    fn resolve_bin_sigma(
        &mut self,
        call: ast_unresolved::PureCall,
        callee: crate::names::FnId,
        arguments: Vec<ast_unresolved::DomainExpression>,
        polarity: crate::pipeline::asts::core::Polarity,
    ) -> Result<ast_resolved::TruthExpression> {
        let resolved_args = arguments
            .into_iter()
            .map(|arg| self.transform_domain(arg))
            .collect::<Result<Vec<_>>>()?;
        let marks = call.into_inner().marks;
        let resolved_call = ast_resolved::FunctorCall {
            callee,
            arguments: ast_resolved::CallArguments::Scalar(
                resolved_args
                    .into_iter()
                    .map(ast_resolved::ScalarArgument::plain)
                    .collect(),
            ),
            marks,
        };
        Ok(ast_resolved::TruthExpression::Sigma(
            SigmaApplication::applied(
                polarity,
                crate::pipeline::asts::core::PureCall::from_inner(resolved_call),
            ),
        ))
    }

    /// A DQL truth rule's body, resolved and left UNDER its observation.
    ///
    /// The polarity is not applied here. It is a collapse — it turns UNKNOWN
    /// into a definite answer — and truth position has no expression for one;
    /// applying Kleene NOT instead is what made `\\+f(x)` drop the rows whose
    /// body is unknown from both polarities. So the application survives
    /// resolution carrying what it observes, and the lowering spells
    /// `IS TRUE` or `IS NOT TRUE`.
    /// Wrap an ALREADY-RESOLVED sigma body as the observed proof: the
    /// polarity observes the body — `IS TRUE` / `IS NOT TRUE` — at the
    /// application, never inside it.
    fn sigma_proof(
        &mut self,
        body: ast_resolved::TruthExpression,
        polarity: crate::pipeline::asts::core::Polarity,
    ) -> ast_resolved::TruthExpression {
        ast_resolved::TruthExpression::Sigma(SigmaApplication {
            polarity,
            proof: crate::pipeline::asts::core::NamedProof::Body(Box::new(body)),
        })
    }
}
/// The SHAPES the mutation's source was piped through, outermost first: the
/// source chain's trailing run steps, classified for the mutation contract.
/// A non-run step ends the run exactly as the shared partition says it does.
fn classify_dml_source_shapes(source: &ast_unresolved::Chain) -> Vec<super::DmlPipeKind> {
    let mut kinds = Vec::new();
    for step in source.steps().iter().rev() {
        match step.form() {
            ast_unresolved::Continuation::Pipe { operator, .. } => {
                kinds.push(super::classify_single_dml_op(operator));
            }
            // An ordering that carries its bound IS the bound: the run
            // ends at it exactly as it ends at the arbitrary bound, and the
            // bounded-mutation law judges what it chose. Only a loose
            // ordering is the presentation the mutation contract refuses.
            ast_unresolved::Continuation::Structural(ast_unresolved::StructuralStep {
                form: ast_unresolved::StructuralForm::Ordering { bound: Some(_), .. },
                ..
            }) => break,
            ast_unresolved::Continuation::Structural(step) => kinds.push(match &step.form {
                ast_unresolved::StructuralForm::Ordering { .. } => {
                    super::DmlPipeKind::TupleOrdering
                }
                ast_unresolved::StructuralForm::Reposition { .. }
                | ast_unresolved::StructuralForm::Meta
                | ast_unresolved::StructuralForm::Witness { .. }
                | ast_unresolved::StructuralForm::SignedWitness
                | ast_unresolved::StructuralForm::Drill { .. }
                | ast_unresolved::StructuralForm::Narrow { .. } => super::DmlPipeKind::General,
            }),
            ast_unresolved::Continuation::Access { .. } => continue,
            ast_unresolved::Continuation::Restrict { .. }
            | ast_unresolved::Continuation::Correlate { .. }
            | ast_unresolved::Continuation::Bound { .. }
            | ast_unresolved::Continuation::Destructure { .. }
            | ast_unresolved::Continuation::Member { .. }
            | ast_unresolved::Continuation::BagOp { .. }
            | ast_unresolved::Continuation::ErJoin(_) => break,
        }
    }
    kinds
}

/// Put the resolved source into the call's source formal — the position
/// after the target, per the descriptor's layout for every mutation verb.
fn insert_dml_source_argument(call: &mut ast_resolved::FunctorCall, source: ast_resolved::Chain) {
    use crate::pipeline::asts::core::operators::{CallArguments, HoArgument};
    let members = match std::mem::replace(&mut call.call_mut().arguments, CallArguments::None) {
        CallArguments::HigherOrder(part) => {
            let mut members = part.into_members().into_vec();
            members.insert(1, HoArgument::Relation(source));
            members
        }
        CallArguments::None => vec![HoArgument::Relation(source)],
        CallArguments::Scalar(_) => {
            unreachable!("a mutation call carries a higher-order argument group")
        }
    };
    call.call_mut().arguments = CallArguments::higher_order(members);
}

/// Take a written DML call apart into the call and the relation it reads.
///
/// The two roles are counted, not searched: a mutation reads ONE source and
/// writes ONE target, and a call carrying two of either says nothing about
/// which one it meant. Taking the first would pick by argument order — the
/// one thing the roles exist so that nothing has to do.
fn split_dml_source(
    call: ast_unresolved::SealedCall,
) -> Result<(ast_unresolved::SealedCall, ast_unresolved::Chain)> {
    use crate::pipeline::asts::core::operators::{CallArguments, HoArgument};
    let effect = call.is_effect();
    let mut inner = call.into_inner();
    let mut relations = Vec::new();
    let mut kept = Vec::new();
    let members = match std::mem::replace(&mut inner.call_mut().arguments, CallArguments::None) {
        CallArguments::HigherOrder(part) => part.into_members().into_vec(),
        CallArguments::None => Vec::new(),
        other @ CallArguments::Scalar(_) => {
            inner.call_mut().arguments = other;
            return Err(DelightQLError::parse_error(
                "a mutation call carries a higher-order argument group",
            ));
        }
    };
    // THE DESCRIPTOR'S LAYOUT: the destination is the first relation formal
    // and the relation being read is the second — the position says what the
    // deleted role marks used to.
    // EXHAUSTIVE, so no member kind can be swallowed by a wildcard and
    // counted as a value: a relation is a relation wherever it came from,
    // and this counting is what tells the two roles apart for the direct
    // spelling as well as the piped one.
    for argument in members {
        match argument {
            HoArgument::Relation(relation) | HoArgument::Landed(relation) => {
                relations.push(relation)
            }
            HoArgument::Rule(_) => {
                return Err(DelightQLError::validation_error_categorized(
                    "dml/roles/rule_value",
                    "a mutation role requires a relation, not a residual rule value",
                    "complete the rule application before using its relation as a mutation operand",
                ));
            }
            value @ (HoArgument::Value(_) | HoArgument::Landing(_) | HoArgument::Skip) => {
                kept.push(value)
            }
        }
    }
    let mut relations = relations.into_iter();
    let (target, source, extra) = (relations.next(), relations.next(), relations.next());
    let Some(target) = target else {
        return Err(DelightQLError::validation_error_categorized(
            "dml/roles/target",
            "a mutation writes one relation; this call names 0".to_string(),
            "write the relation being mutated once: `|> update!(target(*))(*)`",
        ));
    };
    if extra.is_some() {
        return Err(DelightQLError::validation_error_categorized(
            "dml/roles/source",
            "a mutation reads one relation; this call names 2".to_string(),
            "pipe exactly one relation into the mutation",
        ));
    }
    let Some(source) = source else {
        return Err(DelightQLError::validation_error_categorized(
            "dml/roles/source",
            "a mutation reads one relation; this call names 0".to_string(),
            "pipe exactly one relation into the mutation",
        ));
    };
    kept.insert(0, HoArgument::Relation(target));
    inner.call_mut().arguments = CallArguments::higher_order(kept);
    Ok((
        ast_unresolved::SealedCall::from_inner(inner, effect),
        source,
    ))
}

fn extract_unresolved_base_ground_name(expr: &ast_unresolved::Chain) -> Option<String> {
    // Pipes leave the head alone; anything that brings another relation
    // means there is no single base to name.
    if !expr
        .steps()
        .iter()
        .all(|step| matches!(step.form(), ast_unresolved::Continuation::Pipe { .. }))
    {
        return None;
    }
    match expr.head().form() {
        ast_unresolved::GroundForm::Reference(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named { identifier, .. },
            ..
        }) => Some(identifier.name.to_string()),
        _ => None,
    }
}

fn direct_dml_terminal(expr: &ast_unresolved::Chain) -> Result<bool> {
    let Some(ast_unresolved::Relation::FunctorCall { call, .. }) = expr.as_read_relation() else {
        return Ok(false);
    };
    Ok(call.call().relations().nth(1).is_some()
        && Some(&call.call().callee).is_some_and(|reference| {
            matches!(
                crate::pipeline::asts::effects::descriptor_for_reference(reference)
                    .map(|descriptor| descriptor.category)
                    .unwrap_or(crate::pipeline::asts::effects::DirectiveCategory::User),
                crate::pipeline::asts::effects::DirectiveCategory::Dml(_)
            )
        }))
}

/// A `&` operand that is not a plain relation: edges are selected by their
/// terms, so there is nothing to select on.
/// The access a ground read was written under.
///
/// A mention and its parens are built together, so a mention arriving here
/// without one is a compiler fault and says so rather than defaulting to an
/// access nobody wrote.
fn read_access(access: Option<ast_unresolved::Access>) -> Result<ast_unresolved::Access> {
    access.ok_or_else(|| {
        DelightQLError::parse_error("a ground read reached resolution with no access beside it")
    })
}

/// One step of the trailing run a chain ends in.
///
/// An access and a pipe operator are different steps of ONE walk, so the run
/// carries both and parts only where each resolves.
/// The shared run partition, at the phase this walk resolves.

/// A named stage is a semantic export of the exact operator result. Its
/// ports correspond one-for-one in interface order, so the output-bearing
/// syntax moves by that total order rather than recovering a column by name
/// or value.
fn er_operand_error() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "grounding/er/operand_term",
        "an ER-join operand is not a relation-access term".to_string(),
        "edges are selected by their terms' canonical spellings",
    )
}

fn dml_multi_terminal_error() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "dml/shape/multi_terminal",
        "a DML terminal (insert!/update!/delete!) must be the final operation of a statement; multi-step DML via `,` (dataflow) or `;` (sequential) is not yet supported",
        "run each mutation as a separate statement",
    )
}

fn scalar_declaration_for(
    column: crate::names::ColId,
    identities: &crate::relation::Planning,
) -> Option<String> {
    identities
        .facts(column)
        .declared_type
        .filter(|declaration| {
            crate::pipeline::asts::core::metadata::is_plainly_scalar_declaration(declaration)
        })
}

/// Build the catalog-wrapper query carrying the namespace's liminal
/// ledger (EFFECT-ALGEBRA §8, THE LIMINAL RELATION), as a TYPED query —
/// constructed, never spelled: a JSON-derived echo column enters as an
/// identifier value here, where the text road would have spliced it into
/// source and asked the grammar to re-read it. The shape mirrors the
/// stored wrapper (`_(*) :- sys::meta.generator("ns")(*)`,
/// system.rs::register_catalog_wrapper) with one addition: a CTE that reads
/// the namespace's `liminal_receipt` rows and packs them into a `liminal`
/// tree-group column whose keys are the receipt prefix (success, operation)
/// plus THIS namespace's corresponding-union echo columns — fixed in the
/// built query, per-namespace at resolve time. The tree group is what
/// hands the interior-drill machinery its schema; everything downstream
/// (drill resolution, json_each/json_extract emission) is the stock path
/// that answers `.entities(*)` and `.namespaces(*)`. Receipt rows are packed
/// in rowid = insertion = file-appearance order: the `#(lim_id)` step before
/// the tree group pins the pack order through an ORDER BY subquery —
/// without it, SQLite's automatic covering index on the join reorders the
/// receipts (observed live: alias! packed before enlist!). The presented
/// ledger still carries no sequence column: `lim_id` orders the pack and is
/// not a tree-group key. An empty ledger packs to `[]`, which the
/// drill's outer join presents as one all-NULL receipt row — exactly how an
/// empty `namespaces` drill presents. Pinned by effects/liminal--43, --45,
/// and `liminal_drill_presents_the_corresponding_union`.
///
/// Every provenance slot says COMPILER: the binding is a generated
/// subject with `CompilerGenerated` origin, and the two restrictions are
/// `Generated` — no author wrote any of them. The CTE's minted scope
/// therefore hints `cte`-prefixed rather than `lim_cte` (an internal SQL
/// alias only; rows and headings are unchanged).
fn liminal_wrapper_query(
    ns_fq: &str,
    echo_columns: &[String],
    identities: &crate::relation::Planning,
) -> Result<ast_unresolved::Query> {
    use crate::pipeline::asts::core::expressions::enclyph::{Enclyph, Record, RecordMember};
    use crate::pipeline::asts::core::expressions::paths::{JsonAccess, Path, PathStep};
    use crate::pipeline::asts::core::{
        Access, AuthoredCteSubject, Continuation, CteBinding, FilterOrigin, GroupSpec,
        LiteralValue, NamespacePath, OneOut, OrderingSpec, OutItem, PipeOp, ReductionItem,
        StructuralForm, StructuralStep,
    };
    use crate::pipeline::asts::vocabulary::Vec1;

    let mut cols: Vec<String> = vec!["success".to_string(), "operation".to_string()];
    for c in echo_columns {
        if !cols.contains(c) {
            cols.push(c.clone());
        }
    }

    let column = |qualifier: Option<&str>, name: &str| {
        ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
            AuthoredColumn {
                name: SqlIdentifier::new(name),
                qualifier: qualifier.map(SqlIdentifier::new),
                namespace_path: NamespacePath::empty(),
            },
        )))
    };
    let equals = |left: ast_unresolved::DomainExpression,
                  right: ast_unresolved::DomainExpression| {
        ast_unresolved::TruthExpression::Comparison(crate::pipeline::asts::core::Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
            left: Box::new(left),
            right: Box::new(right),
        })
    };
    let sys_ns = NamespacePath::from_fq_string("sys::ns")
        .expect("the engine namespace spelling is constant");
    let read = |namespace_path: NamespacePath, name: &str, alias: &str, outer: bool| {
        ast_unresolved::Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier: ast_unresolved::QualifiedName {
                        namespace_path,
                        name: SqlIdentifier::new(name),
                    },
                    alias: Some(SqlIdentifier::new(alias)),
                    mutation_target: false,
                    passthrough: false,
                },
                outer,
            },
            Access::All,
        )
    };

    // (lim_r.id as lim_id, lim_r.receipt:{.c} as c, …)
    let mut items = vec![OutItem::One(OneOut::authored(
        column(Some("lim_r"), "id"),
        Some(SqlIdentifier::new("lim_id")),
    ))];
    items.extend(cols.iter().map(|c| {
        OutItem::One(OneOut::authored(
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::JsonAccess(JsonAccess {
                    source: Box::new(column(Some("lim_r"), "receipt")),
                    path: Path::try_from_steps(vec![PathStep::Key(c.clone())])
                        .expect("a key step is a path"),
                }),
            ),
            Some(SqlIdentifier::new(c)),
        ))
    }));

    // %(~> {success, operation, …} as liminal)
    let members: Vec<RecordMember<Unresolved>> = cols
        .iter()
        .map(|c| {
            RecordMember::SelfKeyed(NamedReference(AuthoredColumn {
                name: SqlIdentifier::new(c),
                qualifier: None,
                namespace_path: NamespacePath::empty(),
            }))
        })
        .collect();
    let pack = ReductionItem::Out(OutItem::One(OneOut::authored(
        ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Enclyph(Enclyph::Record(Record {
                members: Vec1::try_from_vec(members).expect("the receipt prefix keys are constant"),
            })),
        ),
        Some(SqlIdentifier::new("liminal")),
    )));

    let ledger = read(sys_ns.clone(), "namespace", "lim_ns", false)
        .then(ast_unresolved::Step::authored(Continuation::Restrict {
            condition: equals(
                column(Some("lim_ns"), "fq_name"),
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(LiteralValue::String(
                        ns_fq.to_string(),
                    )),
                ),
            ),
            origin: FilterOrigin::Generated,
        }))
        .then(ast_unresolved::Step::authored(Continuation::Member {
            rhs: read(sys_ns, "liminal_receipt", "lim_r", true),
            correlation: None,
            join_type: Some(JoinType::LeftOuter),
        }))
        .then(ast_unresolved::Step::authored(Continuation::Restrict {
            condition: equals(
                column(Some("lim_r"), "namespace_id"),
                column(Some("lim_ns"), "id"),
            ),
            origin: FilterOrigin::Generated,
        }))
        .then(ast_unresolved::Step::authored(Continuation::Pipe {
            operator: PipeOp::Project(
                Vec1::try_from_vec(items).expect("lim_id is always projected"),
            ),
            named: None,
        }))
        .then(ast_unresolved::Step::authored(Continuation::Structural(
            StructuralStep {
                form: StructuralForm::Ordering {
                    specs: vec![OrderingSpec {
                        column: column(None, "lim_id"),
                        direction: None,
                    }],
                    bound: None,
                },
                named: None,
            },
        )))
        .then(ast_unresolved::Step::authored(Continuation::Pipe {
            operator: PipeOp::Group(GroupSpec::Reduce {
                keys: Vec::new(),
                reductions: Vec1::new(pack),
                plan: crate::pipeline::asts::core::expressions::ReductionPlan::empty(),
            }),
            named: None,
        }));

    // sys::meta.generator("ns")(*), lim_cte(*)
    let generator = ast_unresolved::Chain::read(
        ast_unresolved::Relation::FunctorCall {
            call: crate::pipeline::asts::core::expressions::functions::FunctorCall {
                callee: crate::pipeline::asts::vocabulary::Ref::written(
                    identities.names(),
                    crate::pipeline::asts::vocabulary::Namespace::Path(Vec1::with_tail(
                        identities.intern("sys", false),
                        vec![identities.intern("meta", false)],
                    )),
                    identities.intern("generator", false),
                    crate::pipeline::asts::vocabulary::Mark::Plain,
                    crate::pipeline::asts::vocabulary::ResolutionMode::Normal,
                ),
                arguments: crate::pipeline::asts::core::operators::CallArguments::HigherOrder(
                    crate::pipeline::asts::core::operators::HoPart::of(Vec1::new(
                        crate::pipeline::asts::core::operators::HoArgument::Value(
                            ast_unresolved::ArgumentValue::plain(
                                ast_unresolved::DomainExpression::Application(
                                    ast_unresolved::FunctionApplication::Ground(
                                        LiteralValue::String(ns_fq.to_string()),
                                    ),
                                ),
                            ),
                        ),
                    )),
                ),
                marks: crate::pipeline::asts::vocabulary::FunctorMarks::with_evidence(false, false),
            }
            .into(),
            alias: None,
        },
        Access::All,
    )
    .then(ast_unresolved::Step::authored(Continuation::Member {
        rhs: ast_unresolved::Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier: ast_unresolved::QualifiedName {
                        namespace_path: NamespacePath::empty(),
                        name: SqlIdentifier::new("lim_cte"),
                    },
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
            },
            Access::All,
        ),
        correlation: None,
        join_type: None,
    }));

    Ok(ast_unresolved::Query::binding(
        crate::pipeline::asts::core::QueryLocals::compiler_built(vec![CteBinding::authored(
            ledger,
            AuthoredCteSubject::Generated {
                name: SqlIdentifier::new("lim_cte"),
            },
            crate::pipeline::asts::core::CteAuthority {
                horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                // A compiler-built carrier authors no badge.
                fixpoint: crate::pipeline::asts::vocabulary::Fixpoint::Bag,
            },
        )])?,
        generator,
    ))
}

#[cfg(test)]
mod liminal_drill_tests {
    //! Shape pins for the BUILT liminal wrapper query (EFFECT-ALGEBRA
    //! §8). End-to-end behavior is pinned by the effects-ball liminal--43/45
    //! baselines; these pin the construction contract the drill machinery
    //! depends on — read off the typed query, since no source text exists
    //! to pin.

    use super::liminal_wrapper_query;
    use crate::pipeline::ast_unresolved;
    use crate::pipeline::asts::core::expressions::enclyph::{Enclyph, RecordMember};
    use crate::pipeline::asts::core::{
        Continuation, GroupSpec, OutItem, PipeOp, ReductionItem, StructuralForm, StructuralStep,
    };

    fn wrapper(ns: &str, echoes: &[&str]) -> ast_unresolved::Query {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let echoes: Vec<String> = echoes.iter().map(|s| s.to_string()).collect();
        liminal_wrapper_query(ns, &echoes, &registry)
            .expect("the wrapper binds one generated subject")
    }

    /// The pack's record keys, in construction order.
    fn pack_keys(query: &ast_unresolved::Query) -> Vec<String> {
        let ctes = query.ctes();
        assert!(
            !ctes.is_empty(),
            "the wrapper carries its ledger as a CTE binding"
        );
        let Some(Continuation::Pipe {
            operator: PipeOp::Group(GroupSpec::Reduce { reductions, .. }),
            ..
        }) = ctes[0]
            .body()
            .continuations()
            .last()
            .map(|step| step.form())
        else {
            panic!("the ledger ends at the pack");
        };
        let ReductionItem::Out(OutItem::One(one)) = &reductions[0] else {
            panic!("the pack is one out item");
        };
        let ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Enclyph(Enclyph::Record(record)),
        ) = &one.expr
        else {
            panic!("the pack is a record");
        };
        record
            .members
            .iter()
            .map(|member| match member {
                RecordMember::SelfKeyed(reference) => reference.0.name.as_str().to_string(),
                other => panic!("the pack promotes columns by their own names: {other:?}"),
            })
            .collect()
    }

    /// The tree-group keys are the receipt prefix plus the namespace's
    /// corresponding-union echo columns, in order — the drill's presented
    /// interior schema (NULL-padded by json_extract for rows lacking a key).
    #[test]
    fn liminal_drill_presents_the_corresponding_union() {
        let query = wrapper("fx", &["namespace", "into", "shorthand"]);
        assert_eq!(
            pack_keys(&query),
            ["success", "operation", "namespace", "into", "shorthand"],
            "tree-group keys = receipt prefix + union echoes, in order"
        );

        let (ctes, main) = (query.ctes(), &query.body);
        assert!(
            !ctes.is_empty(),
            "the wrapper carries its ledger as a CTE binding"
        );
        assert!(
            ctes[0].body().continuations().iter().any(|step| matches!(
                step.form(),
                Continuation::Structural(StructuralStep {
                    form: StructuralForm::Ordering { .. },
                    ..
                })
            )),
            "the pack is ordered by insertion (rowid) — SQLite's automatic \
             covering index otherwise reorders the receipts"
        );
        assert!(
            matches!(
                main.head().form(),
                crate::pipeline::asts::core::GroundForm::Reference(
                    ast_unresolved::Relation::FunctorCall { .. }
                )
            ) && main
                .continuations()
                .iter()
                .any(|step| matches!(step.form(), Continuation::Member { .. })),
            "the ledger rides beside the stored wrapper's own generator join"
        );
    }

    /// Every provenance slot says COMPILER. No author wrote the binding,
    /// its name, or the join predicates, and the AST states exactly that:
    /// a generated subject, a generated origin, and generated filters.
    #[test]
    fn the_generated_wrapper_claims_no_author() {
        let query = wrapper("fx", &[]);
        let binding = &query.ctes()[0];
        assert!(
            matches!(
                binding.subject(),
                crate::pipeline::asts::core::CteSubjectView::Generated { .. }
            ),
            "the binding stands on a generated subject"
        );
        assert!(
            matches!(
                binding.authority().origin,
                crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated
            ),
            "origin says who constructed it"
        );
        let restricts: Vec<_> = binding
            .body()
            .continuations()
            .iter()
            .filter_map(|step| match step.form() {
                Continuation::Restrict { origin, .. } => Some(origin),
                _ => None,
            })
            .collect();
        assert_eq!(restricts.len(), 2, "the namespace and receipt joins");
        assert!(
            restricts.iter().all(|origin| matches!(
                origin,
                crate::pipeline::asts::core::FilterOrigin::Generated
            )),
            "no user wrote these predicates"
        );
    }

    /// An empty ledger (namespace created by other means) packs the bare
    /// receipt prefix — the drill then presents one all-NULL receipt row
    /// over `[]`, exactly like an empty namespaces drill.
    #[test]
    fn liminal_drill_empty_ledger_presents_the_bare_prefix() {
        assert_eq!(pack_keys(&wrapper("home", &[])), ["success", "operation"]);
    }

    /// A JSON-derived echo spelling is DATA here: it lands in the query as
    /// an identifier value, never as source for the grammar to re-read. On
    /// the deleted text road this spelling misparsed the whole wrapper.
    #[test]
    fn an_untrustworthy_echo_spelling_stays_data() {
        let hostile = "a\" |> nonsense(";
        assert_eq!(
            pack_keys(&wrapper("fx", &[hostile])),
            ["success", "operation", hostile]
        );
    }
}

/// SPEND A FORMAL'S CALLER-RESOLVED VALUE AGAINST ONE HEADING. The value
/// references the caller's exact ports; the reference lands on the ONE
/// NEW position of the heading that CONTINUES that port's occurrence, by
/// the continuation edge construction wrote and by nothing else. The exact
/// caller port may remain available beside that continuation as outer
/// context; it is the source, not a second landing. A value
/// republished at a second position does not continue it, so no choice is
/// ever made between two positions; a heading continuing the occurrence
/// twice (the carrier joined with itself) refuses. A port the heading does
/// not continue at all stays as the caller resolved it (an enclosing row's
/// column, or a position the body projected away).
pub(in crate::pipeline::resolver) fn anchor_formal(
    identities: &crate::relation::Planning,
    heading: &[crate::relation::PortId],
    value: crate::pipeline::ast_resolved::DomainExpression,
) -> Result<crate::pipeline::ast_resolved::DomainExpression> {
    use crate::pipeline::ast_transform::{walk_transform_domain, AstTransform};
    use crate::pipeline::asts::core::Resolved;
    struct Anchor<'a> {
        identities: &'a crate::relation::Planning,
        heading: &'a [crate::relation::PortId],
    }
    impl AstTransform<Resolved, Resolved> for Anchor<'_> {
        crate::pipeline::ast_transform::same_phase_payload_folds!(Resolved);
        #[stacksafe::stacksafe]
        fn transform_domain(
            &mut self,
            expr: crate::pipeline::ast_resolved::DomainExpression,
        ) -> Result<crate::pipeline::ast_resolved::DomainExpression> {
            if let crate::pipeline::ast_resolved::DomainExpression::Reference(Reference::Named(
                NamedReference(occurrence),
            )) = &expr
            {
                let continuing: Vec<_> = self
                    .heading
                    .iter()
                    .copied()
                    .filter(|port| {
                        *port != occurrence.column
                            && self
                                .identities
                                .continues_occurrence(*port, occurrence.column)
                    })
                    .collect();
                return match continuing.as_slice() {
                    [] => Ok(expr),
                    [carrier] => Ok(crate::pipeline::ast_resolved::DomainExpression::Reference(
                        Reference::Named(NamedReference(occurrence.rebound(*carrier))),
                    )),
                    several => Err(DelightQLError::validation_error_categorized(
                        "ho/actual/ambiguous_occurrence",
                        format!(
                            "the caller's actual continues at {} positions of this row — the \
                             carrier stands beside itself — and a formal names one occurrence",
                            several.len()
                        ),
                        "read the carrier once where the formal is spent",
                    )),
                };
            }
            walk_transform_domain(self, expr)
        }
    }
    Anchor {
        identities,
        heading,
    }
    .transform_domain(value)
}

impl ResolverFold<'_, '_> {
    /// One domain expression read FLAT over the row in view — an anonymous
    /// literal's header or cell, which has no frame of its own to shadow
    /// the row it stands in.
    pub(super) fn resolve_flat_over_the_row(
        &mut self,
        expression: ast_unresolved::DomainExpression,
    ) -> Result<ast_resolved::DomainExpression> {
        let was = self.lexical.set_flat(true);
        let result = self.transform_domain(expression);
        self.lexical.set_flat(was);
        result
    }

    /// The formal's value, spent against THIS fold's row.
    pub(crate) fn spend_formal(
        &self,
        value: crate::pipeline::ast_resolved::DomainExpression,
    ) -> Result<crate::pipeline::ast_resolved::DomainExpression> {
        anchor_formal(
            &self.core.identities,
            &self.lexical.local_ports(&self.core.identities)?,
            value,
        )
    }
}
