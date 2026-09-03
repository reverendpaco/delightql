// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE RESOLVER'S CURRENCY: a resolved relation and what answers over it.
//!
//! Relational resolution used to answer with `(Chain, scope)` at every
//! boundary. A pair at a boundary is a pair anyone can permute: affine
//! values still exchange, so one resolution's relation could meet another's
//! lexical scope and type-check. This carrier's fields are visible to the
//! lexical module alone, and its constructors are the semantic acts
//! resolution actually performs. The frontier is never supplied beside a
//! relation; it is DERIVED from the act — from the relation the act
//! published, from the operands the act stood on, or from the relation the
//! act just rebuilt.
//!
//! Because there is no free pairing, "which relation does this frontier
//! belong to" is not a question a caller can answer wrongly. And because
//! neither this carrier nor its [`Frontier`] is `Clone`, a frontier an act
//! consumed is gone rather than merely unused.

use super::Frontier;
use crate::error::Result;
use crate::pipeline::{ast_resolved, ast_unresolved};

/// A RESOLVED RELATION AND WHAT ANSWERS OVER IT.
///
/// Produced only by the acts below; read only through the questions
/// below; consumed only by [`ResolvedRelation::into_body`] and the
/// operations that answer with another carrier. No operation hands out
/// the relation alone.
pub(crate) struct ResolvedRelation {
    chain: ast_resolved::Chain,
    frontier: Frontier,
}

impl ResolvedRelation {
    /// A RELATION THAT ANSWERS FOR ITSELF — the ordinary read, access,
    /// literal, projection, or expansion. What answers over it is what it
    /// publishes, so the scope is read off the relation rather than
    /// supplied beside it.
    pub(in crate::pipeline::resolver) fn answering_for_itself(chain: ast_resolved::Chain) -> ResolvedRelation {
        let frontier = Frontier::of(chain.semantic_relation());
        ResolvedRelation { chain, frontier }
    }

    /// THE ONE WHOLE-RELATION ARGUMENTATIVE OPERATION.
    ///
    /// A slot row is applied to a relation — a catalog access, an
    /// anonymous literal, a consulted or higher-order result, a join
    /// member, a completed stage, an effect receipt — and one finished read
    /// comes back: the row's own published interface, the constraints it
    /// stated, and what answers over it. Operand, complete interface, slots
    /// and owner are bound HERE, in one act: the interface is derived from
    /// the operand, exact width is judged once, a slot's authored reference
    /// consumes the frontier's terminal judgment through the fold's
    /// position, a binder reuses by the one bare-reuse law over the row in
    /// view, and the owner's disposition is closed — bare binders alone,
    /// or bare binders that also answer through the authored name, interned
    /// AS WRITTEN. No caller supplies an interface, a row to reuse from, or
    /// a judgment beside the operand; the mechanism behind this act is
    /// private to the lexical authority, and the row it binds is sealed.
    pub(crate) fn patterned(
        operand: PatternOperand,
        access: &ast_unresolved::Access,
        owner: PatternOwner,
        fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    ) -> Result<PatternRead> {
        let scope = match &operand {
            PatternOperand::Read { scope, .. } => *scope,
            PatternOperand::Standing(standing) => standing.semantic_relation(),
        };
        let instantiation = crate::pipeline::resolver::SlotInstantiation {
            core: fold.core,
            env: fold.env,
            instances: &fold.config.instances,
            formals: None,
            horizon: None,
        };
        let pattern = super::pattern::resolve(
            access,
            scope,
            &owner,
            &fold.lexical,
            instantiation,
            &fold.core.identities,
        )?;
        let publishes = pattern.publishes();
        // A whole read keeps the relation it read reachable; an authored
        // owner names the one interface the row published (`t(a, b) as u`
        // answers `u.a`); an unaliased row activates no name — and the
        // relation it published records none, so no act that reads a
        // birth answer can grant one later.
        let frontier = if pattern.is_whole_read() || matches!(owner, PatternOwner::Authored(_)) {
            Frontier::of(publishes)
        } else {
            Frontier::bare_only()
        };
        let using_columns = pattern.using_columns().map(<[_]>::to_vec);
        let (chain, where_constraints) = match operand {
            PatternOperand::Read { outer, .. } => {
                pattern.ground_read(outer, &fold.core.identities)?
            }
            // The standing carrier is CONSUMED: the row stands on its chain
            // and nothing of its frontier survives the row.
            PatternOperand::Standing(standing) => {
                pattern.applied_to(standing.into_body(), &fold.core.identities)?
            }
        };
        Ok(PatternRead {
            publishes,
            relation: ResolvedRelation { chain, frontier },
            where_constraints,
            using_columns,
        })
    }

    /// A ROW DECLARED HERE, AND STOOD OVER. A fact function's declared
    /// input row, a DDL door's declared columns: the row is BORN in this
    /// act — derived from the declaration's own slots, which are spellings
    /// and not identities — and read in the same act, so what stands is a
    /// relation nothing else could have supplied. Its birth answer is the
    /// declaration's, or none.
    pub(crate) fn declared_row(
        spec: crate::relation::form::AnonymousSpec<'_>,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let authority = identities.authority();
        let relation = authority.derive(crate::relation::RelForm::Anonymous(spec))?;
        let read = authority.ground_read(ast_resolved::Access::All, false, relation)?;
        Ok(ResolvedRelation::answering_for_itself(read))
    }

    /// A GROUND READ OF A RELATION WHOSE DIMENSIONS ARE UNENUMERABLE.
    ///
    /// The opaque relation is derived and read HERE, so what answers over
    /// the read is the relation the read was made of. It offers no columns
    /// to enumerate and still carries its scope: a reference standing over
    /// it must be able to find out that nothing was enumerated, rather than
    /// be told the name is absent.
    pub(crate) fn opaque_ground(
        outer: bool,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let authority = identities.authority();
        let scope = authority.derive(crate::relation::RelForm::Opaque)?;
        let chain = authority.ground_read(ast_resolved::Access::All, outer, scope)?;
        Ok(ResolvedRelation {
            chain,
            frontier: Frontier::of(scope),
        })
    }

    /// THE DIMENSIONS ASKED OF AN AUTHORITY-MINTED HEAD.
    ///
    /// The head is the authority's own artifact and carries the relation it
    /// publishes; the access is asked of it HERE. What answers is derived
    /// from that one head: an unenumerable heading answers for the head's
    /// own relation, and anything else for what the read published. No
    /// caller supplies a scope beside a finished chain.
    pub(crate) fn asking(
        head: crate::pipeline::asts::core::Grelex<crate::pipeline::asts::core::Resolved>,
        access: ast_resolved::Access,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let published = *head.result();
        let opaque = identities.authority().interface(&published)?.is_opaque();
        let chain = identities
            .authority()
            .read_asking(ast_resolved::Chain::ground(head), access)?;
        Ok(if opaque {
            ResolvedRelation {
                chain,
                frontier: Frontier::of(published),
            }
        } else {
            ResolvedRelation::answering_for_itself(chain)
        })
    }

    /// §6's SET FAMILY: two operands become one merged publication.
    ///
    /// Both carriers are CONSUMED and the set step is DERIVED HERE, from
    /// the relations they hold — so the result the bag installs is the
    /// result these two arms produced, and a step derived over some other
    /// pair cannot reach this operation because no such step crosses the
    /// boundary. What answers over the result is the result's own merged
    /// heading: the rows of both arms flow through positions neither arm
    /// named, so a form over the result reaches nothing of the arms. The
    /// correlation attached to the operation still names them.
    pub(crate) fn bagged(
        left: ResolvedRelation,
        right: ResolvedRelation,
        operator: crate::pipeline::asts::core::SetOperator,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let authority = identities.authority();
        // ONE ENTRANCE for all four operators. Every one of them publishes
        // the RESULT's own positions: the rows of both arms flow through
        // them, so neither arm's heading is the answer. The step comes back
        // with the operator and its result together, over these arms.
        let step = authority.set_step(
            operator,
            &[left.semantic_relation(), right.semantic_relation()],
        )?;
        let frontier_of_arms = (left.frontier, right.frontier);
        let chain = authority.bag(left.chain, step, right.chain, None);
        let frontier = Frontier::bag(
            chain.semantic_relation(),
            &frontier_of_arms.0,
            &frontier_of_arms.1,
        );
        Ok(ResolvedRelation { chain, frontier })
    }

    /// §5's COMMA, OPENED: both operands, owned together.
    ///
    /// A join correspondence is not a property of either operand — it is a
    /// relationship between a specific left occurrence and a specific
    /// right one. So the artifact that derives it owns BOTH, and every
    /// relationship the derivation needs is read off the two relations it
    /// holds. Nothing states a left interface beside a right relation.
    pub(crate) fn joining(left: ResolvedRelation, right: ResolvedRelation) -> ResolvedJoin {
        ResolvedJoin {
            left,
            right,
            directed: None,
            pending: Vec::new(),
        }
    }

    /// §5's COMMA over a CALLER PATTERN'S right member. The pattern's own
    /// constraints are split against the left row this join holds: one
    /// that reaches it is the join's condition, stated at the join so it
    /// lowers against both operands' sites; one that does not stays the
    /// member's own restriction. The USING correspondence, if the pattern
    /// named one, is built from the two headings here, and the two are
    /// conjoined in the same act.
    pub(crate) fn joining_pattern(
        left: ResolvedRelation,
        mut read: PatternRead,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedJoin> {
        let left_columns = crate::relation::published_ports(identities, &left.semantic_relation())?;
        let right_ports =
            crate::relation::published_ports(identities, &read.relation().semantic_relation())?;
        let using = match read.using_columns() {
            Some(columns) if !columns.is_empty() => Some(super::join::create_using_condition(
                columns,
                &left_columns,
                &right_ports,
                identities,
            )?),
            _ => None,
        };
        let (crossing, local): (Vec<_>, Vec<_>) = read
            .take_constraints()
            .into_iter()
            .partition(|constraint| super::join::comparison_reaches(constraint, &left_columns));
        let directed = match (using, ast_resolved::TruthExpression::all(crossing)) {
            (Some(ast_resolved::MemberCorrelation::Condition(using)), Some(more)) => {
                Some(ast_resolved::MemberCorrelation::Condition(
                    ast_resolved::TruthExpression::all(vec![using, more])
                        .expect("two conditions conjoin"),
                ))
            }
            // A USING correspondence and a stated condition are different
            // correlations; both present is a shape no current pattern
            // produces, and the condition wins the seat rather than being
            // dropped in silence.
            (Some(correlation), None) => Some(correlation),
            (Some(_), Some(more)) | (None, Some(more)) => {
                Some(ast_resolved::MemberCorrelation::Condition(more))
            }
            (None, None) => None,
        };
        Ok(ResolvedJoin {
            left,
            right: read.restricted_locally(local, identities)?,
            directed,
            pending: Vec::new(),
        })
    }

    /// AN OPERAND REWORKED INTO A NEW PUBLICATION — an authored alias, an
    /// ER hop's boundary export, a row bound, a read taken apart and
    /// landed again.
    ///
    /// The carrier is consumed and the scope is DERIVED AFRESH from the
    /// relation that comes back, so whatever the rebuild produces, what
    /// answers over it is that relation and not the one it replaced.
    /// There is no relationship for the closure to get wrong.
    pub(crate) fn republished(
        self,
        rebuild: impl FnOnce(ast_resolved::Chain) -> Result<ast_resolved::Chain>,
    ) -> Result<ResolvedRelation> {
        let ResolvedRelation { chain, frontier } = self;
        drop(frontier);
        Ok(ResolvedRelation::answering_for_itself(rebuild(chain)?))
    }

    /// A READ THAT KEEPS ITS REACH. A whole access asks for every dimension
    /// the relation already published and for nothing else, so what
    /// answered over the relation still answers over the read: the
    /// rebuilt relation stands here, and every relation the frontier
    /// reached stays reachable above it. Not a pipe crossing — no scope
    /// died — and an authored name still stamps the read as its owner.
    pub(crate) fn republished_within(
        self,
        answer: Option<crate::names::Spelling>,
        identities: &crate::relation::Planning,
        rebuild: impl FnOnce(ast_resolved::Chain) -> Result<ast_resolved::Chain>,
    ) -> Result<ResolvedRelation> {
        let ResolvedRelation { chain, frontier } = self;
        let chain = rebuild(chain)?;
        if let Some(answer) = answer {
            identities
                .authority()
                .own_stage(&chain.semantic_relation(), answer)?;
        }
        let mut next = Frontier::of(chain.semantic_relation());
        next.also_through_all(&frontier);
        Ok(ResolvedRelation {
            chain,
            frontier: next,
        })
    }

    /// AN EDGE'S BOUNDARY BINDS ITS ENDPOINTS. The boundary publishes ONE
    /// heading — schema(A) + schema(B) — and each endpoint name reaches the
    /// positions that belong to that endpoint: `users_t.name` is one
    /// qualified reference to one column of the edge. The routes are bound
    /// here, on the frontier, from the exports the boundary was derived
    /// from, in the order the boundary published them; no column carries
    /// its endpoint's name.
    pub(crate) fn er_boundary(
        chain: ast_resolved::Chain,
        exports: &[crate::relation::form::ErExport],
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let relation = chain.semantic_relation();
        let ports = crate::relation::published_ports(identities, &relation)?;
        if ports.len() != exports.len() {
            return Err(crate::error::DelightQLError::transformation_error(
                "an edge boundary published a heading of a different width than its exports",
                "edge boundary",
            ));
        }
        let mut frontier = Frontier::of(relation);
        let mut endpoints: Vec<(crate::names::Sym, Vec<crate::relation::PortId>)> = Vec::new();
        for (port, export) in ports.iter().copied().zip(exports) {
            match endpoints
                .iter_mut()
                .find(|(endpoint, _)| *endpoint == export.endpoint)
            {
                Some((_, reached)) => reached.push(port),
                None => endpoints.push((export.endpoint, vec![port])),
            }
        }
        for (endpoint, reached) in endpoints {
            frontier.also_reaching(relation, endpoint, reached);
        }
        Ok(ResolvedRelation { chain, frontier })
    }

    /// AN EDGE'S BOUNDARY OVER A REPUBLISHED OPERAND: the carrier is
    /// consumed, the boundary is derived from its chain, and what answers
    /// is the boundary's own endpoints.
    pub(crate) fn republished_as_er_boundary(
        self,
        identities: &crate::relation::Planning,
        rebuild: impl FnOnce(
            ast_resolved::Chain,
        )
            -> Result<(ast_resolved::Chain, Vec<crate::relation::form::ErExport>)>,
    ) -> Result<ResolvedRelation> {
        let ResolvedRelation { chain, frontier } = self;
        drop(frontier);
        let (chain, exports) = rebuild(chain)?;
        if exports.is_empty() {
            return Ok(ResolvedRelation::answering_for_itself(chain));
        }
        ResolvedRelation::er_boundary(chain, &exports, identities)
    }

    /// A DRILL KEEPS ITS CONTEXT AND OPENS THE INTERIOR. The relation the
    /// drill stood on stays reachable above the drilled read, and the
    /// interior it opened answers to the name of the column it was drilled
    /// out of — `t(*).items(*)` puts `items` in view, so `items.x` names
    /// the interior's `x` and not an `x` the level above also publishes.
    /// The route is bound HERE, on the frontier, by the act that opened
    /// the interior; no column carries it.
    pub(crate) fn drilled(
        self,
        answer: Option<crate::names::Spelling>,
        nest: crate::names::Sym,
        interior: crate::relation::SemanticRelation,
        identities: &crate::relation::Planning,
        rebuild: impl FnOnce(ast_resolved::Chain) -> Result<ast_resolved::Chain>,
    ) -> Result<ResolvedRelation> {
        let ResolvedRelation { chain, frontier } = self;
        let chain = rebuild(chain)?;
        if let Some(answer) = answer {
            identities
                .authority()
                .own_stage(&chain.semantic_relation(), answer)?;
        }
        let mut next = Frontier::of(chain.semantic_relation());
        next.also_through_all(&frontier);
        next.also_through_as(interior, nest);
        Ok(ResolvedRelation {
            chain,
            frontier: next,
        })
    }

    /// AN ARGUMENTATIVE READ crosses bare: the slots bind under their
    /// authored names and nothing reaches them qualified until an alias
    /// names the one interface they publish.
    pub(crate) fn crossed_bare(
        self,
        answer: Option<crate::names::Spelling>,
        identities: &crate::relation::Planning,
        rebuild: impl FnOnce(ast_resolved::Chain) -> Result<ast_resolved::Chain>,
    ) -> Result<ResolvedRelation> {
        let ResolvedRelation { chain, frontier } = self;
        drop(frontier);
        let chain = rebuild(chain)?;
        if let Some(answer) = answer {
            identities
                .authority()
                .own_stage(&chain.semantic_relation(), answer)?;
        }
        Ok(ResolvedRelation {
            chain,
            frontier: Frontier::bare_only(),
        })
    }

    /// THE PIPE CROSSING. The carrier goes in by value and its frontier
    /// ends here; the far side's frontier is born from the produced
    /// relation and from the one name authored on that exact result.
    ///
    /// There is no argument for predecessor state. What the operation
    /// consumed answered inside the operation, through the position that
    /// borrowed this carrier; it is not available to the closure, which
    /// receives the chain alone, and it is not available to the result,
    /// which is derived from the rebuilt chain alone. An authored name is
    /// stamped on the produced relation as its owner — an identity fact the
    /// metadata view reports — and reaches a later reference only because
    /// that relation is what the far side answers for.
    pub(crate) fn crossed(
        self,
        answer: Option<crate::names::Spelling>,
        identities: &crate::relation::Planning,
        rebuild: impl FnOnce(ast_resolved::Chain) -> Result<ast_resolved::Chain>,
    ) -> Result<ResolvedRelation> {
        let ResolvedRelation { chain, frontier } = self;
        drop(frontier);
        let chain = rebuild(chain)?;
        if let Some(answer) = answer {
            identities
                .authority()
                .own_stage(&chain.semantic_relation(), answer)?;
        }
        Ok(ResolvedRelation::answering_for_itself(chain))
    }

    /// A DESTRUCTURING RIDES; IT DOES NOT REPUBLISH. Reading fields out
    /// of a document ADDS positions and leaves every other one exactly
    /// where it was, so every relation the source could be addressed
    /// through is still addressable above it.
    ///
    /// ONE ACT: the expansion's interface, the mappings that name the
    /// positions it adds, and the stored pattern are all written by the
    /// authority from the one pattern this states — over THIS carrier's
    /// own relation, which is the only relation the act can name. A step
    /// derived over some other source cannot reach here, because no such
    /// step crosses the boundary.
    pub(crate) fn destructured(
        self,
        source: ast_resolved::DomainExpression,
        mode: crate::pipeline::asts::core::DestructureMode,
        pattern: crate::pipeline::ast_unresolved::TreePattern,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let ResolvedRelation { chain, frontier } = self;
        let (staged, _) =
            identities
                .authority()
                .bind(crate::relation::pending::Pending::Destructure {
                    input: chain.semantic_relation(),
                    source,
                    mode,
                    pattern,
                })?;
        // A destructuring is not a PIPE FORM: the relations it stood on
        // stay addressable above it, so `n.first_name` still names its
        // column when a destructure stands between the join and the pipe.
        let chain = identities.authority().reland(chain, staged)?;
        let mut expanded = Frontier::of(chain.semantic_relation());
        expanded.also_through_all(&frontier);
        Ok(ResolvedRelation {
            chain,
            frontier: expanded,
        })
    }

    /// A RESTRICTION ATTACHED AT THE BASE — the USING correlation.
    ///
    /// Rows drop and nothing is republished, so what answers over the
    /// relation still answers and the scope travels. The relation never
    /// leaves: the closure is shown the base the filters are computed
    /// against and answers with FILTERS, not with a chain, so there is no
    /// other relation this act could end up holding.
    pub(crate) fn restricted_at_base(
        self,
        identities: &crate::relation::Planning,
        filters: impl FnOnce(&ast_resolved::Chain) -> Result<Vec<ast_resolved::TruthExpression>>,
    ) -> Result<ResolvedRelation> {
        let ResolvedRelation { chain, frontier } = self;
        // THE BASE IS WHAT THE STEP NAMED. A pipe publishes its own
        // heading, so the column the run named may be gone by the end of
        // the chain; the filters are read where they will stand.
        let (base, trailing) = chain
            .peel_while(|form| matches!(form, ast_resolved::Continuation::Pipe { .. }))
            .into_parts();
        let filters = filters(&base)?;
        let correlated =
            crate::pipeline::resolver::insert_filters_at_base(base, filters, identities)?;
        Ok(ResolvedRelation {
            chain: identities.authority().reland_all(correlated, trailing)?,
            frontier,
        })
    }

    /// OUTERNESS IS THE CALL SITE'S. Marking the head changes nothing a
    /// relation publishes, so the scope stands. Closed: no closure, no
    /// second relation.
    pub(crate) fn head_marked_outer(mut self, outer: bool) -> ResolvedRelation {
        self.chain.mark_head_outer(outer);
        self
    }

    /// A TRANSPARENT WRAPPER — a form the AST itself defines as changing
    /// nothing a relation publishes. Closed: there is no closure and no
    /// second relation, so the scope travels because it must.
    pub(crate) fn transparently(self, wrapper: ast_resolved::Transparent) -> ResolvedRelation {
        let ResolvedRelation { chain, frontier } = self;
        ResolvedRelation {
            chain: chain.transparently(wrapper),
            frontier,
        }
    }

    /// A TRANSPARENT WRAPPER LANDED BEHIND THE HEAD'S OWN READ. Where it
    /// stands is derived here, from this chain's own shape: a leading
    /// access belongs to the head, and a generated restriction stands
    /// right behind it.
    pub(crate) fn transparently_behind_head_access(
        self,
        wrapper: ast_resolved::Transparent,
    ) -> ResolvedRelation {
        let ResolvedRelation { chain, frontier } = self;
        let position = usize::from(matches!(
            chain.continuations().first().map(ast_resolved::Step::form),
            Some(ast_resolved::Continuation::Access { .. })
        ));
        ResolvedRelation {
            chain: chain.transparently_at(position, wrapper),
            frontier,
        }
    }

    /// THE PAYLOAD RESTATED under the relation it already publishes — a
    /// bootstrap read served as literal rows. The authority keeps the
    /// result, so the scope is unchanged by construction.
    pub(crate) fn payload_restated(
        mut self,
        identities: &crate::relation::Planning,
        form: crate::pipeline::asts::core::GroundForm<crate::pipeline::asts::core::Resolved>,
    ) -> ResolvedRelation {
        identities
            .authority()
            .restate_payload(&mut self.chain, form);
        self
    }

    /// AN AUTHORED ALIAS AT A CALL SITE. The export answers to the name
    /// from here on, so the scope is derived from the relation the alias
    /// published — the same act an ordinary republication takes, spelled
    /// for the one caller that has an authored name to spend.
    pub(crate) fn aliased(
        mut self,
        answer: crate::names::Spelling,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let relation = identities
            .authority()
            .alias_result(&mut self.chain, answer)?;
        self.frontier.now_answers_for(relation);
        Ok(self)
    }

    /// The semantic relation standing here.
    pub(crate) fn semantic_relation(&self) -> crate::relation::SemanticRelation {
        self.chain.semantic_relation()
    }

    /// What answers over it — read by the lexical authority alone.
    pub(super) fn frontier(&self) -> &Frontier {
        &self.frontier
    }

    /// What this resolution still owes, to read.
    pub(crate) fn owes(&self) -> &[crate::pipeline::resolver::unification::ColumnReference] {
        self.frontier.owes()
    }

    /// AN EXISTS CONDITION'S SIBLING WITNESSES become reachable over this
    /// relation, bare and qualified: `+orders(...), +items(..., orders.x = y)`
    /// names the earlier witness from the later one. Read off this
    /// carrier's own chain — the one the statement will contain.
    pub(crate) fn with_exists_witnesses(mut self) -> ResolvedRelation {
        for witness in self.witness_relations() {
            self.frontier.also_witness(witness);
        }
        self
    }

    /// THE JUDGMENTS THIS CARRIER ANSWERS ABOUT ITS RELATION.
    ///
    /// There is no borrow of the relation itself: `Chain` is `Clone`, so a
    /// borrow would be an owned-chain escape and the carrier would not be
    /// affine in the thing that matters. Every question the resolver asks
    /// is asked here and answers with something that is not a relation.

    /// The relations an EXISTS condition's witnesses stand on.
    pub(crate) fn witness_relations(&self) -> Vec<crate::relation::SemanticRelation> {
        let mut witnesses = Vec::new();
        crate::pipeline::resolver::exists_witness_relations(&self.chain, &mut witnesses);
        witnesses
    }

    /// Whether this relation is a truth witness (`+`/`\+`): it answers to
    /// the relation it probes so a correlation can address it, but it is
    /// not a live row-space relation.
    pub(crate) fn is_truth_witness(&self) -> bool {
        crate::pipeline::resolver::resolver_fold::chain_is_truth_witness(&self.chain)
    }

    /// Whether the head still reads a CALLABLE relation. Higher-order
    /// expansion consumes the call carrier and answers with the expanded
    /// relation; only an ordinary unresolved TVF stays a functor call.
    pub(crate) fn head_reads_a_call(&self) -> bool {
        matches!(
            self.chain.head().form(),
            ast_resolved::GroundForm::Reference(ast_resolved::Relation::FunctorCall {
                alias: (),
                ..
            })
        )
    }

    /// The columns an inline USING access named, when the right operand
    /// wrote one.
    pub(crate) fn inline_using_columns(&self) -> Option<Vec<delightql_types::SqlIdentifier>> {
        crate::pipeline::resolver::extract_inline_using_columns(&self.chain)
    }

    /// THE MUTATION MARK, noted against the relation that carries it.
    pub(crate) fn noting_mutation_mark(
        &self,
        marked: Option<crate::names::Spelling>,
        identities: &crate::relation::Planning,
    ) -> Result<()> {
        crate::pipeline::resolver::relation_resolver::note_mutation_mark(
            marked,
            &self.chain,
            identities,
        )
    }

    /// The correlation filters an interior relation's scope carries, for
    /// the hygienic injection that has to survive a projection.
    pub(crate) fn correlation_filters(
        &self,
        identities: &crate::relation::Planning,
    ) -> Result<Vec<ast_resolved::TruthExpression>> {
        crate::pipeline::refiner::correlation_analyzer::detect_correlation_filters_in_scope(
            &self.chain,
            identities,
        )
    }

    /// THE GROUND HEAD a served bootstrap read stands on: the relation it
    /// publishes and its outerness. `None` when the head is not a ground
    /// reference at all.
    pub(crate) fn ground_head(&self) -> Option<(crate::relation::SemanticRelation, bool)> {
        let head = self.chain.head();
        match head.form() {
            ast_resolved::GroundForm::Reference(ast_resolved::Relation::Ground {
                outer, ..
            }) => Some((*head.result(), *outer)),
            _ => None,
        }
    }

    /// THE PIVOT KEYS a following operator may read, scanned off the
    /// relation standing here.
    ///
    /// One of the two JUDGMENTS this carrier answers ABOUT its relation.
    /// Both answer with a judgment and never with a relation.
    pub(crate) fn pivot_keys(
        &self,
        identities: &crate::relation::Planning,
    ) -> crate::pipeline::resolver::PivotInWitnesses {
        crate::pipeline::resolver::extract_in_predicate_values_from_resolved(
            &self.chain,
            identities,
        )
    }

    /// Narrowing a knowable object literal is a provable mistake — the
    /// other judgment, refused while the anon source is still in hand.
    pub(crate) fn refusing_knowable_object_narrowing(
        &self,
        column: &str,
        identities: &crate::relation::Planning,
    ) -> Result<()> {
        crate::pipeline::resolver::relation_resolver::refuse_knowable_object_narrowing(
            column,
            &self.chain,
            identities,
        )
    }

    /// WHAT A RESOLUTION STILL OWES, for a caller that resolved a body only
    /// to read its dependencies. The relation is spent; the needs travel.
    pub(crate) fn into_needs(self) -> Vec<crate::pipeline::resolver::unification::ColumnReference> {
        self.frontier.owed
    }

    /// THE PHASE BOUNDARY. Lexical resolution state stops being meaningful
    /// once the resolved body leaves resolution, so the body is handed on
    /// and the scope ends here. Narrowing, never splitting: what comes
    /// back is one artifact, so nothing downstream can pair it again.
    pub(crate) fn into_body(self) -> ast_resolved::Chain {
        self.chain
    }

    /// THE PHASE BOUNDARY, for a caller that still stands inside a
    /// lexical extent — a definition body resolved for a use that will
    /// itself answer under the caller's scope. The pair does not travel:
    /// what travels is this carrier.
    pub(crate) fn into_query(
        self,
        into: impl FnOnce(ast_resolved::Chain) -> ast_resolved::Query,
    ) -> ResolvedQuery {
        let ResolvedRelation { chain, frontier } = self;
        ResolvedQuery {
            query: into(chain),
            frontier,
        }
    }
}

/// §5's COMMA, MID-ACT: BOTH OPERANDS AND THE RELATIONSHIP BETWEEN THEM.
///
/// A join correspondence names a left occurrence and a right one, so the
/// artifact that derives it owns both relations. Private fields, and no
/// operation takes a left interface, a resolved correspondence, or a
/// lexical scope from outside: every relationship below is derived from
/// the two relations this value already holds. So evidence for one pair
/// of operands cannot be installed over another — there is no road that
/// carries it, and no second operand to carry it to.
pub(crate) struct ResolvedJoin {
    left: ResolvedRelation,
    right: ResolvedRelation,
    directed: Option<ast_resolved::MemberCorrelation>,
    /// What an authored, deferred join condition owes.
    pending: Vec<crate::pipeline::resolver::unification::ColumnReference>,
}

/// Where an ANONYMOUS right member goes: a membership test against the
/// left row, or on as the join's right operand.
pub(crate) enum AnonRouting {
    /// Every header was a probe: this is not a relation but a truth about
    /// the left row, and what stands is the left relation restricted.
    Membership(ResolvedRelation),
    /// The headers name a relation; the join goes on.
    Join(ResolvedJoin),
}

impl ResolvedJoin {
    /// The left columns this join's relationships are decided against —
    /// read off the left relation this artifact owns, never supplied.
    fn left_columns(
        &self,
        identities: &crate::relation::Planning,
    ) -> Result<Vec<crate::relation::PortId>> {
        crate::relation::published_ports(identities, &self.left.semantic_relation())
    }

    /// `t(*.(a, b))` — the right member's access DEQUALIFIED onto named
    /// columns. The correspondence is built here, from the two headings
    /// this artifact owns.
    pub(crate) fn dequalifying(
        mut self,
        columns: &[delightql_types::SqlIdentifier],
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedJoin> {
        if !columns.is_empty() {
            let left_columns = self.left_columns(identities)?;
            let right_ports =
                crate::relation::published_ports(identities, &self.right.semantic_relation())?;
            self.directed = Some(super::join::create_using_condition(
                columns,
                &left_columns,
                &right_ports,
                identities,
            )?);
        }
        Ok(self)
    }

    /// `t(.*)` — the access dequalified onto every column the two headings
    /// share, computed here from those two headings.
    pub(crate) fn dequalifying_all(
        mut self,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedJoin> {
        let left_columns = self.left_columns(identities)?;
        let right_ports =
            crate::relation::published_ports(identities, &self.right.semantic_relation())?;
        self.directed = Some(super::join::create_using_all_condition(
            &left_columns,
            &right_ports,
            identities,
        )?);
        Ok(self)
    }

    /// An ANONYMOUS right member: whether its headers unify with columns
    /// in scope is decided here, from both headings. An ALIASED anonymous
    /// table is a closed relation — its headers declare under the alias,
    /// so they neither unify bare nor collide, and the refusal-free probe
    /// only detects membership shape.
    ///
    /// The answer routes the member: an anonymous table whose every header
    /// is a probe — a ground literal, or an lvar that unifies with a
    /// column in scope — is not a relation but a MEMBERSHIP test, and what
    /// stands is the left relation restricted by it. The plain comma form
    /// takes that road whenever every column unifies, because multi-row
    /// unification is membership: a duplicate row cannot multiply outer
    /// rows, and a null component is a value the probe can match.
    pub(crate) fn unifying_anonymously(
        mut self,
        headers: Option<&[crate::pipeline::ast_unresolved::DomainExpression]>,
        alias: Option<&delightql_types::SqlIdentifier>,
        identities: &crate::relation::Planning,
    ) -> Result<AnonRouting> {
        let left_columns = self.left_columns(identities)?;
        let visible = &self.left.frontier;
        if let Some(headers) = headers {
            let right_ports =
                crate::relation::published_ports(identities, &self.right.semantic_relation())?;
            self.directed = if alias.is_some() {
                super::join::aliased_anon_would_unify(
                    headers,
                    &left_columns,
                    &right_ports,
                    visible,
                    identities,
                )?
            } else {
                super::join::detect_anonymous_table_unification(
                    headers,
                    &left_columns,
                    &right_ports,
                    visible,
                    identities,
                )?
            };
        }
        match super::join::build_anon_membership(
            headers,
            &self.directed,
            &left_columns,
            &self.right.chain,
            alias,
            visible,
            identities,
        )? {
            Some(membership) => Ok(AnonRouting::Membership(self.left.transparently(
                ast_resolved::Transparent::Restrict {
                    condition: membership,
                    origin: ast_resolved::FilterOrigin::UserWritten,
                },
            ))),
            None => Ok(AnonRouting::Join(self)),
        }
    }

    /// AN AUTHORED CONDITION DEFERS. `a, b : cond` states the join's
    /// relationship in the author's own words; it is resolved later, when
    /// filters are processed, so what this join directs is nothing and
    /// what it owes is the needs the condition bubbled — BUBBLED HERE,
    /// under the left row this artifact owns, so the deferral and its
    /// needs are one act rather than two values a caller brought together.
    pub(crate) fn deferring_authored_condition(
        mut self,
        condition: crate::pipeline::ast_unresolved::TruthExpression,
        fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
    ) -> Result<ResolvedJoin> {
        // THE CONDITION IS BUBBLED UNDER THE LEFT ROW: the left carrier is
        // the frame while its needs are gathered, and comes back to this
        // artifact after.
        fold.lexical.enter(self.left, super::Reach::Row);
        let bubbled = crate::pipeline::resolver::bubble_predicate_expression(condition, fold);
        self.left = fold.lexical.leave();
        let (_unresolved, pending) = bubbled?;
        self.directed = None;
        self.pending = pending;
        Ok(self)
    }

    /// TWO LIVE SCOPES NEVER SHARE A NAME, judged over the co-visible set
    /// this join makes: the relations both operands' frontiers reach. A
    /// relation another reachable relation was built FROM is not a rival
    /// (the construction record says so), and a truth witness carries its
    /// mark. An unaliased argumentative call reaches nothing here — its
    /// frontier holds no route — so `p(x), p(y)` stand side by side, while
    /// two aliased rows named `u` are two live scopes named `u`. The ruled
    /// danger gate admits namesakes beside each other.
    pub(crate) fn admitting_distinct_names(
        &self,
        policy: crate::names::DuplicateScopePolicy,
        identities: &crate::relation::Planning,
    ) -> Result<()> {
        let mut addressable: Vec<crate::relation::SemanticRelation> = Vec::new();
        for relation in self
            .left
            .frontier
            .relations()
            .chain(self.right.frontier.relations())
        {
            if !addressable.contains(&relation) {
                addressable.push(relation);
            }
        }
        let mut co_visible: Vec<crate::names::ScopeId> = Vec::new();
        for relation in &addressable {
            let stood_under = addressable.iter().any(|other| {
                other.relation() != relation.relation()
                    && other.scope() != relation.scope()
                    && crate::relation::contains_scope(identities, other, relation.scope())
                        .unwrap_or(false)
            });
            if !stood_under && !identities.is_truth_witness(relation.scope()) {
                co_visible.push(relation.scope());
            }
        }
        identities.refuse_shared_names(&co_visible, policy)?;
        Ok(())
    }

    /// The left operand, to read.
    pub(crate) fn left(&self) -> &ResolvedRelation {
        &self.left
    }

    /// The right operand, to read.
    pub(crate) fn right(&self) -> &ResolvedRelation {
        &self.right
    }

    /// THE JOIN, ASSEMBLED. Every fact it takes from its operands is
    /// derived while this artifact owns them: the recorded correspondence,
    /// the merged keys, the relation the right operand publishes, and the
    /// total decision that a pair which neither merges nor constrains
    /// CROSSES. Its law comes from its authored spelling and nothing else.
    ///
    /// What answers over the join is what answered over both operands;
    /// what it still owes is what both owed plus what an authored
    /// condition bubbled up for a later phase to answer.
    pub(crate) fn joined(
        self,
        join_type: Option<crate::pipeline::asts::core::operators::JoinType>,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        use crate::pipeline::asts::core::operators::JoinType;
        let kind = match join_type {
            Some(JoinType::LeftOuter) => crate::relation::form::JoinKind::LeftOuter,
            Some(JoinType::RightOuter) => crate::relation::form::JoinKind::RightOuter,
            Some(JoinType::FullOuter) => crate::relation::form::JoinKind::FullOuter,
            None | Some(JoinType::Inner) => crate::relation::form::JoinKind::Inner,
        };
        let ResolvedJoin {
            left,
            right,
            mut directed,
            pending,
        } = self;
        let left_scope = left.semantic_relation();
        let right_scope = right.semantic_relation();
        let left_ports = crate::relation::published_ports(identities, &left_scope)?;
        let right_ports = crate::relation::published_ports(identities, &right_scope)?;
        let authority = identities.authority();
        let mut token_pairs = Vec::new();
        for left in left_ports.iter().copied() {
            let Some(token) = authority.residual_row_token(left) else {
                continue;
            };
            let matches: Vec<_> = right_ports
                .iter()
                .copied()
                .filter(|right| authority.residual_row_token(*right) == Some(token))
                .collect();
            match matches.as_slice() {
                [right] => token_pairs.push(crate::relation::form::MergedKey {
                    left,
                    right: *right,
                }),
                [] => {}
                [_, _, ..] => {
                    return Err(crate::error::DelightQLError::transformation_error(
                        "one relation carries a residual row token more than once",
                        "resolved join",
                    ));
                }
            }
        }
        let condition_token_rights = if matches!(
            directed,
            Some(ast_resolved::MemberCorrelation::Condition(_))
        ) {
            token_pairs
                .iter()
                .map(|pair| pair.right)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        // EXACT REUSE IS CONSUMED, NEVER REDISCOVERED. Right-operand
        // resolution recorded, at each binding it minted, the exactly-one
        // live bare port that spelling reuses. The pairs whose left port
        // this join's left operand publishes are this join's
        // correspondence; nothing here re-derives them from characters,
        // ordinals, ancestry, or AST shapes.
        if directed.is_none() {
            let pairs =
                crate::relation::recorded_correspondence(identities, &left_scope, &right_scope)?;
            if !pairs.is_empty() {
                directed = Some(ast_resolved::MemberCorrelation::Correspond(
                    ast_resolved::Correspondence::new(pairs),
                ));
            }
        }
        if !token_pairs.is_empty() {
            directed = match directed {
                Some(ast_resolved::MemberCorrelation::Correspond(mut correspondence)) => {
                    correspondence.pairs.extend(token_pairs);
                    Some(ast_resolved::MemberCorrelation::Correspond(correspondence))
                }
                Some(ast_resolved::MemberCorrelation::Condition(condition)) => {
                    let mut conditions = vec![condition];
                    conditions.extend(token_pairs.into_iter().map(|pair| {
                        use crate::pipeline::asts::core::{
                            ColumnOccurrence, Comparison, NamedReference, Reference,
                        };
                        ast_resolved::TruthExpression::Comparison(Comparison {
                            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                            left: Box::new(ast_resolved::DomainExpression::Reference(
                                Reference::Named(NamedReference(ColumnOccurrence::engine(
                                    pair.left,
                                ))),
                            )),
                            right: Box::new(ast_resolved::DomainExpression::Reference(
                                Reference::Named(NamedReference(ColumnOccurrence::engine(
                                    pair.right,
                                ))),
                            )),
                        })
                    }));
                    Some(ast_resolved::MemberCorrelation::Condition(
                        ast_resolved::TruthExpression::all(conditions)
                            .expect("a stated condition makes the conjunction nonempty"),
                    ))
                }
                Some(ast_resolved::MemberCorrelation::Cartesian(())) | None => {
                    Some(ast_resolved::MemberCorrelation::Correspond(
                        ast_resolved::Correspondence::new(token_pairs),
                    ))
                }
            };
        }
        // THE CORRELATION IS THE MERGE. A correspondence says which pairs
        // the join merges; the heading it publishes is derived from that
        // ONE fact, and every later reader — the refiner's rebuild
        // included — reads the same correspondence off the step. Computing
        // the merge from a second source left the resolved join and its
        // rebuild publishing different widths.
        let merged = match &directed {
            Some(ast_resolved::MemberCorrelation::Correspond(correspondence)) => {
                correspondence.pairs.clone()
            }
            Some(
                ast_resolved::MemberCorrelation::Condition(_)
                | ast_resolved::MemberCorrelation::Cartesian(_),
            )
            | None => match right.inline_using_columns() {
                Some(columns) => {
                    let names = columns.into_iter().map(|name| {
                        identities.canonical(identities.intern(name.as_str(), name.is_stropped()))
                    });
                    let left_ports = crate::relation::published_ports(identities, &left_scope)?;
                    let right_ports = crate::relation::published_ports(identities, &right_scope)?;
                    ast_resolved::Correspondence::between(
                        names,
                        &left_ports,
                        &right_ports,
                        identities,
                    )?
                    .pairs
                }
                None => Vec::new(),
            },
        };
        if !merged.is_empty()
            && !matches!(
                directed,
                Some(ast_resolved::MemberCorrelation::Condition(_))
            )
        {
            directed = Some(ast_resolved::MemberCorrelation::Correspond(
                ast_resolved::Correspondence::new(merged.clone()),
            ));
        }
        crate::probe::probe!(using, "dedup pairs={merged:?}");
        // THE DECISION IS TOTAL. Every road above has spoken: a
        // correspondence merges the pair, a condition constrains it, and a
        // pair neither merges nor constrains CROSSES — stated here, where
        // the live bare interface was enumerated, so no later phase can
        // read an absence as a join.
        let correlation = directed.unwrap_or(ast_resolved::MemberCorrelation::Cartesian(()));
        let frontier = left.frontier.merged(right.frontier, pending);
        let mut chain = identities.authority().extend(
            left.chain,
            crate::relation::builder::StepOp::Join {
                rhs: right.chain,
                correlation,
                join_type,
                right: right_scope,
                kind,
                merged: &merged,
            },
        )?;
        if !condition_token_rights.is_empty() {
            // A condition join needs both the authored predicate and the
            // construction-row equality, so its correlation cannot occupy
            // the correspondence variant that also merges headings. Remove
            // the right-hand copies immediately after the join instead. The
            // exact carried ports are construction facts, never spellings.
            let input = chain.semantic_relation();
            let dropped: Vec<_> = condition_token_rights
                .into_iter()
                .map(|right| authority.port_in(&input, right))
                .collect::<Result<_>>()?;
            let sources: Vec<_> = crate::relation::published_ports(identities, &input)?
                .into_iter()
                .filter(|port| !dropped.contains(port))
                .collect();
            let slots: Vec<_> = sources
                .iter()
                .copied()
                .map(|source| crate::relation::form::ProjectSlot::Carried {
                    source,
                    naming: crate::relation::form::Naming::Inherited,
                })
                .collect();
            chain = authority.extend(
                chain,
                crate::relation::builder::StepOp::Republish {
                    of: crate::relation::builder::Republishing::Project(
                        crate::relation::form::ProjectSpec {
                            input,
                            why: crate::relation::form::ProjectWhy::Restate,
                            slots: &slots,
                            dependencies: &[],
                        },
                    ),
                    sources,
                },
            )?;
        }
        Ok(ResolvedRelation { chain, frontier })
    }
}

/// WHAT A CALLER PATTERN READ.
///
/// The relation is already a carrier — the read, the source it stays
/// addressable through, and the scope derived in one act — so what a
/// caller partitions here is CONSTRAINTS, never chains.
/// WHO OWNS A SLOT ROW'S PUBLICATION — closed and semantic. The row
/// publishes bare binders either way; whether the same positions also
/// answer through a name is the owner's disposition, and nothing else
/// about the syntax that produced the row survives here.
pub(crate) enum PatternOwner {
    /// An unaliased argumentative access activates no name: `p(x), p(y)`
    /// coexist because the calls are addressed by their variables.
    Unqualified,
    /// `as u` — the one interface the row published answers through `u`.
    Authored(delightql_types::SqlIdentifier),
}

/// WHAT A SLOT ROW STANDS ON: a relation the row reads for the first
/// time, or the carrier standing where the row is placed.
pub(crate) enum PatternOperand {
    Read {
        scope: crate::relation::SemanticRelation,
        outer: bool,
    },
    Standing(ResolvedRelation),
}

pub(crate) struct PatternRead {
    publishes: crate::relation::SemanticRelation,
    relation: ResolvedRelation,
    where_constraints: Vec<ast_resolved::TruthExpression>,
    using_columns: Option<Vec<delightql_types::SqlIdentifier>>,
}

impl PatternRead {
    /// The constraints the pattern stated, taken for a caller that must
    /// decide which of them a join carries and which stay local.
    fn take_constraints(&mut self) -> Vec<ast_resolved::TruthExpression> {
        std::mem::take(&mut self.where_constraints)
    }

    /// The columns the pattern dequalified onto, when it named any.
    fn using_columns(&self) -> Option<&[delightql_types::SqlIdentifier]> {
        self.using_columns.as_deref()
    }

    fn relation(&self) -> &ResolvedRelation {
        &self.relation
    }

    /// EVERY CONSTRAINT THE PATTERN STATED, applied — the single-relation
    /// road, where no join stands to take any of them. A restriction drops
    /// rows and republishes nothing, so what answers over the read still
    /// answers.
    pub(crate) fn restricted_by_its_own_constraints(
        mut self,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let constraints = self.take_constraints();
        self.restricted_locally(constraints, identities)
    }

    /// THE LOCAL CONSTRAINTS, applied against the pattern's own
    /// publication.
    fn restricted_locally(
        self,
        constraints: Vec<ast_resolved::TruthExpression>,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        let PatternRead {
            publishes,
            relation,
            ..
        } = self;
        let ResolvedRelation { chain, frontier } = relation;
        let _ = identities;
        Ok(ResolvedRelation {
            chain: super::pattern::apply_local_constraints(chain, constraints, publishes),
            frontier,
        })
    }
}

/// A RESOLVED QUERY AND WHAT ANSWERS OVER ITS BODY.
///
/// The same carrier one level out: a complete inline query's answer,
/// which some callers consume for the query alone and some carry on into
/// the scope they are still resolving under.
pub(crate) struct ResolvedQuery {
    query: ast_resolved::Query,
    frontier: Frontier,
}

impl ResolvedQuery {
    /// The query alone — for a caller whose own lexical extent has
    /// already ended. Narrowing, so nothing here can be paired again.
    pub(crate) fn into_query(self) -> ast_resolved::Query {
        self.query
    }

    /// The same query with RESOLVED bindings standing ahead of its own: the
    /// caller-resolved carriers a body reads by identity. The scope is the
    /// body's; the carriers publish nothing above it.
    pub(crate) fn with_leading_ctes(mut self, leading: Vec<ast_resolved::CteBinding>) -> Self {
        if !leading.is_empty() {
            let own = std::mem::take(self.query.locals.ctes_mut());
            *self.query.locals.ctes_mut() = leading.into_iter().chain(own).collect();
        }
        self
    }

    /// THE BODY, when the query carries no CTEs — the shape an ER edge's
    /// composition requires. What comes back is the carrier again, so the
    /// body and the scope it answers under are never apart.
    pub(crate) fn into_relational_body(
        self,
    ) -> std::result::Result<ResolvedRelation, ResolvedQuery> {
        let ResolvedQuery { query, frontier } = self;
        match query.into_bare_body() {
            Ok(chain) => Ok(ResolvedRelation { chain, frontier }),
            Err(query) => Err(ResolvedQuery {
                query: *query,
                frontier,
            }),
        }
    }
}
