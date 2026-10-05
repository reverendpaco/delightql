// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The chain spine — DESIGN-CORE-AST §2.2's relational carrier, shaped for
//! the phase-marker tree the compiler runs today.
//!
//! A relational expression is a HEAD and the continuations that consume it.
//! `continuations[i]` consumes exactly the relation produced by
//! `head ++ continuations[..i]`, so left-associativity is structural: there
//! is no `source` field to nest, and pipe order is the vector's order.
//!
//! The payloads are the compiler's own carriers (`SigmaCondition`,
//! `TruthExpression`, `PipeOp`, `Relation`). The head's
//! reference-versus-literal split is the taxonomy `Grelex` states.

use super::super::TupleOrdinalClause;
use super::super::{
    LiteralValue, Phase, Unresolved,
};
use super::access::Slot;
use super::domain::DomainExpression;
use super::metadata_types::{FilterOrigin, SetOperator};
use super::relational::Relation;
use super::truth::TruthExpression;
use crate::diagnostic::Internal;
use crate::pipeline::asts::core::operators::PipeOp;
use crate::pipeline::asts::vocabulary::ArmIx;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// §2.2 — the chain: a ground relational expression and the continuations
/// applied to it, in authored order.
///
/// PRIVATE FIELDS. A bound step's relation is its prefix's answer, so a
/// vector anyone can push into or assign through is the attachment road the
/// step's own privacy closed: `chain.continuations[0] = other_step` relates
/// two valid objects wrongly without naming a single private field. The
/// authored phase has nothing to mispair and keeps ordinary mutation.
#[derive(Debug, PartialEq, ToLispy)]
#[lispy("chain")]
pub struct Chain<P: Phase = Unresolved> {
    #[lispy("head")]
    head: Grelex<P>,
    #[lispy("continuations")]
    continuations: Vec<Step<P>>,
}

// Manual Clone: `#[stacksafe]` keeps a deeply nested chain (member arms and
// bag arms recurse) off the 8 MB spawned-thread stack.
impl<P: Phase> Clone for Chain<P> {
    #[stacksafe::stacksafe]
    fn clone(&self) -> Self {
        Chain {
            head: self.head.clone(),
            continuations: self.continuations.clone(),
        }
    }
}

impl<P: Phase<Scope = ()>> Chain<P> {
    /// A chain that is nothing but its authored head.
    ///
    /// The authored phase has no relation to pair, so ordinary syntax
    /// constructors live here: there is nothing a caller could mispair.
    pub fn authored(form: GroundForm<P>) -> Self {
        Chain::ground(Grelex::authored(form))
    }

    /// A READ: a relation, and what the parens on it asked for. The one
    /// constructor for the pairing, so a mention cannot be built without an
    /// access or an access without the relation it asks of.
    pub fn read(relation: Relation<P>, access: super::access::Access<P>) -> Self {
        Chain::authored(GroundForm::Reference(relation)).then(Step::authored(
            Continuation::Access {
                access,
                named: P::no_stage_name(),
            },
        ))
    }

    /// The same pairing over a head that is already built.
    pub fn read_head(head: GroundForm<P>, access: super::access::Access<P>) -> Self {
        Chain::authored(head).then(Step::authored(Continuation::Access {
            access,
            named: P::no_stage_name(),
        }))
    }

    /// One bag operation. The chain-so-far is the left operand and `arm` is
    /// the single right operand this step owns; `a ; b ; c` is a SEQUENCE of
    /// these steps, never one grouped node. There is no grouped constructor
    /// because a group has nowhere to record which pair a correlation
    /// relates (ruling 3).
    pub fn bag_op(self, operator: SetOperator, arm: Chain<P>, correlation: P::Corr) -> Self {
        self.then(Step::authored(Continuation::BagOp {
            operator,
            arm,
            correlation,
        }))
    }
}

impl<P: Phase<Scope = ()>> Chain<P> {
    /// Extend an AUTHORED chain. Nothing is paired in this phase, so
    /// ordering is the only thing a step carries and there is nothing a
    /// caller could relate wrongly.
    ///
    /// There is no bound-phase twin. A step that already publishes a
    /// relation cannot be appended behind another prefix, because the
    /// relation it publishes is an answer about the prefix it was derived
    /// over; putting it behind a different one keeps an answer to a
    /// question nobody asked. Bound chains grow through the authority,
    /// which derives the step's result from the prefix it is landing on.
    pub fn then(mut self, step: Step<P>) -> Self {
        self.continuations.push(step);
        self
    }

    /// Bound the chain: THE ONE ROAD a row bound enters an authored chain.
    ///
    /// A bound consumes the ordering it stands immediately beside — the
    /// ordering chooses which rows the bound keeps — so it lands INSIDE that
    /// ordering's node rather than after it, and there is no authored shape
    /// in which an ordering and the bound that consumes it are two steps.
    /// An ordering that already carries a bound is a finished membership
    /// act; a further bound selects from its members and stands alone,
    /// as does a bound that no ordering precedes.
    pub fn bounding(mut self, bound: TupleOrdinalClause) -> Self {
        use super::super::specs::TupleOrdinalOperator;
        if let Some(Continuation::Structural(StructuralStep {
            form: StructuralForm::Ordering {
                bound: consumer, ..
            },
            ..
        })) = self.continuations.last_mut().map(Step::form_mut)
        {
            match consumer {
                None => {
                    *consumer = Some(bound);
                    return self;
                }
                // AN OFFSET IS CONSUMED BY THE CAP IT PRECEDES: `#(x), #>a,
                // #<m` is one row clause — the ordering, where its count
                // starts, and how many — so the cap joins the act rather
                // than capping its members arbitrarily.
                Some(TupleOrdinalClause {
                    operator: TupleOrdinalOperator::GreaterThan,
                    value: skip,
                    offset: None,
                }) if matches!(
                    bound,
                    TupleOrdinalClause {
                        operator: TupleOrdinalOperator::LessThan,
                        offset: None,
                        ..
                    }
                ) =>
                {
                    *consumer = Some(TupleOrdinalClause {
                        operator: TupleOrdinalOperator::LessThan,
                        value: bound.value,
                        offset: Some(*skip),
                    });
                    return self;
                }
                Some(_) => {}
            }
        }
        self.then(Step::authored(Continuation::Bound { bound }))
    }

    /// The head, for an authored walk that rewrites what it names.
    pub fn head_mut(&mut self) -> &mut Grelex<P> {
        &mut self.head
    }

    /// The steps, for an authored walk that rewrites payloads in place.
    pub fn continuations_mut(&mut self) -> &mut Vec<Step<P>> {
        &mut self.continuations
    }
}

impl<P: Phase> Continuation<P> {
    /// The scalar formals this continuation's row bound names (its count or
    /// its offset), whether the bound stands alone or in its ordering.
    pub fn bound_formals(&self) -> Vec<crate::pipeline::asts::core::definitions::FormalSelector> {
        let bound = match self {
            Continuation::Bound { bound } => Some(bound),
            Continuation::Structural(StructuralStep {
                form: StructuralForm::Ordering { bound, .. },
                ..
            }) => bound.as_ref(),
            _ => None,
        };
        bound.map(TupleOrdinalClause::formals).unwrap_or_default()
    }

    /// WHETHER THIS CONTINUATION PUBLISHES ITS OPERAND'S OWN RELATION, BY
    /// LAW.
    ///
    /// A restriction drops rows, a bound selects a prefix of an order, a
    /// correlation states an alignment — none creates an occurrence. An
    /// ordering re-orders rows and republishes its operand ONE-TO-ONE
    /// through the stage export: removing it removes only the re-ordering,
    /// and every position it published lands on its operand through the
    /// construction record.
    ///
    /// ONE ANSWER, and every road that moves or drops a step asks HERE —
    /// so "which steps may move" is decided once rather than re-listed at
    /// each pass.
    pub fn is_transparent(&self) -> bool {
        match self {
            Continuation::Restrict { .. }
            | Continuation::Bound { .. }
            | Continuation::Correlate { .. } => true,
            Continuation::Structural(step) => {
                matches!(step.form, StructuralForm::Ordering { .. })
            }
            // A correlated restriction publishes its operand's relation,
            // but attaching it recorded an obligation on that relation:
            // it is not a form a pass may drop or move, so the answer
            // every such road asks here is no.
            Continuation::Correlated(_) => false,
            Continuation::Access { .. }
            | Continuation::Member { .. }
            | Continuation::BagOp { .. }
            | Continuation::Destructure { .. }
            | Continuation::Pipe { .. }
            | Continuation::ErJoin(_) => false,
        }
    }
}

impl<P: Phase> Continuation<P> {
    /// THE OPERAND NESTED IN THIS STEP — a member's arm, a bag step's arm —
    /// as the relation it publishes in this phase: what the step's
    /// derivation consumed beside the prefix. `None` says the step nests
    /// no operand; a relation nested in a condition or a value is evaluated
    /// in place and enters no derivation, so it is not one.
    fn nested_operand(&self) -> Option<Option<crate::relation::SemanticRelation>> {
        match self {
            Continuation::Member { rhs, .. } => Some(P::into_scope(rhs.published())),
            Continuation::BagOp { arm, .. } => Some(P::into_scope(arm.published())),
            Continuation::Access { .. }
            | Continuation::Restrict { .. }
            | Continuation::Correlated(_)
            | Continuation::Bound { .. }
            | Continuation::Destructure { .. }
            | Continuation::Correlate { .. }
            | Continuation::Pipe { .. }
            | Continuation::ErJoin(_)
            | Continuation::Structural(_) => None,
        }
    }
}

impl<P: Phase> GroundForm<P> {
    /// THE BODY NESTED IN THIS HEAD — a derived table's subquery, a
    /// consulted expansion's body — as the relation it publishes in this
    /// phase: what the head's boundary was derived over. `None` says the
    /// head names something rather than containing it.
    fn nested_body(&self) -> Option<Option<crate::relation::SemanticRelation>> {
        match self {
            GroundForm::Reference(Relation::InnerRelation { pattern, .. }) => {
                Some(P::into_scope(pattern.subquery().published()))
            }
            GroundForm::Reference(Relation::ConsultedView { body, .. }) => {
                Some(P::into_scope(body.body.published()))
            }
            GroundForm::Reference(Relation::Ground { .. } | Relation::FunctorCall { .. })
            | GroundForm::Literal(_) => None,
        }
    }
}

/// WHETHER A STEP STANDS. See [`Chain::rebuilding`].
///
/// Two answers, and neither of them replaces anything: the walk that asks
/// holds no road to a payload or a result, so a step that stands publishes
/// exactly what it published.
pub enum Standing {
    /// The step stands, and the operand nested inside it is rebuilt.
    Keep,
    /// A transparent step comes off. Refused for any other form.
    Drop,
}

/// A CONTINUATION THAT PUBLISHES ITS OPERAND'S OWN RELATION, BY LAW.
///
/// Three, and the taxonomy is closed: a restriction drops rows, a bound
/// selects an arbitrary prefix (the ordered bound is the ordering's own
/// node, a stage), a correlation states an alignment. None creates an
/// occurrence, so none creates an interface — what such a step publishes
/// IS the relation standing to its left.
///
/// Nothing is constructed by attaching one, which is why moving one of
/// these onto a different operand is safe where moving any other step is
/// not — the result is RESTATED from the prefix it lands on, never carried
/// over from the prefix it came off. Attaching is still the authority's act
/// ([`Chain::transparently`] takes its token): a restriction READS, and
/// what it reads of the standing relation's obligations is spent by the
/// act that lands it, nowhere else.
///
/// The CORRELATED restriction is deliberately absent. It publishes its
/// operand's relation too, but attaching it records what that relation
/// owes, so it is an act of the relation authority and not a payload a
/// caller may put on a chain: [`Transparent::of`] refuses it, and the one
/// road that attaches one is [`Chain::correlated`].
pub enum Transparent<P: Phase = Unresolved> {
    Restrict {
        condition: TruthExpression<P>,
        origin: FilterOrigin,
    },
    Bound {
        bound: TupleOrdinalClause,
    },
    Correlate {
        whole: WholeHeading<P>,
    },
}

impl<P: Phase> Transparent<P> {
    /// The continuation this is, for the tree to store.
    pub fn into_form(self) -> Continuation<P> {
        match self {
            Transparent::Restrict { condition, origin } => {
                Continuation::Restrict { condition, origin }
            }
            Transparent::Bound { bound } => Continuation::Bound { bound },
            Transparent::Correlate { whole } => Continuation::Correlate { whole },
        }
    }

    /// The same, read back off a stored continuation. `None` says the
    /// continuation publishes a heading of its own, which is a refusal for
    /// whoever wanted to move it.
    pub fn of(form: Continuation<P>) -> std::result::Result<Self, Continuation<P>> {
        match form {
            Continuation::Restrict { condition, origin } => {
                Ok(Transparent::Restrict { condition, origin })
            }
            Continuation::Bound { bound } => Ok(Transparent::Bound { bound }),
            Continuation::Correlate { whole } => Ok(Transparent::Correlate { whole }),
            other => Err(other),
        }
    }
}

impl<P: Phase> Chain<P> {
    /// A chain that is nothing but its head.
    ///
    /// Safe in every phase: the head carries its own result, so there is no
    /// second thing here to pair it with.
    pub fn ground(head: Grelex<P>) -> Self {
        Chain {
            head,
            continuations: Vec::new(),
        }
    }

    /// What this chain publishes at its outermost node.
    pub(super) fn published(&self) -> P::Scope {
        match self.continuations.last() {
            Some(step) => step.result.clone(),
            None => self.head.result.clone(),
        }
    }

    /// THE ONE ADMISSION OF A STEP ONTO A PREFIX.
    ///
    /// Every road that puts a step on a chain — a landing, a fold, a
    /// crossing, a rebuild, the correlation act itself — comes through
    /// here, and the ACTUAL operand is what the step is judged against:
    /// what this chain publishes at its outermost node, never a label the
    /// step carried from wherever it came off. A correlated restriction
    /// carries its owner in its payload; if this chain does not publish
    /// that owner, the step is refused, and if it does, the step publishes
    /// it — restated from the prefix, exactly as a restriction's result is.
    /// So a closure that hands a road a different prefix, a walk that hands
    /// it another head, or a fold that lands a step it took off elsewhere
    /// cannot put a correlation over a relation its act did not record.
    fn admit(mut self, form: Continuation<P>, carried: P::Scope) -> crate::error::Result<Self> {
        let result = match &form {
            Continuation::Correlated(correlated) => {
                let operand = self.published();
                if !P::correlated_stands_on(correlated, &operand) {
                    return Err(Internal::invariant(
                        "chain",
                        "a correlated restriction was landed on a relation other than the one \
                         its correlation act recorded the obligation on",
                    ));
                }
                operand
            }
            // A form that publishes its operand's relation by law publishes
            // THIS operand's: restated, never carried.
            Continuation::Restrict { .. }
            | Continuation::Bound { .. }
            | Continuation::Correlate { .. } => self.published(),
            Continuation::Access { .. }
            | Continuation::Destructure { .. }
            | Continuation::Member { .. }
            | Continuation::BagOp { .. }
            | Continuation::Pipe { .. }
            | Continuation::ErJoin(_)
            | Continuation::Structural(_) => carried,
        };
        self.continuations.push(Step { form, result });
        Ok(self)
    }

    /// The ground expression this chain stands on.
    pub fn head(&self) -> &Grelex<P> {
        &self.head
    }

    /// The steps consuming it, in authored order.
    pub fn continuations(&self) -> &[Step<P>] {
        &self.continuations
    }

    /// The head and its steps, by value, for a walk that owns the tree.
    pub fn into_parts(self) -> (Grelex<P>, Vec<Step<P>>) {
        (self.head, self.continuations)
    }

    /// REBUILD THE OPERANDS STANDING INSIDE THIS CHAIN, keeping what every
    /// surviving node publishes.
    ///
    /// THE ONE payload-preserving rewrite, and what it reaches is exactly
    /// the relational operands NESTED inside the nodes — a member's right
    /// arm, a bag step's arm, a derived table's subquery. It is handed the
    /// operand and nothing else, so there is no borrow here through which a
    /// payload could be replaced while the relation it publishes stays.
    /// Every surviving node keeps the result it already had because nothing
    /// could have moved what it means.
    ///
    /// The one structural move is [`Standing::Drop`], and it refuses a step
    /// that publishes a heading of its own: dropping one of those would
    /// leave every later node claiming a relation built over an operand
    /// that is no longer there.
    pub fn rebuilding(
        self,
        mut nested: impl FnMut(Chain<P>) -> crate::error::Result<Chain<P>>,
        mut standing: impl FnMut(usize, &Continuation<P>) -> crate::error::Result<Standing>,
    ) -> crate::error::Result<Self> {
        let Chain {
            head,
            continuations,
        } = self;
        let head = head.rebuilding_nested(&mut nested)?;
        let mut chain = Chain::ground(head);
        for (at, step) in continuations.into_iter().enumerate() {
            match standing(at, &step.form)? {
                Standing::Keep => {
                    let Step { form, result } = step.rebuilding_arm(&mut nested)?;
                    chain = chain.admit(form, result)?;
                }
                Standing::Drop if step.form.is_transparent() => {}
                Standing::Drop => {
                    return Err(Internal::invariant(
                        "chain",
                        "a step publishing a heading of its own cannot be dropped from a chain",
                    ))
                }
            }
        }
        Ok(chain)
    }

    /// THE CORRELATION ON ONE BAG STEP, for the pass that decides which
    /// earlier arm an operand is filtered by.
    ///
    /// A correlation constrains the PAIRING, not the heading: the arms
    /// contribute exactly what they contributed, so the step publishes what
    /// it published and this borrow cannot move it. `None` says the step at
    /// that position is not a bag operation, which is a refusal for whoever
    /// wanted to correlate it.
    pub fn bag_correlation_at(&mut self, at: usize) -> Option<&mut P::Corr> {
        match &mut self.continuations.get_mut(at)?.form {
            Continuation::BagOp { correlation, .. } => Some(correlation),
            _ => None,
        }
    }

    /// Which set operation the step at that position is. `None` says it is
    /// not a set operation at all.
    pub fn bag_operator_at(&self, at: usize) -> Option<SetOperator> {
        match &self.continuations.get(at)?.form {
            Continuation::BagOp { operator, .. } => Some(*operator),
            _ => None,
        }
    }

    /// TAKE ONE TRANSPARENT STEP OUT OF THE MIDDLE.
    ///
    /// A rewrite lifts a bound or an ordering out to re-express it as a
    /// window: what stood above it publishes exactly what it published,
    /// because the step it stood on created no occurrence. A step that
    /// publishes a heading of its own REFUSES — taking that one out would
    /// leave every later node claiming a relation built over an operand
    /// that is no longer there.
    pub fn without(mut self, at: usize) -> crate::error::Result<Self> {
        if !self.continuations[at].form.is_transparent() {
            return Err(Internal::invariant(
                "chain",
                "a step publishing a heading of its own cannot be taken out of a chain",
            ));
        }
        self.continuations.remove(at);
        Ok(self)
    }

    /// AN ORDERING SURRENDERS ITS MEMBERSHIP ACT. The window rewrite performs
    /// the act by ranking instead: the rank orders by the ordering's keys and
    /// keeps the interval its bound spells, so both come off the node
    /// together — an ordering left standing without its bound would be one
    /// nothing consumes. The node stays only as the stage its export
    /// published, because everything above it stands on the ports that
    /// export minted, and it orders nothing. The relation the node publishes
    /// is unchanged: neither the keys nor the bound were part of the
    /// derivation, only the row-bounded fact stamped on its result. Answers
    /// `None` for any step that is not an ordering carrying a bound, and
    /// changes nothing then.
    pub fn surrender_membership(
        &mut self,
        at: usize,
    ) -> Option<(
        Vec<super::super::specs::OrderingSpec<P>>,
        TupleOrdinalClause,
    )> {
        match &mut self.continuations[at].form {
            Continuation::Structural(StructuralStep {
                form: StructuralForm::Ordering { specs, bound },
                ..
            }) => {
                let bound = bound.take()?;
                Some((std::mem::take(specs), bound))
            }
            _ => None,
        }
    }

    /// THE OPERAND A STEP CONSUMES: this chain's head and its first `at`
    /// steps.
    ///
    /// A PREFIX OF ONE CHAIN. Every node in it publishes exactly what it
    /// published, because it is the same node standing on the same
    /// operand; nothing here relates two chains.
    pub fn prefix(&self, at: usize) -> Self {
        Chain {
            head: self.head.clone(),
            continuations: self.continuations[..at].to_vec(),
        }
    }

    /// CUT THE CHAIN BACK TO ITS FIRST `at` STEPS.
    ///
    /// Dropping a SUFFIX touches nothing that survives: every remaining
    /// node stands on the same operand it stood on and publishes what it
    /// published.
    pub fn truncated(mut self, at: usize) -> Self {
        self.continuations.truncate(at);
        self
    }

    /// TAKE A TRAILING SUFFIX OFF, deciding step by step where it stops.
    ///
    /// `admit` says whether a step belongs to the suffix; the first
    /// refusal ends it and that step stays where it is. The head's own
    /// access is never taken.
    pub fn peel_while(mut self, mut admit: impl FnMut(&Continuation<P>) -> bool) -> Run<P> {
        let mut steps = Vec::new();
        while self.has_steps()
            && self
                .continuations
                .last()
                .is_some_and(|step| admit(&step.form))
        {
            steps.push(self.continuations.pop().expect("just matched a step"));
        }
        steps.reverse();
        Run {
            prefix: self,
            steps,
        }
    }

    /// TAKE THE TRAILING RUN OFF, with the operand it shapes.
    ///
    /// `Err` hands the chain back untouched: there is no run. The
    /// partition is [`Chain::pop_run_step`]'s, so membership is decided in
    /// one place and this cannot disagree with it.
    pub fn peel_run(mut self) -> std::result::Result<Run<P>, Chain<P>> {
        let mut steps = Vec::new();
        while let Some(step) = self.pop_run_step() {
            steps.push(step.into_step());
        }
        if steps.is_empty() {
            return Err(self);
        }
        steps.reverse();
        Ok(Run {
            prefix: self,
            steps,
        })
    }

    /// TAKE THE TRAILING TRANSPARENT STEPS OFF, as the forms they are.
    ///
    /// No step comes out — only the payloads — so what a caller holds
    /// cannot be put back carrying an old result. Putting them back is
    /// [`Chain::transparently`], which restates the relation from the
    /// prefix they land on. A step past `from` that publishes a heading of
    /// its own REFUSES: it is not a form this road may move.
    pub fn split_transparent_tail(
        &mut self,
        from: usize,
    ) -> crate::error::Result<Vec<Transparent<P>>> {
        self.continuations
            .split_off(from)
            .into_iter()
            .map(|step| {
                Transparent::of(step.form).map_err(|_| {
                    Internal::invariant(
                        "chain",
                        "a step publishing a heading of its own cannot be lifted off a chain",
                    )
                })
            })
            .collect()
    }

    /// SPLIT THE TRAILING BAG RUN OFF, as the steps it is made of.
    ///
    /// What is left is arm 0 — the chain the run stands on. The run is
    /// named by a [`BagRun`], which only [`Chain::trailing_bag_run`]
    /// produces, so this cuts where the partition says and nowhere else.
    /// Nothing goes back on: a run lowers as ONE operation over its arms.
    pub fn split_run(&mut self, run: BagRun) -> Vec<Step<P>> {
        self.continuations.split_off(run.base)
    }

    /// TAKE THE CHAIN APART AT ITS OUTERMOST STEP.
    ///
    /// The operand and the step come back as ONE value, so a walk that
    /// looks at what the outermost step does cannot end up holding the step
    /// beside somebody else's chain. `Err` hands the chain back untouched:
    /// nothing stands on the head's own read.
    pub fn peel(mut self) -> std::result::Result<Peel<P>, Chain<P>> {
        match self.pop_step() {
            Some(last) => Ok(Peel { prefix: self, last }),
            None => Err(self),
        }
    }

    /// PUT BACK STEPS THIS CHAIN GAVE UP.
    ///
    /// The exact inverse of taking them off, and the ONLY road that
    /// appends a bound step without deriving one. It is not an attachment
    /// road: what goes back on is what came off, in order, over the prefix
    /// it came off — nothing here pairs a step with a relation it did not
    /// already publish.
    ///
    /// Visible to the AST carriers alone, so the compression a
    /// `ScalarizedRelation` lifts out can be put back where it was and
    /// nowhere else.
    pub(super) fn rejoin(mut self, taken: Vec<Step<P>>) -> Self {
        self.continuations.extend(taken);
        self
    }

    /// The other half of that inverse: the steps, mutably, for the carrier
    /// that lifts its own compression off. Same narrow visibility.
    pub(super) fn steps_mut(&mut self) -> &mut Vec<Step<P>> {
        &mut self.continuations
    }

    /// CROSS A PHASE BOUNDARY, or survive a same-phase rewrite.
    ///
    /// Each node goes through its OWN fold — the head through the head's,
    /// each step through the step's — and what every node PUBLISHES goes
    /// through the phases' own door. There is no argument here for a
    /// relation and no reassembly from loose parts, so a walk cannot land
    /// one node's payload on another node's result.
    ///
    /// EVERY NODE IS CROSSED BY THE CARRIER, never handed back by the walk,
    /// AND EVERY IDENTITY IS THE PHASES' TO CARRY, never the walk's to
    /// answer. The head crosses through [`Grelex::folded`] and each step
    /// through [`Step::folded`]: a node's PAYLOAD goes through the walk's
    /// payload hooks, and what the node PUBLISHES goes through
    /// [`crate::pipeline::asts::core::phases::carry_scope`] — the walk holds
    /// no method that answers with a head, a step, or the relation either
    /// publishes, so a walk cannot put another relation's identity under a
    /// correlation, at the head or at any step of its prefix. A correlated
    /// restriction crosses through the phases' one correlation door and is
    /// never offered to the continuation hook, so no hook can answer with
    /// another kind in its place. Every crossed step is then ADMITTED onto
    /// the chain built so far, against what that chain actually publishes.
    #[stacksafe::stacksafe]
    pub fn folded<Q: Phase, F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
    ) -> crate::error::Result<Chain<Q>> {
        let Chain {
            head,
            continuations,
        } = self;
        let mut chain = Chain::ground(head.folded(walk)?);
        for step in continuations {
            let Step { form, result } = step.folded(walk)?;
            chain = chain.admit(form, result)?;
        }
        Ok(chain)
    }

    /// The forms of this chain's steps past the head's own read, borrowed
    /// for a pattern match over the run.
    pub fn step_forms(&self) -> Vec<&Continuation<P>> {
        self.steps().iter().map(Step::form).collect()
    }

    /// The forms of this chain's steps, for a walk that reads what happened
    /// and not what each step publishes.
    pub fn forms(&self) -> impl DoubleEndedIterator<Item = &Continuation<P>> {
        self.continuations.iter().map(Step::form)
    }

    /// The trailing run of bag steps this chain ends in, when they carry one
    /// operator and that operator accumulates its arms' rows.
    ///
    /// The run is the unit an [`ArmIx`] counts in: arm 0 is the chain the
    /// run stands on and arm `k` is the `k`th step's arm, so a correlation
    /// says exactly which two arms it constrains. Minus never joins a run —
    /// its arm contributes no rows, so `a - b - c` is two subtractions, not
    /// a three-arm operation. Both the refiner that writes an `ArmIx` and
    /// the lowering that reads one ask HERE, so the two cannot disagree
    /// about which arm a number names.
    pub fn trailing_bag_run(&self) -> Option<BagRun> {
        let operator = match self.continuations.last()?.form() {
            Continuation::BagOp { operator, .. } => *operator,
            _ => return None,
        };
        let mut base = self.continuations.len() - 1;
        if operator.accumulates_arm_rows() {
            while base > 0 {
                match self.continuations[base - 1].form() {
                    Continuation::BagOp {
                        operator: earlier, ..
                    } if *earlier == operator => base -= 1,
                    _ => break,
                }
            }
        }
        Some(BagRun {
            operator,
            steps: self.continuations.len() - base,
            base,
        })
    }

    /// Whether this chain stands on a bag step — the step itself, or
    /// predicates standing directly on one.
    ///
    /// Such a chain belongs to the bag road whole. Its arms are relations
    /// in their own right and a predicate above them may be the correlation
    /// naming two of them, so pooling its parts into a flat table-and-
    /// predicate segment is what turned "which arms does this relate" into
    /// a classification guess.
    pub fn stands_on_bag_step(&self) -> bool {
        self.forms()
            .rev()
            .find_map(|continuation| match continuation {
                Continuation::BagOp { .. } => Some(true),
                // A predicate and a correlation both stand ON the step
                // below: they are what `claim_bag_correlations` reads, so
                // the chain they stand on is still the bag's. A correlated
                // restriction stands on it the same way.
                Continuation::Restrict { .. }
                | Continuation::Correlated(_)
                | Continuation::Correlate { .. } => None,
                Continuation::Access { .. }
                | Continuation::Bound { .. }
                | Continuation::Destructure { .. }
                | Continuation::Member { .. }
                | Continuation::Pipe { .. }
                | Continuation::Structural(_)
                | Continuation::ErJoin(_) => Some(false),
            })
            .unwrap_or(false)
    }

    /// The access the HEAD's own read consumes, when the head has a read to
    /// parameterize and the chain begins with one.
    ///
    /// THE READING RULE, in one place. `users(*)`, `users()*` and
    /// `f(x)(*)` all normalize to an access at index 0 over a mention, and
    /// that access says what the read asks for — it is not a step on the
    /// read's result. An anonymous table, a derived table and a consulted
    /// expansion have no read to parameterize, so an access standing after
    /// one is an ordinary step; so is every access past index 0
    /// (`users(a, b, _).(a)` is a caller pattern and THEN a USING step).
    pub fn head_access(&self) -> Option<&super::access::Access<P>> {
        if !self.head_takes_an_access() {
            return None;
        }
        match self.continuations.first().map(Step::form) {
            Some(Continuation::Access { access, .. }) => Some(access),
            _ => None,
        }
    }

    /// Whether the head is a MENTION — the two relational forms whose read a
    /// leading access parameterizes.
    pub fn head_takes_an_access(&self) -> bool {
        match self.head.form() {
            GroundForm::Reference(relation) => relation.takes_an_access(),
            GroundForm::Literal(_) => false,
        }
    }

    /// How many leading continuations belong to the head's own read: one
    /// when it carries an access, zero otherwise. The boundary every walk
    /// that separates "the read" from "steps on the read" reads.
    pub fn head_span(&self) -> usize {
        usize::from(self.head_access().is_some())
    }

    /// The continuations that consume the relation the head's READ publishes
    /// — everything past the head's own access.
    ///
    /// An outside-in walk reads these; the head and its access are the walk's
    /// base, because separating a mention from what its parens asked for
    /// leaves a read nobody parameterized.
    pub fn steps(&self) -> &[Step<P>] {
        &self.continuations[self.head_span()..]
    }

    /// Whether anything consumes the relation the head's read publishes.
    pub fn has_steps(&self) -> bool {
        self.continuations.len() > self.head_span()
    }

    /// The outermost step, taken off. `None` leaves the chain at its base:
    /// the head and the access its own read consumes.
    pub fn pop_step(&mut self) -> Option<Step<P>> {
        self.has_steps().then(|| {
            self.continuations
                .pop()
                .expect("a chain with steps has a last continuation")
        })
    }

    /// Take the outermost CONTINUATION off — the head's own access
    /// included.
    ///
    /// [`Chain::pop_step`] stops at the read the chain stands on; this
    /// does not. A mutation terminal's RECEIPT is the head's own access,
    /// so the road that takes a terminal apart has to be able to reach it.
    pub fn pop_continuation(&mut self) -> Option<Step<P>> {
        self.continuations.pop()
    }

    /// THE ONE RUN PARTITION, exercised in place: take the chain's outermost
    /// step off WHEN it is a step of the trailing run — a pipe stage, a
    /// dimension access, or a structural step — as the exact typed payload a
    /// consumer matches exhaustively. A trailing nonmember (or no step at
    /// all) leaves the chain UNCHANGED and answers `None`, so membership and
    /// payload are one answer, no walk keeps a boolean membership list
    /// beside this operation, and none ends in a reachable panic.
    /// Exhaustive on both sides: a new continuation variant must decide its
    /// membership here, at compile time. A structural step moves WHOLE —
    /// the exact typed family, never the broad continuation.
    pub fn pop_run_step(&mut self) -> Option<RunStep<P>> {
        let step = self.pop_step()?;
        let Step { form, result } = step;
        match form {
            Continuation::Pipe { operator, named } => Some(RunStep {
                form: RunForm::Pipe { operator, named },
                result,
            }),
            Continuation::Access { access, named } => Some(RunStep {
                form: RunForm::Access { access, named },
                result,
            }),
            Continuation::Structural(structural) => Some(RunStep {
                form: RunForm::Structural(structural),
                result,
            }),
            other @ (Continuation::Restrict { .. }
            | Continuation::Correlated(_)
            | Continuation::Correlate { .. }
            | Continuation::Bound { .. }
            | Continuation::Destructure { .. }
            | Continuation::Member { .. }
            | Continuation::BagOp { .. }
            | Continuation::ErJoin(_)) => {
                self.continuations.push(Step {
                    form: other,
                    result,
                });
                None
            }
        }
    }

    /// The chain's READ — the head with the access its own parens asked for
    /// — and the steps that consume what the read publishes.
    ///
    /// The two travel together by construction, so a walk that takes a chain
    /// apart cannot leave a mention holding no access or an access holding
    /// no mention.
    pub fn split_read(mut self) -> (Chain<P>, Vec<Step<P>>) {
        let steps = self.continuations.split_off(self.head_span());
        (self, steps)
    }

    /// The head, its own access, and the steps that consume the relation it
    /// publishes — by value, for a walk that owns what it rewrites.
    #[allow(clippy::type_complexity)]
    pub fn split_head_access(
        mut self,
    ) -> (Grelex<P>, Option<super::access::Access<P>>, Vec<Step<P>>) {
        // THE READ'S OWN ACCESS CARRIES NO NAME: the name of a read is the
        // mention's, so the leading access is taken as the bare shape it
        // is, and a name standing on it would be a normalizer defect.
        let access = match self.head_span() {
            1 => match self.continuations.remove(0).into_form() {
                Continuation::Access { access, .. } => Some(access),
                _ => unreachable!("head_span counted an access"),
            },
            _ => None,
        };
        (self.head, access, self.continuations)
    }

    /// The relation this chain READS: its head, when nothing past the head's
    /// own access has consumed it.
    ///
    /// The question `as_bare_relation` used to answer before an access was a
    /// continuation. A caller that also needs what the read asked for takes
    /// [`Chain::head_access`] beside this.
    pub fn as_read_relation(&self) -> Option<&Relation<P>> {
        match (self.head.form(), self.has_steps()) {
            (GroundForm::Reference(relation), false) => Some(relation),
            _ => None,
        }
    }

    /// Whether this chain HEADS A CALL: its head reads a functor call,
    /// whatever follows it. A join frames its right member by this — a
    /// call at the head takes the row to its left as its caller row
    /// whether or not restrictions or members follow the call inside the
    /// same operand.
    pub fn heads_a_call(&self) -> bool {
        matches!(
            self.head.form(),
            GroundForm::Reference(Relation::FunctorCall { .. })
        )
    }

    /// The relation this chain names when nothing has consumed it yet.
    pub fn as_bare_relation(&self) -> Option<&Relation<P>> {
        match (self.head.form(), self.continuations.is_empty()) {
            (GroundForm::Reference(relation), true) => Some(relation),
            _ => None,
        }
    }

    /// The head, by value, when nothing has consumed it.
    pub fn into_bare_head(self) -> Option<Grelex<P>> {
        self.continuations.is_empty().then_some(self.head)
    }

    /// The last continuation, and the chain that produces its operand.
    /// This is the fold read backwards — the shape a consumer needs when it
    /// asks what produced the relation it is looking at.
    pub fn split_last(&self) -> Option<(&Step<P>, ChainPrefix<'_, P>)> {
        let (last, rest) = self.continuations.split_last()?;
        Some((
            last,
            ChainPrefix {
                head: &self.head,
                continuations: rest,
            },
        ))
    }
}

/// ONE STEP OF A CHAIN: what it does, and the relation it publishes.
///
/// PRIVATE FIELDS, and no setter. Before resolution there is nothing to
/// pair — the phase has no relation — so [`Step::authored`] builds one from
/// the form alone. After it the only road is the semantic authority's, and
/// that road derives the relation from THIS form in one act: a caller never
/// holds a loose relation, so it has nothing to attach to another node and
/// nothing to swap in afterwards.
///
/// The result lives HERE rather than in each continuation variant because
/// an enum variant's fields are as public as the enum: a `result` field on
/// `Continuation::Pipe` is a construction road no visibility can close.
#[derive(Debug, PartialEq, ToLispy)]
#[lispy("step")]
pub struct Step<P: Phase = Unresolved> {
    #[lispy("form")]
    form: Continuation<P>,
    #[lispy("result")]
    result: P::Scope,
}

impl<P: Phase> Step<P> {
    /// What this step does.
    pub fn form(&self) -> &Continuation<P> {
        &self.form
    }

    pub fn into_form(self) -> Continuation<P> {
        self.form
    }

    /// What this step publishes.
    pub fn result(&self) -> &P::Scope {
        &self.result
    }
}

impl<P: Phase<Scope = ()>> Step<P> {
    /// The authored step. Nothing is paired here — the phase has no
    /// relation to pair — so the form alone builds it.
    pub fn authored(form: Continuation<P>) -> Self {
        Step { form, result: () }
    }

    /// The payload, for an authored walk that rewrites in place.
    ///
    /// AUTHORED ONLY. A bound step's payload is what its relation was
    /// derived FROM, so a road that swaps the payload while the result
    /// stays is the mismatch this cut exists to remove: the bound phase
    /// takes the form off and lands the new one through the authority,
    /// which derives what the step publishes over the prefix it is landing
    /// on.
    pub fn form_mut(&mut self) -> &mut Continuation<P> {
        &mut self.form
    }

    /// Rewrite an authored payload by value.
    pub fn rewrite_form(
        self,
        rewrite: impl FnOnce(Continuation<P>) -> crate::error::Result<Continuation<P>>,
    ) -> crate::error::Result<Self> {
        Ok(Step {
            form: rewrite(self.form)?,
            result: (),
        })
    }
}

impl<P: Phase> Step<P> {
    /// REBUILD THE RELATIONAL ARM STANDING INSIDE THIS STEP.
    ///
    /// A member's right arm and a bag step's arm are the only chains a step
    /// holds. The rewrite is handed the ARM and answers with an arm; the
    /// step's own operation is rebuilt around it HERE, so there is nothing
    /// a caller could hand back that changes what the step means. A join
    /// still concatenates two headings and a set still merges its arms
    /// through the matrix it was built with, and the step keeps the result
    /// it has because nothing could have moved it.
    ///
    /// A step holding no arm stands unchanged. A change that DOES move what
    /// a step publishes goes through the authority, which derives the
    /// result over the operand the step lands on. THE ARM KEEPS ITS
    /// IDENTITY: the rewrite hands back a chain publishing the relation it
    /// was handed, or is refused — see [`rebuilt_in_place`].
    pub fn rebuilding_arm(
        self,
        arm: impl FnOnce(Chain<P>) -> crate::error::Result<Chain<P>>,
    ) -> crate::error::Result<Self> {
        let Step { form, result } = self;
        let form = match form {
            Continuation::Member {
                rhs,
                correlation,
                join,
            } => Continuation::Member {
                rhs: rebuilt_in_place(rhs, arm)?,
                correlation,
                join,
            },
            Continuation::BagOp {
                operator,
                arm: standing,
                correlation,
            } => Continuation::BagOp {
                operator,
                arm: rebuilt_in_place(standing, arm)?,
                correlation,
            },
            held @ (Continuation::Access { .. }
            | Continuation::Restrict { .. }
            | Continuation::Correlated(_)
            | Continuation::Bound { .. }
            | Continuation::Destructure { .. }
            | Continuation::Correlate { .. }
            | Continuation::Pipe { .. }
            | Continuation::ErJoin(_)
            | Continuation::Structural(_)) => held,
        };
        Ok(Step { form, result })
    }

    /// Cross a phase boundary, or survive a same-phase rewrite.
    ///
    /// The new form is this step's own, transformed by the walk; what it
    /// publishes goes through the phases' own door, which carries the
    /// identity as itself and refuses where no relation can be carried.
    /// There is no argument here for a relation and the walk is not asked
    /// for one, so a fold cannot be the place a step acquires a different
    /// result.
    pub fn folded<Q: Phase, F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
    ) -> crate::error::Result<Step<Q>> {
        let Step { form, result } = self;
        let result = crate::pipeline::asts::core::phases::carry_scope::<P, Q>(result)?;
        // THE FORM IS THIS STEP'S OWN, crossed by the walk function that
        // rebuilds the operation it was handed around what it holds: the
        // walk has no hook that answers with a continuation, so a step's
        // structure — its kind, its operator, its items, its ordering, its
        // pattern — is the carrier's through a fold, and the walk answers
        // for the leaves inside it and for the subtree it nests. A
        // correlated restriction crosses by the phases' one correlation
        // door, its condition re-read.
        let operand = form.nested_operand();
        let form = crossed_form(walk, form)?;
        // AND THE SUBTREE COMES BACK AS THE OPERAND THE RESULT WAS DERIVED
        // OVER: the subtree hook may rebuild an arm, never replace it, so a
        // hook that answers a member with another arm has paired this
        // step's identity with a derivation it never had, and is refused
        // here rather than trusted.
        if form.nested_operand() != operand {
            return Err(Internal::invariant(
                "chain",
                "a fold handed a step's arm back publishing a relation other than the \
                 operand the step was derived over",
            ));
        }
        Ok(Step { form, result })
    }
}

/// A REBUILD HANDS BACK WHAT IT WAS HANDED.
///
/// Every rebuild road hands its rewrite an OPERAND — a prefix, a member's
/// arm, a bag step's arm, a derived table's subquery — and the node above
/// keeps the result it has, because nothing could have moved what it means.
/// That is true only while the operand comes back publishing the relation
/// it was handed: a rebuild may seal an operand, reorder what it holds, or
/// take a transparent step off it, and each of those republishes the same
/// relation; a chain publishing another relation is not a rebuild of this
/// operand, and the node above was derived over no such thing. One check,
/// asked at every operand position, so no road keeps a node's identity
/// over an operand somebody swapped.
fn rebuilt_in_place<P: Phase>(
    operand: Chain<P>,
    rebuild: impl FnOnce(Chain<P>) -> crate::error::Result<Chain<P>>,
) -> crate::error::Result<Chain<P>> {
    let was = operand.published();
    let now = rebuild(operand)?;
    if now.published() != was {
        return Err(Internal::invariant(
            "chain",
            "a rebuild handed back a chain publishing a relation other than the operand it \
             was handed",
        ));
    }
    Ok(now)
}

/// CROSS ONE STEP'S FORM: a correlated restriction by its own door — the
/// condition re-spelled by the walk inside the value the act minted, its
/// occurrences re-read, its owner kept — and every other form by the walk
/// function that rebuilds it around its leaves. The carrier calls this; a
/// walk is never handed a continuation to answer with.
fn crossed_form<
    P: Phase,
    Q: Phase,
    F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized,
>(
    walk: &mut F,
    form: Continuation<P>,
) -> crate::error::Result<Continuation<Q>> {
    match form {
        Continuation::Correlated(correlated) => Ok(Continuation::Correlated(
            crate::pipeline::asts::core::phases::carry_correlated::<P, Q>(
                correlated,
                |condition| walk.transform_boolean(condition),
            )?,
        )),
        other => crate::pipeline::ast_transform::walk_transform_continuation(walk, other),
    }
}

// Manual Clone for the same reason `Chain`'s is manual.
impl<P: Phase> Clone for Step<P> {
    #[stacksafe::stacksafe]
    fn clone(&self) -> Self {
        Step {
            form: self.form.clone(),
            result: self.result.clone(),
        }
    }
}

/// A CHAIN TAKEN APART AT ITS OUTERMOST STEP: the operand, and the step
/// that consumes it.
///
/// ONE value. There is no accessor that hands the step out beside the
/// prefix, so a walk cannot carry a bound step to a chain it was not
/// derived over — the two roads out are putting it back exactly
/// ([`Peel::rejoin`]) and taking it apart into halves that carry no
/// pairing ([`Peel::take_apart`]).
pub struct Peel<P: Phase> {
    prefix: Chain<P>,
    last: Step<P>,
}

impl<P: Phase> Peel<P> {
    /// The operand the outermost step consumes.
    pub fn prefix(&self) -> &Chain<P> {
        &self.prefix
    }

    /// What the outermost step does, and what it publishes.
    pub fn last(&self) -> &Step<P> {
        &self.last
    }

    /// Put it back, exactly. Not an attachment: what goes on is what came
    /// off, over the prefix it came off.
    pub fn rejoin(mut self) -> Chain<P> {
        self.prefix.continuations.push(self.last);
        self.prefix
    }

    /// The operand, and the NODE that consumes it — two values, and the
    /// step is still one of them. A step handed out here carries what it
    /// publishes with it, so there is no loose relation to pair with
    /// another payload; a consumer reads the pair and a rebuild goes
    /// through the authority.
    pub fn split(self) -> (Chain<P>, Step<P>) {
        (self.prefix, self.last)
    }

    /// THE OPERAND AND A TRANSPARENT STEP'S OWN PAYLOAD.
    ///
    /// A restriction, a bound and a correlation publish their operand's
    /// relation by law, so what comes off here is the payload ALONE — there
    /// is no relation to hand back, and the road that puts one on
    /// ([`Chain::transparently`]) restates the result from the prefix it
    /// lands on. `Err` says the step publishes a heading of its own, which
    /// is a refusal for whoever wanted to move it.
    #[allow(clippy::result_large_err)]
    pub fn transparent(self) -> std::result::Result<(Chain<P>, Transparent<P>), Self> {
        match Transparent::of(self.last.form) {
            Ok(payload) => Ok((self.prefix, payload)),
            Err(form) => Err(Peel {
                prefix: self.prefix,
                last: Step {
                    form,
                    result: self.last.result,
                },
            }),
        }
    }

    /// REBUILD THE OPERAND, AND THE ARM STANDING INSIDE THE STEP.
    ///
    /// A member's right arm and a bag step's arm are operands standing
    /// INSIDE one node. The rebuild is handed each operand ALONE — the
    /// step is rebuilt around its arm rather than replaced — so the join still
    /// concatenates two headings and the set still merges its arms through
    /// the matrix it was built with, and the step keeps the result it had
    /// because nothing could have moved it. The two halves land back at the
    /// node they occupied and neither is ever loose.
    pub fn rebuilding_arms(
        self,
        prefix: impl FnOnce(Chain<P>) -> crate::error::Result<Chain<P>>,
        arm: impl FnOnce(Chain<P>) -> crate::error::Result<Chain<P>>,
    ) -> crate::error::Result<Chain<P>> {
        let Peel {
            prefix: operand,
            last,
        } = self;
        let Step { form, result } = last.rebuilding_arm(arm)?;
        // THE OPERAND KEEPS ITS IDENTITY, at the prefix exactly as at the
        // arm: see `rebuilt_in_place`.
        let landed = rebuilt_in_place(operand, prefix)?;
        landed.admit(form, result)
    }
}

/// A CHAIN TAKEN APART AT ITS TRAILING RUN: the operand, and the run's
/// steps in authored order.
///
/// ONE value, for the reason [`Peel`] is one. The run is the pipe stages,
/// dimension accesses and structural steps standing at the end — the
/// partition [`Chain::pop_run_step`] states — and it goes back on in the
/// order it came off.
pub struct Run<P: Phase> {
    prefix: Chain<P>,
    steps: Vec<Step<P>>,
}

impl<P: Phase> Run<P> {
    /// The relation the run shapes.
    pub fn prefix(&self) -> &Chain<P> {
        &self.prefix
    }

    /// The run's steps, innermost first.
    pub fn steps(&self) -> &[Step<P>] {
        &self.steps
    }

    /// The run's steps, by value, for a pass that lands each one back
    /// through [`Chain::relanded`].
    pub fn into_parts(self) -> (Chain<P>, Vec<Step<P>>) {
        (self.prefix, self.steps)
    }

    /// Put the run back, exactly.
    pub fn rejoin(mut self) -> Chain<P> {
        self.prefix.continuations.extend(self.steps);
        self.prefix
    }

}

/// Where a chain's trailing bag run starts and how many steps it has.
///
/// `base` indexes the continuation the run begins at, so
/// `continuations[..base]` is arm 0's own chain and `continuations[base + k]`
/// is the step owning arm `k + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BagRun {
    pub operator: SetOperator,
    pub steps: usize,
    pub base: usize,
}

impl BagRun {
    /// The number of operands the run combines, arm 0 included.
    pub fn arms(&self) -> usize {
        self.steps + 1
    }
}

/// One shaping continuation seen while reading a chain from the outside in.
///
/// The walk EXPOSES each restriction's condition and each pipe's operator to
/// the caller but never recurses into them — reading a condition's subquery
/// as if it were part of the chain answers the wrong question (wrong base
/// relation, wrong operator, wrong connection).
pub enum SpineStep<'a, P: Phase> {
    Restrict(&'a TruthExpression<P>),
    /// The whole-heading correlation standing in the comma position.
    Correlate(&'a WholeHeading<P>),
    Bound(&'a TupleOrdinalClause),
    Destructure,
    Pipe(&'a PipeOp<P>),
    Access(&'a super::access::Access<P>),
    Structural(&'a StructuralForm<P>),
}

impl<'a, P: Phase> SpineStep<'a, P> {
    /// The row bound this step carries, if it bounds: the arbitrary bound's
    /// own clause, or the one an ordering consumed. ONE ANSWER for both
    /// spellings, so a reader asking "is the run bounded here" cannot see
    /// the bare bound and miss the ordered one.
    pub fn bound(&self) -> Option<&'a TupleOrdinalClause> {
        match self {
            SpineStep::Bound(bound) => Some(bound),
            SpineStep::Structural(StructuralForm::Ordering {
                bound: Some(bound), ..
            }) => Some(bound),
            SpineStep::Structural(_)
            | SpineStep::Restrict(_)
            | SpineStep::Correlate(_)
            | SpineStep::Destructure
            | SpineStep::Pipe(_)
            | SpineStep::Access(_) => None,
        }
    }
}

/// Reads a chain's shaping continuations, outermost first, and stops at the
/// first continuation that brings another relation in (a member, a bag arm,
/// an ER edge).
pub struct SourceSpine<'a, P: Phase> {
    rest: &'a [Step<P>],
}

impl<'a, P: Phase> Iterator for SourceSpine<'a, P> {
    type Item = SpineStep<'a, P>;

    fn next(&mut self) -> Option<Self::Item> {
        let (last, rest) = self.rest.split_last()?;
        let step = match last.form() {
            Continuation::Access { access, .. } => SpineStep::Access(access),
            Continuation::Restrict { condition, .. } => SpineStep::Restrict(condition),
            // The spine reads what SHAPES the relation, and a correlated
            // restriction shapes it as a restriction does: rows drop, the
            // heading stands.
            Continuation::Correlated(correlated) => {
                SpineStep::Restrict(P::correlated(correlated).condition())
            }
            Continuation::Correlate { whole, .. } => SpineStep::Correlate(whole),
            Continuation::Bound { bound, .. } => SpineStep::Bound(bound),
            Continuation::Destructure { .. } => SpineStep::Destructure,
            Continuation::Pipe { operator, .. } => SpineStep::Pipe(operator),
            Continuation::Structural(step) => SpineStep::Structural(&step.form),
            // A continuation that brings another relation is the boundary:
            // the walk stops AT it, so nothing recursive is silently skipped.
            Continuation::Member { .. } | Continuation::BagOp { .. } | Continuation::ErJoin(_) => {
                return None
            }
        };
        self.rest = rest;
        Some(step)
    }
}

impl<P: Phase> Chain<P> {
    /// Read the shaping continuations from the outside in. See [`SourceSpine`].
    ///
    /// Pinned by `source_spine_reads_restrictions_and_pipes_outermost_first`,
    /// `source_spine_stops_at_a_member_without_entering_either_relation`, and
    /// `source_spine_stops_at_a_bag_operation_and_at_an_edge`.
    pub fn source_spine(&self) -> SourceSpine<'_, P> {
        SourceSpine {
            rest: &self.continuations,
        }
    }
}

impl<P: Phase> Chain<P> {
    /// Fold over the ENDING of a chain, per the effect-algebra ledger tail
    /// (EFFECT-ALGEBRA §3/§10): a member ends where its right-hand chain
    /// ends, a bag operation ends in `set_fold` over every arm's ending —
    /// the left operand first — and anything else is handed WHOLESALE to
    /// `leaf`, so no field is silently dropped.
    ///
    /// Never descends a restriction, a condition, a subquery, or a member's
    /// LEFT operand.
    ///
    /// Pinned by `fold_tail_ends_in_a_members_right_hand_chain`,
    /// `fold_tail_folds_every_bag_arm_left_operand_first`,
    /// `fold_tail_treats_a_trailing_restriction_as_an_opaque_leaf`, and
    /// `fold_tail_hands_the_leaf_the_whole_chain`.
    #[stacksafe::stacksafe]
    pub fn fold_tail<R>(&self, leaf: &dyn Fn(&Chain<P>) -> R, set_fold: &dyn Fn(Vec<R>) -> R) -> R {
        let Some((last, prefix)) = self.split_last() else {
            return leaf(self);
        };
        match last.form() {
            Continuation::Member { rhs, .. } => rhs.fold_tail(leaf, set_fold),
            Continuation::BagOp { arm, .. } => set_fold(vec![
                prefix.to_chain().fold_tail(leaf, set_fold),
                arm.fold_tail(leaf, set_fold),
            ]),
            Continuation::Access { .. }
            | Continuation::Restrict { .. }
            | Continuation::Correlated(_)
            | Continuation::Correlate { .. }
            | Continuation::Bound { .. }
            | Continuation::Destructure { .. }
            | Continuation::Pipe { .. }
            | Continuation::Structural(_)
            | Continuation::ErJoin(_) => leaf(self),
        }
    }
}

/// A borrowed view of a chain's prefix: the operand a continuation consumes.
///
/// PRIVATE FIELDS: the view is of one chain's own prefix, and the one road
/// out of it clones exactly that. A view whose halves could be reassigned
/// would be an assembly road that pairs one chain's head with another's
/// steps without any admission.
pub struct ChainPrefix<'a, P: Phase> {
    head: &'a Grelex<P>,
    continuations: &'a [Step<P>],
}

impl<'a, P: Phase> ChainPrefix<'a, P> {
    pub fn to_chain(&self) -> Chain<P> {
        Chain {
            head: self.head.clone(),
            continuations: self.continuations.to_vec(),
        }
    }
}

/// §2.2 — THE CHAIN'S HEAD: the ground relational expression, and the
/// relation it publishes.
///
/// Private fields, exactly as a [`Step`]'s are, and for the same reason: a
/// relation attachable to any valid head is a relation attachable to the
/// wrong one. Before resolution the pairing is empty and
/// [`Grelex::authored`] builds one from the form; after it, only the
/// semantic authority can, and it derives the relation from THIS form.
#[derive(Debug, PartialEq, ToLispy)]
#[lispy("grelex")]
pub struct Grelex<P: Phase = Unresolved> {
    #[lispy("form")]
    form: GroundForm<P>,
    #[lispy("result")]
    result: P::Scope,
}

impl<P: Phase> Grelex<P> {
    pub fn form(&self) -> &GroundForm<P> {
        &self.form
    }

    pub fn into_form(self) -> GroundForm<P> {
        self.form
    }

    pub fn result(&self) -> &P::Scope {
        &self.result
    }
}

impl<P: Phase<Scope = ()>> Grelex<P> {
    /// The authored head. Nothing is paired — the phase has no relation.
    pub fn authored(form: GroundForm<P>) -> Self {
        Grelex { form, result: () }
    }

    /// The payload, for an authored walk. AUTHORED ONLY, for the reason
    /// [`Step::form_mut`] states.
    pub fn form_mut(&mut self) -> &mut GroundForm<P> {
        &mut self.form
    }

    /// Rewrite an authored head payload by value.
    pub fn rewrite_form(
        self,
        rewrite: impl FnOnce(GroundForm<P>) -> crate::error::Result<GroundForm<P>>,
    ) -> crate::error::Result<Self> {
        Ok(Grelex {
            form: rewrite(self.form)?,
            result: (),
        })
    }
}

impl<P: Phase> Grelex<P> {
    /// REBUILD THE RELATIONAL OPERAND STANDING INSIDE THIS HEAD.
    ///
    /// Same contract as [`Step::rebuilding_arm`], at the head: a derived
    /// table's subquery is an operand nested in ONE node, the rewrite is
    /// handed that operand alone, and the head is rebuilt around it here —
    /// so which relation the head IS cannot change, and what it publishes
    /// stays true because the subquery comes back publishing what it
    /// published ([`rebuilt_in_place`]).
    pub fn rebuilding_nested(
        self,
        nested: impl FnOnce(Chain<P>) -> crate::error::Result<Chain<P>>,
    ) -> crate::error::Result<Self> {
        let Grelex { form, result } = self;
        let form = match form {
            GroundForm::Reference(relation) => GroundForm::Reference(
                relation.rebuilding_nested(|subquery| rebuilt_in_place(subquery, nested))?,
            ),
            literal @ GroundForm::Literal(_) => literal,
        };
        Ok(Grelex { form, result })
    }

    /// Cross a phase boundary, through the same door a [`Step`] uses.
    ///
    /// THE FORM IS THIS HEAD'S OWN, crossed by the walk functions that
    /// rebuild it around its leaves — a mention, a call's arguments, an
    /// anonymous header's slots — and around the subtree it nests, and what
    /// the head publishes is ITS OWN result, carried by the phases' door.
    /// There is no argument for a form, no hook that answers with a head or
    /// a relation, and no hook that answers with what a head publishes, so
    /// a walk cannot put another relation's identity, or another kind of
    /// head, at the base of a chain. The nested body comes back as the one
    /// the boundary was derived over, and an INTERIOR is then realized
    /// through the one hook that answers with one, judged here against the
    /// body the head actually holds: a realization keeps that body, or
    /// replaces it with a rebuild the relation authority judged for that
    /// body, and nothing else stands under a boundary's kept identity.
    pub fn folded<Q: Phase, F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
    ) -> crate::error::Result<Grelex<Q>> {
        let Grelex { form, result } = self;
        let result = crate::pipeline::asts::core::phases::carry_scope::<P, Q>(result)?;
        let body = form.nested_body();
        let form = match form {
            GroundForm::Reference(relation) => GroundForm::Reference(
                crate::pipeline::ast_transform::walk_transform_relation(walk, relation)?,
            ),
            GroundForm::Literal(anon) => GroundForm::Literal(anon.crossed(walk)?),
        };
        if form.nested_body() != body {
            return Err(Internal::invariant(
                "chain",
                "a fold handed a head's body back publishing a relation other than the one \
                 its boundary was derived over",
            ));
        }
        let form = match form {
            GroundForm::Reference(Relation::InnerRelation { pattern, alias }) => {
                let stood_over = Q::into_scope(pattern.subquery().published());
                let pattern = walk.realize_interior(pattern)?.judged(stood_over)?;
                GroundForm::Reference(Relation::InnerRelation { pattern, alias })
            }
            other => other,
        };
        Ok(Grelex { form, result })
    }
}

impl<P: Phase> Clone for Grelex<P> {
    #[stacksafe::stacksafe]
    fn clone(&self) -> Self {
        Grelex {
            form: self.form.clone(),
            result: self.result.clone(),
        }
    }
}

/// The two alternatives are the owner's taxonomy: R-REFERENCE names
/// something, R-LITERAL writes rows out. Which one a head is gets decided
/// where the head is built, and is never rediscovered by inspecting a
/// payload downstream.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum GroundForm<P: Phase = Unresolved> {
    /// Every named relational form: table, view, TVF, HO application, inner
    /// relation, consulted view, plan scratch, the DML target.
    #[lispy("grelex:reference")]
    Reference(Relation<P>),
    /// The anonymous table and the membership probe.
    #[lispy("grelex:literal")]
    Literal(AnonRelation<P>),
}

/// One occurrence of an anonymous relation. Ordinary and receipt payloads
/// are separate construction products, so a receipt's table cannot cross
/// this boundary as an ordinary table that another caller can re-pair.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("anon_relation")]
pub struct AnonRelation<P: Phase = Unresolved> {
    payload: AnonRelationPayload<P>,
}

/// The payload products an anonymous-relation authority can construct.
/// Receipt rows remain in the receipt arm even when an authored alias is
/// attached; aliasing changes publication, not provenance.
#[derive(Debug, Clone, PartialEq, ToLispy)]
enum AnonRelationPayload<P: Phase> {
    Ordinary {
        table: AnonTable<P>,
        name: AnonRelationName,
    },
    Receipt {
        table: AnonTable<P>,
        alias: Option<SqlIdentifier>,
        isolation: crate::bin_cartridge::prelude::ReceiptIsolation,
    },
}

#[derive(Debug, Clone, PartialEq, ToLispy)]
enum AnonRelationName {
    #[lispy("unnamed")]
    Unnamed,
    #[lispy("authored")]
    Authored(SqlIdentifier),
}

impl AnonRelationName {
    fn authored(&self) -> Option<&SqlIdentifier> {
        match self {
            Self::Authored(alias) => Some(alias),
            Self::Unnamed => None,
        }
    }
}

impl<P: Phase> AnonRelation<P> {
    pub fn plain(table: AnonTable<P>) -> Self {
        Self {
            payload: AnonRelationPayload::Ordinary {
                table,
                name: AnonRelationName::Unnamed,
            },
        }
    }

    pub(crate) fn authored(table: AnonTable<P>, alias: SqlIdentifier) -> Self {
        Self {
            payload: AnonRelationPayload::Ordinary {
                table,
                name: AnonRelationName::Authored(alias),
            },
        }
    }

    pub(crate) fn receipt(
        table: AnonTable<P>,
        isolation: crate::bin_cartridge::prelude::ReceiptIsolation,
    ) -> Self {
        Self {
            payload: AnonRelationPayload::Receipt {
                table,
                alias: None,
                isolation,
            },
        }
    }

    pub(crate) fn authored_receipt(
        table: AnonTable<P>,
        alias: SqlIdentifier,
        isolation: crate::bin_cartridge::prelude::ReceiptIsolation,
    ) -> Self {
        Self {
            payload: AnonRelationPayload::Receipt {
                table,
                alias: Some(alias),
                isolation,
            },
        }
    }

    /// Cross the payload through the canonical structural AST walk. The walk
    /// answers only for fields inside this occurrence; it cannot return a
    /// replacement table to pair with this relation's disposition.
    pub(crate) fn crossed<Q: Phase, F>(self, walk: &mut F) -> crate::error::Result<AnonRelation<Q>>
    where
        F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized,
    {
        let Self { payload } = self;
        let payload = match payload {
            AnonRelationPayload::Ordinary { table, name } => AnonRelationPayload::Ordinary {
                table: crate::pipeline::ast_transform::walk_transform_anon_table(walk, table)?,
                name,
            },
            AnonRelationPayload::Receipt {
                table,
                alias,
                isolation,
            } => AnonRelationPayload::Receipt {
                table: crate::pipeline::ast_transform::walk_transform_anon_table(walk, table)?,
                alias,
                isolation,
            },
        };
        Ok(AnonRelation { payload })
    }

    /// Ordinary consumers may inspect their table. Receipt rows have no
    /// ordinary table view; receipt operations and the structural crossing
    /// remain the only roads that retain their disposition.
    pub(crate) fn table(&self) -> Option<&AnonTable<P>> {
        match &self.payload {
            AnonRelationPayload::Ordinary { table, .. } => Some(table),
            AnonRelationPayload::Receipt { .. } => None,
        }
    }

    pub(crate) fn authored_name(&self) -> Option<&SqlIdentifier> {
        match &self.payload {
            AnonRelationPayload::Ordinary { name, .. } => name.authored(),
            AnonRelationPayload::Receipt { alias, .. } => alias.as_ref(),
        }
    }

    #[cfg(test)]
    pub(crate) fn has_authored_name(&self) -> bool {
        self.authored_name().is_some()
    }

    #[cfg(test)]
    pub(crate) fn is_receipt_isolated(&self) -> bool {
        matches!(self.payload, AnonRelationPayload::Receipt { .. })
    }

    pub(crate) fn set_authored_name(self, alias: SqlIdentifier) -> Self {
        let payload = match self.payload {
            AnonRelationPayload::Ordinary { table, .. } => AnonRelationPayload::Ordinary {
                table,
                name: AnonRelationName::Authored(alias),
            },
            AnonRelationPayload::Receipt {
                table, isolation, ..
            } => AnonRelationPayload::Receipt {
                table,
                alias: Some(alias),
                isolation,
            },
        };
        Self { payload }
    }

    fn into_terminal_table_inner(self) -> AnonTable<P> {
        match self.payload {
            AnonRelationPayload::Ordinary { table, .. }
            | AnonRelationPayload::Receipt { table, .. } => table,
        }
    }

    /// The canonical non-consuming AST visit for either payload product.
    /// Receipt inspection stays a walk over the owned occurrence; it does not
    /// return a table that can be installed in an ordinary relation.
    pub(crate) fn visited_by<F: crate::pipeline::ast_visit::AstVisit<P> + ?Sized>(
        &self,
        visitor: &mut F,
    ) -> crate::error::Result<crate::pipeline::ast_visit::Descent> {
        match &self.payload {
            AnonRelationPayload::Ordinary { table, .. }
            | AnonRelationPayload::Receipt { table, .. } => {
                crate::pipeline::ast_visit::walk_visit_anon_table(visitor, table)
            }
        }
    }
}

impl AnonRelation<Unresolved> {
    /// Combine the prelude executable's completed result. The authority is
    /// private to that result accumulator, so an unresolved receipt cannot be
    /// extracted and rebuilt as an ordinary anonymous relation by a caller.
    pub(crate) fn into_combined_table(
        self,
        _authority: crate::bin_cartridge::prelude::CompileResultCombinationAuthority,
    ) -> AnonTable<Unresolved> {
        self.into_terminal_table_inner()
    }
}

/// The shared, nonempty geometry of an anonymous or fact body.
#[derive(Debug, Clone, PartialEq)]
pub struct TabularBody<H, D> {
    pub header: Option<TabularRow<H>>,
    pub rows: crate::pipeline::asts::vocabulary::Vec1<TabularRow<D>>,
}

impl<H: ToLispy, D: ToLispy> ToLispy for TabularBody<H, D> {
    fn to_lispy(&self) -> String {
        format!(
            "(tabular_body {} {})",
            self.header.to_lispy(),
            self.rows.to_lispy()
        )
    }
}

/// One nonempty row in a tabular body.
#[derive(Debug, Clone, PartialEq)]
pub struct TabularRow<T>(pub Box<crate::pipeline::asts::vocabulary::Vec1<T>>);

impl<T: ToLispy> ToLispy for TabularRow<T> {
    fn to_lispy(&self) -> String {
        format!("(tabular_row {})", self.0.to_lispy())
    }
}

impl<T> TabularRow<T> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter()
    }

    pub fn into_vec(self) -> Vec<T> {
        (*self.0).into_vec()
    }
}

/// One header position: the caller-pattern slot and whether rows may fill it
/// by name.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("header_item")]
pub struct HeaderItem<P: Phase = Unresolved> {
    pub slot: Slot<P>,
    pub sparse: bool,
}

impl<P: Phase> HeaderItem<P> {
    pub fn term(&self) -> Option<DomainExpression<P>>
    where
        P: Clone,
    {
        self.slot.term()
    }
}

/// A cell admitted by a tabular datum position. Sparse cells retain the
/// column they answer and the ground fallback used when the row omitted it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Datum<P: Phase = Unresolved> {
    #[lispy("datum:value")]
    Value(DomainExpression<P>),
    #[lispy("datum:sparse_fill")]
    SparseFill {
        column: SqlIdentifier,
        fallback: LiteralValue,
    },
}

impl<P: Phase> Datum<P> {
    pub fn value(&self) -> DomainExpression<P>
    where
        P: Clone,
    {
        match self {
            Self::Value(value) => value.clone(),
            Self::SparseFill { fallback, .. } => DomainExpression::Application(
                super::functions::FunctionApplication::Ground(fallback.clone()),
            ),
        }
    }

    pub fn into_value(self) -> DomainExpression<P> {
        match self {
            Self::Value(value) => value,
            Self::SparseFill { fallback, .. } => DomainExpression::Application(
                super::functions::FunctionApplication::Ground(fallback),
            ),
        }
    }
}

/// §2.5 — the anonymous table's literal contents.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("anon_table")]
pub struct AnonTable<P: Phase = Unresolved> {
    pub body: TabularBody<HeaderItem<P>, Datum<P>>,
}

impl<P: Phase> AnonTable<P> {
    pub fn from_values(
        header: Option<Vec<DomainExpression<P>>>,
        rows: Vec<Vec<DomainExpression<P>>>,
    ) -> Option<Self> {
        let header = match header {
            Some(terms) => Some(TabularRow(Box::new(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                    terms
                        .into_iter()
                        .map(|term| HeaderItem {
                            slot: Slot::classify(term),
                            sparse: false,
                        })
                        .collect(),
                )?,
            ))),
            None => None,
        };
        let rows = rows
            .into_iter()
            .map(|values| {
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                    values.into_iter().map(Datum::Value).collect(),
                )
                .map(|row| TabularRow(Box::new(row)))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            body: TabularBody {
                header,
                rows: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(rows)?,
            },
        })
    }

    pub fn header(&self) -> Option<&TabularRow<HeaderItem<P>>> {
        self.body.header.as_ref()
    }

    pub fn rows(&self) -> &crate::pipeline::asts::vocabulary::Vec1<TabularRow<Datum<P>>> {
        &self.body.rows
    }
}

/// §2.2 — one frame of the chain: a function Relation → Relation awaiting
/// its left operand.
#[derive(Debug, PartialEq, ToLispy)]
pub enum Continuation<P: Phase = Unresolved> {
    /// A dimension access on the relation to its left: `(*)`, `()`,
    /// `(a, _, 30)`, `.(a, b)`, `.*`.
    ///
    /// THE ONE STRUCTURAL CARRIER for relational access. `users(*)` and
    /// `users()*` are the same query, so the mention's own parens and the
    /// postfix step are the same continuation at the same index; a
    /// callable relation's access group and a directive's receipt are that
    /// same continuation standing after the call. No relation node and no
    /// call payload carries an access field beside this one.
    ///
    /// A leading access — index 0, over a head that HAS a read to
    /// parameterize — is that read's own; see [`Chain::head_access`]. Every
    /// other one is a step on the relation built so far.
    #[lispy("continuation:access")]
    Access {
        access: super::access::Access<P>,
        /// `t(*) as u(a, b)` — the name the occurrence this access
        /// publishes answers to. Naming and patterning are one act at one
        /// occurrence: the slot row replaces the operand's interface and
        /// the name is the result's. A head's own leading access never
        /// carries one — the name of the read is the mention's.
        ///
        /// Phase-selected exactly as a pipe stage's: authored characters
        /// before resolution, nothing after.
        named: P::StageName,
    },
    /// Codd's σ — a truth expression standing where one can stand.
    ///
    /// The three kinds below share the comma position and NOTHING else
    /// (top-grammar's `comma_continuation`): a restriction drops rows, a
    /// bound keeps a prefix of them, and a destructure ADDS columns and may
    /// multiply rows. Reaching them through one condition enum made every
    /// consumer re-ask which of the three it was holding.
    #[lispy("continuation:restrict")]
    Restrict {
        condition: TruthExpression<P>,
        origin: FilterOrigin,
    },
    /// A RESTRICTION THE ENCLOSING JOIN EVALUATES — a correlation, carrying
    /// the interior occurrences it reads, as the one value the relation
    /// authority's correlation act minted (`Phase::Correlated`, inhabited
    /// in the resolved phase alone).
    ///
    /// It publishes its operand's relation exactly as a restriction does,
    /// and it is deliberately NOT a [`Transparent`] form: attaching one IS
    /// the correlation act, which records what the relation it stands on
    /// owes, so no road moves, drops, or re-attaches one — the classifier
    /// takes it off whole and spends it at the interior boundary. The one
    /// attachment road is [`Chain::correlated`], which the act alone holds
    /// the capability for.
    #[lispy("continuation:correlated")]
    Correlated(P::Correlated),
    /// `#<n` / `#>n` — the authored row bound standing beside NO ordering.
    /// Not a restriction: it selects by position, and with no order to
    /// select from its members are arbitrary. A bound written immediately
    /// after an ordering never stands here: [`Chain::bounding`] folds it
    /// into that ordering's own node, so the ordering a bound consumes is
    /// never a step beside it.
    #[lispy("continuation:bound")]
    Bound { bound: TupleOrdinalClause },
    /// `col ~= {…}` / `col ~= ~> {…}` — top-grammar's `destructure_relex`,
    /// an EXPANSION continuation. The scalar mode reads fields out of one
    /// document; the aggregate mode iterates and explodes rows.
    #[lispy("continuation:destructure")]
    Destructure {
        source: Box<DomainExpression<P>>,
        /// The heading witness together with the cardinality operation that
        /// owns it. A scalar-array binder exists only in the iterating arm.
        /// Once bound, each publishing member holds the occurrence it
        /// publishes.
        pattern: super::patterns::DestructurePattern<P>,
    },
    /// §5 — the comma's relation case: another relation joined to the
    /// chain-so-far. The member's role and what correlates the pair are
    /// member data; the relation in `rhs` carries neither.
    #[lispy("continuation:member")]
    Member {
        rhs: Chain<P>,
        correlation: P::MemberCorr,
        join: P::MemberJoin,
    },
    /// §6 — the `;` `|;|` `||` `-` family, BINARY: the chain-so-far is the
    /// left operand and this step owns exactly one right arm. `a ; b ; c` is
    /// two steps, so a correlation can name the pair it constrains instead
    /// of standing over an anonymous group.
    ///
    /// Correlation is a FIELD — bare and correlated are one carrier (H4),
    /// and there is no `distinct` field: the multiset law is an absent
    /// capability, not a flag set to false.
    #[lispy("continuation:bag_op")]
    BagOp {
        operator: SetOperator,
        arm: Chain<P>,
        correlation: P::Corr,
    },
    /// `x.* = y.*` in the comma position, BEFORE the pair of arms it
    /// relates is known.
    ///
    /// Its own comma kind, beside restrict/bound/destructure: it drops no
    /// row by itself and adds no column — it states an alignment, and the
    /// refiner moves it onto the bag step that owns the pair. Standing
    /// anywhere else it correlates nothing, and that refuses where the
    /// chain is read rather than reaching a lowering with no arm for it.
    #[lispy("continuation:correlate")]
    Correlate { whole: WholeHeading<P> },
    /// §4 — everything written after a pipe that is not a call.
    #[lispy("continuation:pipe")]
    Pipe {
        operator: PipeOp<P>,
        /// `|> (id) as f` — the name this stage's output answers to. An
        /// alias REPLACES the anonymous form rather than standing beside
        /// it, so a named stage is no longer something the deictic `_` can
        /// point at.
        ///
        /// Phase-selected: authored characters before resolution, and
        /// nothing after — resolution spends the spelling on the stage's
        /// scope, and the scope is what answers from then on.
        named: P::StageName,
    },
    /// `&` / `&&` with its mandatory per-edge context. Consumed by the
    /// resolver, which expands the edge into ordinary members — so after
    /// resolution the payload is uninhabited and this continuation cannot
    /// be built.
    #[lispy("continuation:er_join")]
    ErJoin(P::ErJoin),
    /// One of the seven structural run forms — ordering, reposition, meta,
    /// the witnesses, drill, narrowing — carried as the EXACT typed step the
    /// run partition returns. Membership in the trailing run is this
    /// variant's TYPE: no walker re-derives it from a list.
    #[lispy("continuation:structural")]
    Structural(StructuralStep<P>),
}

/// A structural step of the trailing run: one of the seven structural forms,
/// with the stage name and published scope every run step owns.
///
/// This is the exact payload [`RunForm::Structural`] carries, and the same
/// value a [`Continuation::Structural`] holds in the chain — one type, so the
/// partition moves it whole and no phase classifies it again.
#[derive(Debug, PartialEq, ToLispy)]
#[lispy("structural_step")]
pub struct StructuralStep<P: Phase = Unresolved> {
    #[lispy("form")]
    pub form: StructuralForm<P>,
    /// `as f` — the name this stage's output answers to, exactly as on a
    /// Pipe step: a run step owns its stage name whatever its kind.
    #[lispy("named")]
    pub named: P::StageName,
}

/// The seven structural forms, each with its exact payload. Adding a member
/// here breaks every consumer's match at compile time — omission cannot
/// become a runtime panic.
#[derive(Debug, PartialEq, ToLispy)]
pub enum StructuralForm<P: Phase = Unresolved> {
    /// `#(a, b desc)` — tuple ordering, WITH THE BOUND THAT CONSUMES IT.
    /// ORDER IS CONSUMED, NEVER CARRIED: terminal position presents
    /// (`bound: None`), and the immediately adjacent bound takes it
    /// (`bound: Some`) — `#(a), #<2` is ONE membership act, the ordering
    /// choosing which rows the bound keeps. The two are one node so no
    /// pass can move, commute, or lower one without the other, and the
    /// lowering emits both in one query scope. A bound-to-one compression
    /// owns its ordering the same way. Chain structure, not an anonymous
    /// operator — it directs row order and publishes the heading it
    /// received.
    #[lispy("structural:ordering")]
    Ordering {
        specs: Vec<super::super::specs::OrderingSpec<P>>,
        bound: Option<TupleOrdinalClause>,
    },
    /// `*[c as n]` — reposition: move columns to positions. Chain
    /// structure; the heading's names are untouched.
    #[lispy("structural:reposition")]
    Reposition {
        moves: Vec<super::super::specs::RepositionSpec<P>>,
    },
    /// `^` — meta-ize: reify the relation's schema as data, with the fixed
    /// heading (scope, column_name, ordinal). `^^` is two adjacent
    /// applications, never a token; the tower is constant from level two.
    #[lispy("structural:meta")]
    Meta,
    /// `+` / `\+` — the witness: existence reified as the one-row,
    /// one-column `met` relation. Polarity is DATA, one carrier — the two
    /// spellings are one form observed two ways, never a variant pair.
    #[lispy("structural:witness")]
    Witness { polarity: super::Polarity },
    /// `+-` — the signed witness (THE TOTAL LEDGER): the relation's schema
    /// plus `met` appended last; a NO arm contributes one all-NULL proxy
    /// row with met = 0.
    #[lispy("structural:signed_witness")]
    SignedWitness,
    /// `.col(…)` — the interior drill: explode an interior relation column
    /// into rows, context carried forward (lateral-join semantics). The
    /// payload is the phase's: names before binding, occurrences after.
    #[lispy("structural:drill")]
    Drill { drill: P::Drill },
    /// `|> .nest{a, .b.c}` — the narrowing destructure: iterate the array
    /// the nest carries and bind the pattern against each element. PAYLOAD
    /// ONLY — no context rides through, which is the whole difference from
    /// drill. The pattern is the same `RecordPattern` a `~=` destructure
    /// declares, read through the same path and published-name authorities.
    #[lispy("structural:narrow")]
    Narrow {
        nest: super::references::Reference<P>,
        /// Once bound, each member holds the occurrence it publishes.
        pattern: super::patterns::RecordPattern<P>,
    },
}

impl<P: Phase> Clone for StructuralStep<P> {
    fn clone(&self) -> Self {
        StructuralStep {
            form: self.form.clone(),
            named: self.named.clone(),
        }
    }
}

impl<P: Phase> Clone for StructuralForm<P> {
    fn clone(&self) -> Self {
        match self {
            StructuralForm::Ordering { specs, bound } => StructuralForm::Ordering {
                specs: specs.clone(),
                bound: bound.clone(),
            },
            StructuralForm::Reposition { moves } => StructuralForm::Reposition {
                moves: moves.clone(),
            },
            StructuralForm::Meta => StructuralForm::Meta,
            StructuralForm::Witness { polarity } => StructuralForm::Witness {
                polarity: *polarity,
            },
            StructuralForm::SignedWitness => StructuralForm::SignedWitness,
            StructuralForm::Drill { drill } => StructuralForm::Drill {
                drill: drill.clone(),
            },
            StructuralForm::Narrow { nest, pattern } => StructuralForm::Narrow {
                nest: nest.clone(),
                pattern: pattern.clone(),
            },
        }
    }
}

/// A STEP OF THE TRAILING RUN, partitioned by [`Chain::pop_run_step`]:
/// a pipe stage, a dimension access, or a structural step — each carrying its
/// EXACT payload. Only the partition constructs one, so holding a `RunStep`
/// IS the membership proof — a consumer matches this family exhaustively and
/// never re-derives membership from the continuation list.
#[derive(Debug)]
pub enum RunForm<P: Phase> {
    Pipe {
        operator: PipeOp<P>,
        named: P::StageName,
    },
    Access {
        access: super::super::Access<P>,
        named: P::StageName,
    },
    /// A structural step, moved whole — the exact family, never the broad
    /// continuation enum.
    Structural(StructuralStep<P>),
}

/// A run step and what it publishes, taken off a chain as one value.
///
/// Private fields: the partition is the only producer, so holding one is
/// the membership proof, and the relation cannot be separated from the
/// exact form that published it.
#[derive(Debug)]
pub struct RunStep<P: Phase> {
    form: RunForm<P>,
    result: P::Scope,
}

impl<P: Phase> RunStep<P> {
    pub fn form(&self) -> &RunForm<P> {
        &self.form
    }

    pub fn into_form(self) -> RunForm<P> {
        self.form
    }

    pub fn result(&self) -> &P::Scope {
        &self.result
    }

    /// Put the step back on the chain it came off, unchanged. Visible to
    /// the carrier alone: [`Chain::peel_run`] is what puts a run back.
    pub(super) fn into_step(self) -> Step<P> {
        Step {
            form: match self.form {
                RunForm::Pipe { operator, named } => Continuation::Pipe { operator, named },
                RunForm::Access { access, named } => Continuation::Access { access, named },
                RunForm::Structural(step) => Continuation::Structural(step),
            },
            result: self.result,
        }
    }
}

// Manual Clone for the same reason `Chain`'s is manual.
impl<P: Phase> Clone for Continuation<P> {
    #[stacksafe::stacksafe]
    fn clone(&self) -> Self {
        match self {
            Continuation::Access { access, named } => Continuation::Access {
                access: access.clone(),
                named: named.clone(),
            },
            Continuation::Restrict { condition, origin } => Continuation::Restrict {
                condition: condition.clone(),
                origin: origin.clone(),
            },
            Continuation::Correlated(correlated) => Continuation::Correlated(correlated.clone()),
            Continuation::Bound { bound } => Continuation::Bound {
                bound: bound.clone(),
            },
            Continuation::Correlate { whole } => Continuation::Correlate {
                whole: whole.clone(),
            },
            Continuation::Destructure { source, pattern } => Continuation::Destructure {
                source: source.clone(),
                pattern: pattern.clone(),
            },
            Continuation::Member {
                rhs,
                correlation,
                join,
            } => Continuation::Member {
                rhs: rhs.clone(),
                correlation: correlation.clone(),
                join: join.clone(),
            },
            Continuation::BagOp {
                operator,
                arm,
                correlation,
            } => Continuation::BagOp {
                operator: *operator,
                arm: arm.clone(),
                correlation: correlation.clone(),
            },
            Continuation::Pipe { operator, named } => Continuation::Pipe {
                operator: operator.clone(),
                named: named.clone(),
            },
            Continuation::ErJoin(step) => Continuation::ErJoin(step.clone()),
            Continuation::Structural(step) => Continuation::Structural(step.clone()),
        }
    }
}

/// §6 — the correlation a bag operation carries once the refiner has
/// settled it. Not a predicate variant: a correlation relates arms, so it
/// can never be classified as an ordinary non-participating predicate.
///
/// PAIR-SCOPED: the correlation constrains exactly the two arms it names —
/// its own step's arm and `with_arm` — and leaves every other arm of the
/// run alone. A three-arm union correlated on its outer two keeps the
/// middle arm whole, and nothing downstream re-derives the pair from
/// the predicate's column owners.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("bag_correlation")]
pub struct BagCorrelation<P: Phase = Unresolved> {
    /// The OTHER arm of the constrained pair, counted in the trailing bag
    /// run (arm 0 is the chain the run stands on). `ArmIx(0)` is the
    /// ordinary binary case: the correlation relates the chain-so-far.
    pub with_arm: ArmIx,
    /// What the pair is constrained by: a truth over the two arms, or the
    /// whole-heading form that relates their headings wholesale.
    pub predicate: CorrPred<P>,
    /// ROW_NUMBER + JOIN for min(m,n) multiplicity, when the danger gate
    /// asks for it. Only a bidirectional pair can want this.
    pub min_multiplicity: bool,
}

/// The columns a join CORRESPONDS on — a reused bare binder, a shared row
/// identity, and the unifying anonymous header.
///
/// NOT A TRUTH. By itself it accepts and rejects no row: it names which
/// columns must agree, and it decides what the join PUBLISHES. That second
/// half is why it cannot be spelled as a conjunction of equalities — a
/// correspondence merges the column it joined on and an ordinary condition
/// repeats it, so the two would join the same rows under different headings.
///
/// The exact operand ports are recorded at resolution. A spelling chose the
/// pair lexically; it is not retained as evidence after that judgment.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("correspondence")]
pub struct Correspondence {
    pub pairs: Vec<crate::relation::form::MergedKey>,
}

impl Correspondence {
    pub fn new(pairs: Vec<crate::relation::form::MergedKey>) -> Self {
        Correspondence { pairs }
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }
}

/// What correlates a member with the chain to its left.
///
/// The two are ALTERNATIVES, and this position holds the alternation. A
/// condition is a truth over the pair; a correspondence directs the join
/// and publishes its own heading. Carrying the second one through the truth
/// enum forced every truth consumer to hold an arm for something that is
/// not a truth, and every join consumer to re-ask which of the two it had.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum MemberCorrelation<P: Phase = Unresolved> {
    /// `a.x = b.y` — a truth the pair must satisfy.
    #[lispy("member_correlation:condition")]
    Condition(TruthExpression<P>),
    /// Phase-selected, and uninhabited before resolution: a correspondence
    /// is SYNTHESIZED from the anonymous header, the positional pattern,
    /// or the row identity that directs it, so there is no authored one to
    /// carry and no authored/resolved twin to drift.
    #[lispy("member_correlation:correspond")]
    Correspond(P::Correspondence),
    /// The pair CROSSES, deliberately: resolution enumerated the complete
    /// live bare interface and found no reuse, no correspondence, and no
    /// stated condition. Phase-witnessed, so an authored tree cannot state
    /// it — and lowering, holding this instead of an absence, cannot let
    /// missing evidence masquerade as a natural join.
    #[lispy("member_correlation:cartesian")]
    Cartesian(P::Decided),
}

impl<P: Phase> MemberCorrelation<P> {
    /// The truth this correlation is, when it is one. A correspondence
    /// answers `None`: a caller that wants a predicate has not been handed
    /// one and must say what it does with the correspondence.
    pub fn condition(&self) -> Option<&TruthExpression<P>> {
        match self {
            MemberCorrelation::Condition(condition) => Some(condition),
            MemberCorrelation::Correspond(_) | MemberCorrelation::Cartesian(_) => None,
        }
    }

    pub fn into_condition(self) -> Option<TruthExpression<P>> {
        match self {
            MemberCorrelation::Condition(condition) => Some(condition),
            MemberCorrelation::Correspond(_) | MemberCorrelation::Cartesian(_) => None,
        }
    }

    /// The correspondence this correlation directs, when it directs one.
    pub fn correspondence(&self) -> Option<&Correspondence> {
        match self {
            MemberCorrelation::Correspond(carried) => Some(P::correspondence(carried)),
            MemberCorrelation::Condition(_) | MemberCorrelation::Cartesian(_) => None,
        }
    }
}

/// `x.* = y.*` / `x|*| = y|*|` — one arm's WHOLE HEADING correlated with
/// another's.
///
/// NOT A TRUTH, and not a restriction: it names two ARMS and the mode they
/// align by, and it cannot be evaluated against one row. A truth variant had
/// to be admitted in every truth position — inside a negation, a disjunction,
/// a case result — and outside a bag run it reached the lowering as an
/// unimplemented discriminant.
///
/// The two modes are two forms because the columns they pair are found two
/// different ways: by name, and by position. An atom that mixed them would
/// name no alignment, which is refused where the atom is read.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum WholeHeading<P: Phase = Unresolved> {
    /// `x.* = y.*` — every name both arms publish.
    #[lispy("whole_heading:by_name")]
    ByName {
        left: P::CorrelationArm,
        right: P::CorrelationArm,
    },
    /// `x|*| = y|*|` — every position both arms have.
    #[lispy("whole_heading:by_position")]
    ByPosition {
        left: P::CorrelationArm,
        right: P::CorrelationArm,
    },
}

impl<P: Phase> WholeHeading<P> {
    /// The two arms this correlation names, in authored order.
    pub fn arms(&self) -> (&P::CorrelationArm, &P::CorrelationArm) {
        match self {
            WholeHeading::ByName { left, right } | WholeHeading::ByPosition { left, right } => {
                (left, right)
            }
        }
    }

    /// Whether the arms align by name. The authored MODE survives into the
    /// carrier: a pass that expanded it into comparisons and forgot which
    /// mode produced them could not tell the two spellings apart again.
    pub fn by_name(&self) -> bool {
        matches!(self, WholeHeading::ByName { .. })
    }
}

/// What a pair-scoped correlation constrains its pair WITH.
///
/// One decision, two contents: an ordinary truth over the pair, or the
/// whole-heading form that relates their headings wholesale.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum CorrPred<P: Phase = Unresolved> {
    #[lispy("corr_pred:expression")]
    Expression(TruthExpression<P>),
    #[lispy("corr_pred:whole")]
    Whole(WholeHeading<P>),
}

impl<P: Phase> CorrPred<P> {
    /// The truth this predicate is, when it is one.
    pub fn expression(&self) -> Option<&TruthExpression<P>> {
        match self {
            CorrPred::Expression(expression) => Some(expression),
            CorrPred::Whole(_) => None,
        }
    }
}

/// §2.1/§2.2 — one `&`/`&&` edge. The context is total: the normalizer
/// writes `normal` for an omitted `&(::ctx)`, so no consumer decides what
/// an absence means.
///
/// The spellings are the canonical SELECTION keys — the written term with
/// the alias outside. Exports are governed by access mode; the two never
/// touch. `left_spelling` names the endpoint of the chain-so-far, so a
/// three-relation chain is two steps whose spellings meet in the middle.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("er_join_step")]
pub struct ErJoinStep<P: Phase = Unresolved> {
    /// `&&` finds a path; `&` demands a direct edge.
    pub transitive: bool,
    pub context: String,
    pub left_spelling: String,
    pub right_spelling: String,
    /// The term's own READ — the mention and the access its parens asked
    /// for. A relation and what was asked of it travel together.
    pub rhs: Chain<P>,
    /// THE PEER'S ROLE: `?` on the peer preserves the left rows the edge
    /// fails to reach, exactly as on a comma member. Fixed from the peer's
    /// syntax; the read in `rhs` carries no mark.
    pub role: super::super::operators::MemberRole,
}

#[cfg(test)]
mod tests;
