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
use super::super::{LiteralValue, Phase, Unresolved};
use super::access::Slot;
use super::domain::DomainExpression;
use super::metadata_types::{FilterOrigin, SetOperator};
use super::pipes::DestructureMode;
use super::relational::Relation;
use super::truth::TruthExpression;
use crate::pipeline::asts::core::operators::{JoinType, PipeOp};
use crate::pipeline::asts::vocabulary::ArmIx;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// §2.2 — the chain: a ground relational expression and the continuations
/// applied to it, in authored order.
#[derive(Debug, PartialEq, ToLispy)]
#[lispy("chain")]
pub struct Chain<P: Phase = Unresolved> {
    #[lispy("head")]
    pub head: Grelex<P>,
    #[lispy("continuations")]
    pub continuations: Vec<Continuation<P>>,
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

impl<P: Phase> Chain<P> {
    /// A chain that is nothing but its head.
    pub fn ground(head: Grelex<P>) -> Self {
        Chain {
            head,
            continuations: Vec::new(),
        }
    }

    /// A chain headed by a named relational form.
    pub fn relation(relation: Relation<P>) -> Self {
        Chain::ground(Grelex::Reference(relation))
    }

    /// A READ: a relation, and what the parens on it asked for. The one
    /// constructor for the pairing, so a mention cannot be built without an
    /// access or an access without the relation it asks of.
    pub fn read(
        relation: Relation<P>,
        access: super::access::Access<P>,
        cpr_schema: P::Scope,
    ) -> Self {
        Chain::relation(relation).then(Continuation::Access { access, cpr_schema })
    }

    /// The same pairing over a head that is already built.
    pub fn read_head(
        head: Grelex<P>,
        access: super::access::Access<P>,
        cpr_schema: P::Scope,
    ) -> Self {
        Chain::ground(head).then(Continuation::Access { access, cpr_schema })
    }

    /// Extend the chain. The continuation consumes everything to its left,
    /// which is what makes ordering structural rather than remembered.
    pub fn then(mut self, continuation: Continuation<P>) -> Self {
        self.continuations.push(continuation);
        self
    }

    /// One bag operation. The chain-so-far is the left operand and `arm` is
    /// the single right operand this step owns; `a ; b ; c` is a SEQUENCE of
    /// these steps, never one grouped node. There is no grouped constructor
    /// because a group has nowhere to record which pair a correlation
    /// relates (ruling 3).
    pub fn bag_op(
        self,
        operator: SetOperator,
        arm: Chain<P>,
        correlation: P::Corr,
        cpr_schema: P::Scope,
    ) -> Self {
        self.then(Continuation::BagOp {
            operator,
            arm,
            correlation,
            cpr_schema,
        })
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
        let operator = match self.continuations.last()? {
            Continuation::BagOp { operator, .. } => *operator,
            _ => return None,
        };
        let mut base = self.continuations.len() - 1;
        if operator.accumulates_arm_rows() {
            while base > 0 {
                match &self.continuations[base - 1] {
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
        self.continuations
            .iter()
            .rev()
            .find_map(|continuation| match continuation {
                Continuation::BagOp { .. } => Some(true),
                // A predicate and a correlation both stand ON the step
                // below: they are what `claim_bag_correlations` reads, so
                // the chain they stand on is still the bag's.
                Continuation::Restrict { .. } | Continuation::Correlate { .. } => None,
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
        match self.continuations.first() {
            Some(Continuation::Access { access, .. }) => Some(access),
            _ => None,
        }
    }

    /// Whether the head is a MENTION — the two relational forms whose read a
    /// leading access parameterizes.
    pub fn head_takes_an_access(&self) -> bool {
        match &self.head {
            Grelex::Reference(relation) => relation.takes_an_access(),
            Grelex::Literal(_) => false,
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
    pub fn steps(&self) -> &[Continuation<P>] {
        &self.continuations[self.head_span()..]
    }

    /// Whether anything consumes the relation the head's read publishes.
    pub fn has_steps(&self) -> bool {
        self.continuations.len() > self.head_span()
    }

    /// The outermost step, taken off. `None` leaves the chain at its base:
    /// the head and the access its own read consumes.
    pub fn pop_step(&mut self) -> Option<Continuation<P>> {
        self.has_steps().then(|| {
            self.continuations
                .pop()
                .expect("a chain with steps has a last continuation")
        })
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
        match step {
            Continuation::Pipe {
                operator,
                named,
                cpr_schema,
            } => Some(RunStep::Pipe {
                operator,
                named,
                cpr_schema,
            }),
            Continuation::Access { access, cpr_schema } => {
                Some(RunStep::Access { access, cpr_schema })
            }
            Continuation::Structural(step) => Some(RunStep::Structural(step)),
            other @ (Continuation::Restrict { .. }
            | Continuation::Correlate { .. }
            | Continuation::Bound { .. }
            | Continuation::Destructure { .. }
            | Continuation::Member { .. }
            | Continuation::BagOp { .. }
            | Continuation::ErJoin(_)) => {
                self.continuations.push(other);
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
    pub fn split_read(mut self) -> (Chain<P>, Vec<Continuation<P>>) {
        let steps = self.continuations.split_off(self.head_span());
        (self, steps)
    }

    /// The head, its own access, and the steps that consume the relation it
    /// publishes — by value, for a walk that owns what it rewrites.
    #[allow(clippy::type_complexity)]
    pub fn split_head_access(
        mut self,
    ) -> (
        Grelex<P>,
        Option<super::access::Access<P>>,
        Vec<Continuation<P>>,
    ) {
        let access = match self.head_span() {
            1 => match self.continuations.remove(0) {
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
        match (&self.head, self.has_steps()) {
            (Grelex::Reference(relation), false) => Some(relation),
            _ => None,
        }
    }

    /// The relation this chain names when nothing has consumed it yet.
    pub fn as_bare_relation(&self) -> Option<&Relation<P>> {
        match (&self.head, self.continuations.is_empty()) {
            (Grelex::Reference(relation), true) => Some(relation),
            _ => None,
        }
    }

    /// The head, by value, when nothing has consumed it.
    pub fn into_bare_head(self) -> Option<Grelex<P>> {
        self.continuations.is_empty().then_some(self.head)
    }

    /// The bare relation, by value.
    pub fn into_bare_relation(self) -> Result<Relation<P>, Self> {
        match (self.head, self.continuations.is_empty()) {
            (Grelex::Reference(relation), true) => Ok(relation),
            (head, _) => Err(Chain {
                head,
                continuations: self.continuations,
            }),
        }
    }

    /// The last continuation, and the chain that produces its operand.
    /// This is the fold read backwards — the shape a consumer needs when it
    /// asks what produced the relation it is looking at.
    pub fn split_last(&self) -> Option<(&Continuation<P>, ChainPrefix<'_, P>)> {
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
    /// The arm this run's `k`th step owns, counting arm 0 as the chain the
    /// run stands on.
    pub fn arm_of_step(&self, step: usize) -> ArmIx {
        ArmIx::from_raw((step + 1) as u16)
    }

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

/// Reads a chain's shaping continuations, outermost first, and stops at the
/// first continuation that brings another relation in (a member, a bag arm,
/// an ER edge).
pub struct SourceSpine<'a, P: Phase> {
    rest: &'a [Continuation<P>],
}

impl<'a, P: Phase> Iterator for SourceSpine<'a, P> {
    type Item = SpineStep<'a, P>;

    fn next(&mut self) -> Option<Self::Item> {
        let (last, rest) = self.rest.split_last()?;
        let step = match last {
            Continuation::Access { access, .. } => SpineStep::Access(access),
            Continuation::Restrict { condition, .. } => SpineStep::Restrict(condition),
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
        match last {
            Continuation::Member { rhs, .. } => rhs.fold_tail(leaf, set_fold),
            Continuation::BagOp { arm, .. } => set_fold(vec![
                prefix.to_chain().fold_tail(leaf, set_fold),
                arm.fold_tail(leaf, set_fold),
            ]),
            Continuation::Access { .. }
            | Continuation::Restrict { .. }
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
pub struct ChainPrefix<'a, P: Phase> {
    pub head: &'a Grelex<P>,
    pub continuations: &'a [Continuation<P>],
}

impl<'a, P: Phase> ChainPrefix<'a, P> {
    pub fn to_chain(&self) -> Chain<P> {
        Chain {
            head: self.head.clone(),
            continuations: self.continuations.to_vec(),
        }
    }
}

/// §2.2 — the ground relational expression: what a chain starts from.
///
/// The two alternatives are the owner's taxonomy: R-REFERENCE names
/// something, R-LITERAL writes rows out. Which one a head is gets decided
/// where the head is built, and is never rediscovered by inspecting a
/// payload downstream.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Grelex<P: Phase = Unresolved> {
    /// Every named relational form: table, view, TVF, HO application, inner
    /// relation, consulted view, plan scratch, the DML target.
    #[lispy("grelex:reference")]
    Reference(Relation<P>),
    /// The anonymous table and the membership probe.
    #[lispy("grelex:literal")]
    Literal(AnonRelation<P>),
}

/// One occurrence of an anonymous relation. Naming and outer orientation
/// describe where the table stands, not the literal rows it contains.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("anon_relation")]
pub struct AnonRelation<P: Phase = Unresolved> {
    pub table: AnonTable<P>,
    pub alias: Option<SqlIdentifier>,
    pub outer: bool,
}

impl<P: Phase> AnonRelation<P> {
    pub fn plain(table: AnonTable<P>) -> Self {
        Self {
            table,
            alias: None,
            outer: false,
        }
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
    pub cpr_schema: P::Scope,
}

impl<P: Phase> AnonTable<P> {
    pub fn from_values(
        header: Option<Vec<DomainExpression<P>>>,
        rows: Vec<Vec<DomainExpression<P>>>,
        cpr_schema: P::Scope,
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
            cpr_schema,
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
        cpr_schema: P::Scope,
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
        cpr_schema: P::Scope,
    },
    /// `#<n` / `#>n` — the authored row bound. Not a restriction: it selects
    /// by position in an order, so it consumes an adjacent ordering rather
    /// than testing a tuple.
    #[lispy("continuation:bound")]
    Bound {
        bound: TupleOrdinalClause,
        cpr_schema: P::Scope,
    },
    /// `col ~= {…}` / `col ~= ~> {…}` — top-grammar's `destructure_relex`,
    /// an EXPANSION continuation. The scalar mode reads fields out of one
    /// document; the aggregate mode iterates and explodes rows.
    #[lispy("continuation:destructure")]
    Destructure {
        source: Box<DomainExpression<P>>,
        /// The heading witness, declared not evaluated. A `TreePattern` by
        /// type: no consumer checks that a value function "happens to be
        /// curly" or that an array function is standing in for a pattern.
        pattern: super::patterns::TreePattern<P>,
        mode: DestructureMode,
        /// The columns the expansion publishes — empty before resolution.
        schema: P::Destructure,
        cpr_schema: P::Scope,
    },
    /// §5 — the comma's relation case: another relation joined to the
    /// chain-so-far. Outerness and what correlates the pair are member data.
    #[lispy("continuation:member")]
    Member {
        rhs: Chain<P>,
        correlation: Option<MemberCorrelation<P>>,
        join_type: Option<JoinType>,
        cpr_schema: P::Scope,
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
        cpr_schema: P::Scope,
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
    Correlate {
        whole: WholeHeading<P>,
        cpr_schema: P::Scope,
    },
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
        cpr_schema: P::Scope,
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
/// This is the exact payload [`RunStep::Structural`] carries, and the same
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
    #[lispy("cpr_schema")]
    pub cpr_schema: P::Scope,
}

/// The seven structural forms, each with its exact payload. Adding a member
/// here breaks every consumer's match at compile time — omission cannot
/// become a runtime panic.
#[derive(Debug, PartialEq, ToLispy)]
pub enum StructuralForm<P: Phase = Unresolved> {
    /// `#(a, b desc)` — tuple ordering. ORDER IS CONSUMED, NEVER CARRIED:
    /// terminal position presents, an adjacent bound takes it, a
    /// bound-to-one compression owns it. Chain structure, not an anonymous
    /// operator — it directs row order and publishes the heading it
    /// received.
    #[lispy("structural:ordering")]
    Ordering {
        specs: Vec<super::super::specs::OrderingSpec<P>>,
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
        pattern: super::patterns::RecordPattern<P>,
        /// The columns the narrowing publishes — empty before resolution.
        schema: P::Destructure,
    },
}

impl<P: Phase> Clone for StructuralStep<P> {
    fn clone(&self) -> Self {
        StructuralStep {
            form: self.form.clone(),
            named: self.named.clone(),
            cpr_schema: self.cpr_schema.clone(),
        }
    }
}

impl<P: Phase> Clone for StructuralForm<P> {
    fn clone(&self) -> Self {
        match self {
            StructuralForm::Ordering { specs } => StructuralForm::Ordering {
                specs: specs.clone(),
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
            StructuralForm::Narrow {
                nest,
                pattern,
                schema,
            } => StructuralForm::Narrow {
                nest: nest.clone(),
                pattern: pattern.clone(),
                schema: schema.clone(),
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
pub enum RunStep<P: Phase> {
    Pipe {
        operator: PipeOp<P>,
        named: P::StageName,
        cpr_schema: P::Scope,
    },
    Access {
        access: super::super::Access<P>,
        cpr_schema: P::Scope,
    },
    /// A structural step, moved whole — the exact family, never the broad
    /// continuation enum.
    Structural(StructuralStep<P>),
}

// Manual Clone for the same reason `Chain`'s is manual.
impl<P: Phase> Clone for Continuation<P> {
    #[stacksafe::stacksafe]
    fn clone(&self) -> Self {
        match self {
            Continuation::Access { access, cpr_schema } => Continuation::Access {
                access: access.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Continuation::Restrict {
                condition,
                origin,
                cpr_schema,
            } => Continuation::Restrict {
                condition: condition.clone(),
                origin: origin.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Continuation::Bound { bound, cpr_schema } => Continuation::Bound {
                bound: bound.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Continuation::Correlate { whole, cpr_schema } => Continuation::Correlate {
                whole: whole.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Continuation::Destructure {
                source,
                pattern,
                mode,
                schema,
                cpr_schema,
            } => Continuation::Destructure {
                source: source.clone(),
                pattern: pattern.clone(),
                mode: mode.clone(),
                schema: schema.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Continuation::Member {
                rhs,
                correlation,
                join_type,
                cpr_schema,
            } => Continuation::Member {
                rhs: rhs.clone(),
                correlation: correlation.clone(),
                join_type: join_type.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Continuation::BagOp {
                operator,
                arm,
                correlation,
                cpr_schema,
            } => Continuation::BagOp {
                operator: *operator,
                arm: arm.clone(),
                correlation: correlation.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Continuation::Pipe {
                operator,
                named,
                cpr_schema,
            } => Continuation::Pipe {
                operator: operator.clone(),
                named: named.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Continuation::ErJoin(step) => Continuation::ErJoin(step.clone()),
            Continuation::Structural(step) => Continuation::Structural(step.clone()),
        }
    }
}

impl<P: Phase> Continuation<P> {
    /// The schema the chain publishes once this continuation has consumed
    /// it. `None` for the preverbal forms, which are gone before any
    /// schema exists.
    pub fn cpr_schema(&self) -> Option<&P::Scope> {
        match self {
            Continuation::Access { cpr_schema, .. }
            | Continuation::Restrict { cpr_schema, .. }
            | Continuation::Correlate { cpr_schema, .. }
            | Continuation::Bound { cpr_schema, .. }
            | Continuation::Destructure { cpr_schema, .. }
            | Continuation::Member { cpr_schema, .. }
            | Continuation::BagOp { cpr_schema, .. }
            | Continuation::Pipe { cpr_schema, .. } => Some(cpr_schema),
            Continuation::Structural(step) => Some(&step.cpr_schema),
            Continuation::ErJoin(_) => None,
        }
    }

    pub fn cpr_schema_mut(&mut self) -> Option<&mut P::Scope> {
        match self {
            Continuation::Access { cpr_schema, .. }
            | Continuation::Restrict { cpr_schema, .. }
            | Continuation::Correlate { cpr_schema, .. }
            | Continuation::Bound { cpr_schema, .. }
            | Continuation::Destructure { cpr_schema, .. }
            | Continuation::Member { cpr_schema, .. }
            | Continuation::BagOp { cpr_schema, .. }
            | Continuation::Pipe { cpr_schema, .. } => Some(cpr_schema),
            Continuation::Structural(step) => Some(&mut step.cpr_schema),
            Continuation::ErJoin(_) => None,
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

/// The columns a join CORRESPONDS on — `t(*.(a, b))`, `t(.*)`, and the
/// unifying anonymous header.
///
/// NOT A TRUTH. By itself it accepts and rejects no row: it names which
/// columns must agree, and it decides what the join PUBLISHES. That second
/// half is why it cannot be spelled as a conjunction of equalities — a
/// correspondence merges the column it joined on and an ordinary condition
/// repeats it, so the two would join the same rows under different headings.
///
/// The names are canonical symbols because a strop is part of the name a
/// join corresponds on.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("correspondence")]
pub struct Correspondence {
    pub columns: Vec<crate::names::Sym>,
}

impl Correspondence {
    pub fn new(columns: Vec<crate::names::Sym>) -> Self {
        Correspondence { columns }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
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
    /// is SYNTHESIZED from the access, the anonymous header, or the
    /// positional pattern that directs it, so there is no authored one to
    /// carry and no authored/resolved twin to drift.
    #[lispy("member_correlation:correspond")]
    Correspond(P::Correspondence),
}

impl<P: Phase> MemberCorrelation<P> {
    /// The truth this correlation is, when it is one. A correspondence
    /// answers `None`: a caller that wants a predicate has not been handed
    /// one and must say what it does with the correspondence.
    pub fn condition(&self) -> Option<&TruthExpression<P>> {
        match self {
            MemberCorrelation::Condition(condition) => Some(condition),
            MemberCorrelation::Correspond(_) => None,
        }
    }

    pub fn into_condition(self) -> Option<TruthExpression<P>> {
        match self {
            MemberCorrelation::Condition(condition) => Some(condition),
            MemberCorrelation::Correspond(_) => None,
        }
    }

    /// The correspondence this correlation directs, when it directs one.
    pub fn correspondence(&self) -> Option<&Correspondence> {
        match self {
            MemberCorrelation::Correspond(carried) => Some(P::correspondence(carried)),
            MemberCorrelation::Condition(_) => None,
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

/// §2.1/§2.2 — one `&`/`&&` edge: the context is MANDATORY (`&(::ctx)`);
/// `None` is the removed bare-operator dialect, refused at resolve with the
/// symbol-form teaching.
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
    pub context: Option<String>,
    pub left_spelling: String,
    pub right_spelling: String,
    /// The term's own READ — the mention and the access its parens asked
    /// for. A relation and what was asked of it travel together.
    pub rhs: Chain<P>,
}

#[cfg(test)]
mod tests;
