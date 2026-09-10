// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE LEXICAL FRONTIER: what an authored reference may reach at one
//! position of a query, and the one authority that answers it.
//!
//! Three facts about a column occurrence are kept apart here, because
//! conflating them is how a predecessor's qualifier outlived the PIPE FORM
//! that consumed it:
//!
//! - IDENTITY — which occurrence a position is (`PortId`, `ScopeId`,
//!   `SemanticRelation`); copyable, and never permission;
//! - PROVENANCE — which earlier occurrence a position continues (carry
//!   edges, lineage); kept by the relation store for its own laws;
//! - ADDRESSABILITY — which authored routes name a position HERE; owned by
//!   this module and by nothing else.
//!
//! Addressability is a [`Frontier`]: the relations an authored qualifier
//! reaches at the position a relation stands at. Its fields are private to
//! this module. It has no `Clone`, no accessor that hands out its parts, no
//! constructor a caller can feed a copied relation identity, and no
//! operation that turns a publication, a provenance record, or a spelling
//! back into a route. It is born only inside the acts of
//! [`ResolvedRelation`], each of which derives what answers over its
//! result FROM that result — and it dies where the carrier that owns it is
//! consumed.
//!
//! Every PIPE FORM crosses through [`ResolvedRelation::crossed`]: the input
//! carrier is consumed, its frontier with it, and the far side's frontier
//! is born from the produced relation and the optional name authored on
//! that exact result. There is no argument for predecessor state, so a far
//! side that still answers to its predecessor's qualifier is not a value
//! this module can construct.
//!
//! A resolving operation borrows the frontier through a [`Position`] — the
//! relations under the reader's finger, innermost last, and the enclosing
//! fold's position behind them. Enclosing frames are BORROWED, never
//! copied: an interior expression sees the row it is correlated to because
//! its fold holds a reference to the outer position, and the outer
//! position cannot move while it is held. A frame is entered by the
//! operation that consumes it and left by the same operation, by value, so
//! the crossing that follows receives the very carrier the operation
//! borrowed and no second copy of what it answered to survives.
//!
//! Every authored lookup — a bare or qualified name, an ordinal, a
//! qualified glob, the deictic `_`, a set operation's whole-heading
//! correlation owner, an anonymous member's header, a slot row's owner
//! and binders — is a terminal judgment of [`Position`] or of the acts
//! in [`standing`]. The ingredients it is decided over are assembled here,
//! from the frames, and are handed to no caller: no candidate list leaves
//! this module for a caller to finish a lookup with, and no frame is
//! minted from a relation identity, a vector of positions, or a list of
//! carriers a caller supplies. A frame is an affine carrier an act of
//! [`standing`] produced — a read the resolver performed, a crossing, a
//! join, a row a lexical act declared from spellings — or the row of
//! carriers one call bound, minted here from that call's own record
//! ([`carriers`]), which is the only holder of what landing names what
//! carrier. A compiler-owned row travels as the product of the act that
//! bound or allocated it, never as an identity a caller copied.

mod anonymous;
mod join;
mod lookup;
mod pattern;
mod standing;

pub(crate) use anonymous::{BornPosition, JudgedBirth};
pub(in crate::pipeline::resolver) use join::shared_using_names;
pub(crate) use pattern::StrictPhaseConverter;
/// An UNFINISHED read: the pattern applied, its constraints not yet spent.
/// Only the join position needs one, and only the relation authority mints
/// one — the pair is what keeps a member's read from being assembled from
/// a name and a relation chosen apart.
pub(in crate::pipeline::resolver) use standing::PatternRead;
pub(crate) use standing::{PatternOperand, PatternOwner, ResolvedQuery, ResolvedRelation};

use crate::diagnostic::{Constraint, Resolution, ResolutionSetop};
use crate::error::Result;
use crate::names::Sym;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::resolver::unification::{ColumnReference, UnificationResult};
use crate::relation::{PortId, SemanticRelation};
use delightql_types::SqlIdentifier;

/// ONE ROUTE: a relation in view and the spelling that reaches it — the
/// answer its birth recorded, or the one the act that made it visible
/// bound it under (an interior under its nest name). Assembled by the
/// authority from a frontier; never accepted from a caller.
#[derive(Clone)]
pub(super) struct Binding {
    pub(super) relation: SemanticRelation,
    pub(super) answer: Option<Sym>,
    /// The positions this route reaches, when it reaches fewer than the
    /// relation publishes: an edge's endpoint names each reach the columns
    /// that belong to that endpoint. `None` reaches the whole interface.
    pub(super) ports: Option<Vec<PortId>>,
}

/// WHAT ANSWERS OVER ONE STANDING RELATION.
///
/// Private fields, no `Clone`, no parts accessor. Born only by the acts in
/// [`standing`], each from the relation that act produced or the operands
/// it stood on; ended with the carrier that owns it.
/// THE PROOF OF A TERMINAL JUDGMENT: the frontier decided which live
/// occurrence an authored reference addresses. Minted here and nowhere
/// else — its one field is private to this module — and spent by
/// [`crate::pipeline::asts::core::ColumnOccurrence::addressed`], so a
/// resolved authored reference exists only where a lookup was made.
pub(crate) struct Terminal(());

impl Terminal {
    fn judged() -> Self {
        Terminal(())
    }

    /// A proof for a test of the relation authority, which cannot open a
    /// lexical position of its own. Test-only: production mints the proof
    /// only where a lookup was made.
    #[cfg(test)]
    pub(crate) fn judged_for_test() -> Self {
        Terminal(())
    }
}

pub(crate) struct Frontier {
    /// The relations an authored qualifier reaches here, in visibility
    /// order. A relation reaches by the answer its own birth recorded — an
    /// entity name, an authored alias, a stage owner — and by nothing a
    /// caller can supply beside it.
    visible: Vec<Visible>,
    /// The routes a CORRELATION attached to this relation may take and a
    /// form over it may not: a set operation's operands. `x ; y, x.k =
    /// y.k` relates the arms, and the refinement that lowers it reads
    /// the arms' own positions — so the condition standing in the
    /// operation's row reaches them, while a stage over the result sees
    /// the merged heading the operation published and nothing of the
    /// arms.
    correlates: Vec<Visible>,
    /// What this position still owes: references bubbled out of a form
    /// that a later act must answer. Not a route; a debt.
    owed: Vec<ColumnReference>,
}

struct Visible {
    relation: SemanticRelation,
    /// The spelling this entry is bound under HERE, when the act that made
    /// it visible chose one: a drilled interior answers to the nest name
    /// it was drilled out of. `None` means the relation's own recorded
    /// answer.
    answer: Option<Sym>,
    /// The positions the route reaches, when fewer than the relation
    /// publishes.
    ports: Option<Vec<PortId>>,
    /// Its ports are ALSO offered to a bare reference. An EXISTS sibling
    /// witness enters this way: `+orders(...), +items(..., orders.x = y)`
    /// names the earlier witness bare and qualified both.
    offers_bare: bool,
}

impl Visible {
    /// The same route: one relation under one spelling. An edge's endpoints
    /// are several routes to one relation, and each is kept.
    fn same_route(&self, other: &Visible) -> bool {
        self.relation == other.relation && self.answer == other.answer
    }

    /// The same route, held again by another frontier.
    fn duplicate(&self) -> Visible {
        Visible {
            relation: self.relation,
            answer: self.answer,
            ports: self.ports.clone(),
            offers_bare: self.offers_bare,
        }
    }
}

impl Frontier {
    /// A relation that answers for itself: what reaches it is the answer
    /// its own birth recorded.
    fn of(relation: SemanticRelation) -> Self {
        Frontier {
            visible: vec![Visible {
                relation,
                answer: None,
                ports: None,
                offers_bare: false,
            }],
            correlates: Vec::new(),
            owed: Vec::new(),
        }
    }

    /// A publication nothing reaches qualified — an argumentative access
    /// publishes bare binders and activates no name.
    fn bare_only() -> Self {
        Frontier {
            visible: Vec::new(),
            correlates: Vec::new(),
            owed: Vec::new(),
        }
    }

    /// A SET OPERATION'S RESULT over its two arms: what answers over it is
    /// the merged heading it published, and every route either arm held
    /// — an arm's own, or the arms of an inner operation — stays open to
    /// the correlation attached to it.
    fn bag(result: SemanticRelation, left: &Frontier, right: &Frontier) -> Self {
        let mut frontier = Frontier::of(result);
        for seen in left
            .correlates
            .iter()
            .chain(left.visible.iter())
            .chain(right.correlates.iter())
            .chain(right.visible.iter())
        {
            if !frontier.correlates.iter().any(|mine| mine.same_route(seen)) {
                frontier.correlates.push(seen.duplicate());
            }
        }
        frontier
    }

    /// A REPUBLICATION KEEPS ITS OPERAND REACHABLE — a scope-preserving
    /// form leaves every route its operand held open above it, and the
    /// correlation routes the operand carried stay open to a condition in
    /// the result's row.
    fn also_through_all(&mut self, other: &Frontier) {
        for seen in &other.visible {
            if !self.visible.iter().any(|mine| mine.same_route(seen)) {
                self.visible.push(seen.duplicate());
            }
        }
        for seen in &other.correlates {
            if !self.correlates.iter().any(|mine| mine.same_route(seen)) {
                self.correlates.push(seen.duplicate());
            }
        }
    }

    /// A relation bound under a spelling the act chose: a drilled interior
    /// answers to the name of the column it was drilled out of.
    fn also_through_as(&mut self, relation: SemanticRelation, answer: Sym) {
        if !self
            .visible
            .iter()
            .any(|seen| seen.relation == relation && seen.answer == Some(answer))
        {
            self.visible.push(Visible {
                relation,
                answer: Some(answer),
                ports: None,
                offers_bare: false,
            });
        }
    }

    /// A NAME THAT REACHES SOME OF A RELATION'S POSITIONS: an edge's
    /// endpoint reaches the columns that belong to that endpoint, out of
    /// the one heading the edge publishes.
    fn also_reaching(&mut self, relation: SemanticRelation, answer: Sym, ports: Vec<PortId>) {
        self.visible.push(Visible {
            relation,
            answer: Some(answer),
            ports: Some(ports),
            offers_bare: false,
        });
    }

    /// A sibling truth witness becomes reachable, bare and qualified.
    fn also_witness(&mut self, relation: SemanticRelation) {
        match self
            .visible
            .iter_mut()
            .find(|seen| seen.relation == relation && seen.answer.is_none())
        {
            Some(seen) => seen.offers_bare = true,
            None => self.visible.push(Visible {
                relation,
                answer: None,
                ports: None,
                offers_bare: true,
            }),
        }
    }

    /// AN EXPORT REPLACES WHAT ANSWERS. An authored alias publishes a new
    /// relation the prior spellings do not reach around; what this
    /// frontier still owes is unchanged.
    fn now_answers_for(&mut self, relation: SemanticRelation) {
        self.visible.clear();
        self.visible.push(Visible {
            relation,
            answer: None,
            ports: None,
            offers_bare: false,
        });
    }

    /// TWO OPERANDS BECOME ONE ROW — the join. What answers is what
    /// answered over both; what is owed is what both owed and what the
    /// join's own deferred condition owes.
    fn merged(mut self, other: Frontier, owed: Vec<ColumnReference>) -> Frontier {
        for seen in other.visible {
            match self.visible.iter_mut().find(|mine| mine.same_route(&seen)) {
                Some(mine) => mine.offers_bare |= seen.offers_bare,
                None => self.visible.push(seen),
            }
        }
        self.owed.extend(other.owed);
        self.owed.extend(owed);
        self
    }

    /// What this position still owes, to read.
    pub(crate) fn owes(&self) -> &[ColumnReference] {
        &self.owed
    }

    fn relations(&self) -> impl Iterator<Item = SemanticRelation> + '_ {
        self.visible.iter().map(|seen| seen.relation)
    }

    /// The routes this frontier holds: each relation under the spelling
    /// that reaches it here.
    fn bindings(&self, registry: &crate::relation::Planning) -> Vec<Binding> {
        Self::routes(&self.visible, registry)
    }

    /// The routes a correlation standing in this relation's row may take
    /// beyond what answers over it.
    fn correlation_bindings(&self, registry: &crate::relation::Planning) -> Vec<Binding> {
        Self::routes(&self.correlates, registry)
    }

    fn routes(seen: &[Visible], registry: &crate::relation::Planning) -> Vec<Binding> {
        seen.iter()
            .map(|seen| Binding {
                relation: seen.relation,
                answer: seen
                    .answer
                    .or_else(|| registry.answers_to(seen.relation.scope())),
                ports: seen.ports.clone(),
            })
            .collect()
    }

    fn witness_ports(&self, registry: &crate::relation::Planning) -> Result<Vec<PortId>> {
        let mut ports = Vec::new();
        for seen in self.visible.iter().filter(|seen| seen.offers_bare) {
            ports.extend(crate::relation::published_ports(registry, &seen.relation)?);
        }
        Ok(ports)
    }
}

/// What a correlated condition's names did, gathered as they resolve.
///
/// A reference reaching out of an interior relation is a correlation when
/// the condition also names a column of that relation, and a mistake when
/// it does not — the same act, judged by its company. No single reference
/// knows which it is, so the fact is accumulated over the whole condition
/// and read once at the end.
///
/// A VERDICT, not a record: it says whether the condition correlates, and
/// nothing about which occurrences it read. What a hoisted correlation owes
/// is derived by the relation authority's own act from the condition and
/// the relation it stands on; a list carried from here would be a second
/// authority for the same fact, and a list is something a caller can pair
/// with the wrong condition.
#[derive(Clone, Copy, Debug)]
struct Witness {
    /// Some reference landed on the relation under the reader's finger.
    anchored: bool,
    /// Some reference — at this position or nested inside it, spelled by
    /// name or by position — was answered past the relation under the
    /// reader's finger: by an earlier frame here or by an enclosing row.
    escaped: bool,
    /// HOW MANY INTERIOR BOUNDARIES THE ENCLOSING JOIN EVALUATES lie
    /// between this position and the farthest answer: none, and every
    /// answer stands in the statement this position's level emits; one,
    /// and an answer stands in the row the join evaluating this interior
    /// reads; two or more, and an answer stands in a row no such join can
    /// read.
    crossings: u8,
    /// HOW MANY POSITIONS OUT the farthest answer stands: none, and every
    /// answer is a frame of this position — the statement its level emits
    /// or a member standing beside it; one or more, and an answer stands in
    /// a row enclosing this position, readable in place where no hoisted
    /// boundary lies between.
    positions_out: u8,
}

impl Witness {
    /// Whether some name was bound inside the interior relation.
    fn is_anchored(&self) -> bool {
        self.anchored
    }

    /// Whether some name reached out to the enclosing row.
    fn has_escaped(&self) -> bool {
        self.escaped
    }

    /// Whether the condition is a CORRELATION: it reads the enclosing row
    /// and the interior relation both.
    fn correlates(&self) -> bool {
        self.anchored && self.escaped
    }

    /// Whether some answer stands in a row enclosing this position.
    fn reaches_enclosing(&self) -> bool {
        self.positions_out >= 1
    }

    /// Whether some answer stands past an interior boundary the enclosing
    /// join evaluates — outside the statement this position's level emits.
    fn reaches_past_interior(&self) -> bool {
        self.crossings >= 1
    }

    /// Whether some answer stands past the join that evaluates the
    /// interior this position is in — a row no join can read.
    fn reaches_beyond_join(&self) -> bool {
        self.crossings >= 2
    }

    fn none() -> Self {
        Witness {
            anchored: false,
            escaped: false,
            crossings: 0,
            positions_out: 0,
        }
    }
}

/// RUN ONE LEXICAL EXTENT: the marks are cleared before the operation and
/// read after it — a nested position's lookups mark this one as they reach
/// past it — and what the operation resolved comes out OWNED BESIDE what
/// its lookups reached, as one [`Judged`]. Begin and end are private to
/// this module, `Witness` has no public mint and leaves this module only
/// inside a `Judged`, and the extent is opened only under a [`Role`] — a
/// token only the fold's own module can make — so the product reaches
/// exactly the fold's operation-owned acts, and the only roads out of it
/// are the consuming judgments below.
pub(in crate::pipeline::resolver) fn extent<'reg, 'db, T>(
    fold: &mut super::resolver_fold::ResolverFold<'reg, 'db>,
    op: impl FnOnce(&mut super::resolver_fold::ResolverFold<'reg, 'db>) -> Result<T>,
    _role: super::resolver_fold::Role,
) -> Result<Judged<T>> {
    let prior = fold.lexical.marks.replace(Witness::none());
    let out = op(fold);
    let witness = fold.lexical.marks.replace(prior);
    Ok(Judged {
        value: out?,
        witness,
        correlations: fold.correlations,
    })
}

/// THE PRODUCT OF ONE LEXICAL EXTENT: what the operation resolved, owned
/// beside EVERY fact its lexical evaluation judgment is derived from — the
/// witness of what its lookups reached, and the evaluation point of the
/// fold that ran the extent — one thing from the moment the extent
/// closes. Born only in [`extent`]; its fields are private; it is neither
/// `Clone` nor `Copy`; no accessor answers any part alone, and no consumer
/// supplies or substitutes a part later. The value leaves only through a
/// consuming judgment that spends the witness and the evaluation point on
/// that very value in the same act: the here-only judgment, the
/// restriction verdict, and the relation authority's stating of a
/// position. There is no road by which a witness earned by one extent, or
/// the mode of one fold, meets a value resolved by another.
pub(crate) struct Judged<T> {
    value: T,
    witness: Witness,
    /// The evaluation point of the fold that ran the extent, read when the
    /// extent closed: in place, or hoisted to the enclosing join.
    correlations: super::Correlations,
}

impl<T> Judged<T> {
    /// THE HERE-ONLY JUDGMENT: answer the value only after a read past the
    /// interior boundary the enclosing join evaluates has been refused —
    /// the level that consumes it is emitted inside that boundary, where
    /// the enclosing row is not readable. The value that comes out has
    /// been judged to read nothing an evaluation would have to record.
    pub(crate) fn here(self, what: &str) -> Result<T> {
        if self.witness.reaches_past_interior() {
            return Err(crate::relation::enclosing_position_refusal(what));
        }
        Ok(self.value)
    }

    /// THE STATING SPLIT, for the relation authority alone: the value and
    /// the evaluation its extent earned under the evaluation point of the
    /// fold that ran it — here when nothing enclosing was reached;
    /// enclosing otherwise, past the join when two hoisted boundaries were
    /// crossed. The halves come apart only under a
    /// [`Stating`](crate::relation::pending::Stating) license, which only
    /// `relation::pending` mints and spends inside the acts that write
    /// them into one position in the same breath.
    pub(crate) fn into_stated(
        self,
        _stating: crate::relation::pending::Stating,
    ) -> (T, crate::relation::pending::Evaluation) {
        use crate::relation::pending::{EnclosingReach, Evaluation};
        let evaluation = if !self.witness.reaches_enclosing() {
            Evaluation::Here
        } else {
            Evaluation::Enclosing {
                correlations: self.correlations,
                reach: if self.witness.reaches_beyond_join() {
                    EnclosingReach::PastTheJoin
                } else {
                    EnclosingReach::TheJoin
                },
            }
        };
        (self.value, evaluation)
    }
}

impl Judged<crate::pipeline::asts::core::TruthExpression<crate::pipeline::asts::core::Resolved>> {
    /// THE RESTRICTION VERDICT: the condition answered inside the verdict
    /// its lookups earned under the evaluation point of the fold that ran
    /// the extent. The refusing verdicts carry no condition; the using
    /// verdicts carry it.
    pub(in crate::pipeline::resolver) fn restriction(self) -> super::resolver_fold::Restriction {
        use super::resolver_fold::Restriction;
        let Judged {
            value,
            witness,
            correlations,
        } = self;
        if witness.has_escaped() && !witness.is_anchored() {
            return Restriction::ConstrainsNothing;
        }
        if correlations == super::Correlations::Hoisted && witness.correlates() {
            if witness.reaches_beyond_join() {
                return Restriction::BeyondReach;
            }
            return Restriction::Correlated(value);
        }
        Restriction::Plain(value)
    }
}

/// HOW FAR A BARE NAME REACHES from the frame it is written over.
///
/// A CONDITION constrains a row: a bare name it writes binds in the
/// relation under the reader's finger first and, absent there, in the
/// enclosing row it is correlated to. A PIPE FORM consumes its input: a
/// bare name it writes selects over that input's heading and nowhere
/// else, while a qualified name still reaches every relation in view —
/// which is what lets `|> (u.id)` inside a correlated interior name the
/// outer `u`, and what keeps `|> (id)` from silently selecting an outer
/// column the interior never published.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reach {
    Row,
    Stage,
    /// The frame alone, bare and qualified: a set operation's whole-heading
    /// correlation names its own operands, and a destructuring reads its
    /// document from the source it stands on.
    Local,
}

/// WHAT A FRAME STANDS OVER: one carrier an act of [`standing`] produced,
/// or the row of carriers one call bound — minted HERE from the call's own
/// record, never assembled by a caller.
enum Standing {
    Carrier(ResolvedRelation),
    Row(Vec<ResolvedRelation>),
}

impl Standing {
    fn carriers(&self) -> impl Iterator<Item = &ResolvedRelation> {
        match self {
            Standing::Carrier(carrier) => std::slice::from_ref(carrier).iter(),
            Standing::Row(carriers) => carriers.iter(),
        }
    }
}

/// ONE ROW UNDER THE READER'S FINGER — entered by the operation that
/// consumes the row and left by that operation — and how far a bare name
/// written over it reaches.
struct Frame {
    standing: Standing,
    reach: Reach,
}

impl Frame {
    fn bindings(&self, reach: Reach, registry: &crate::relation::Planning) -> Vec<Binding> {
        let mut bindings = Vec::new();
        for carrier in self.standing.carriers() {
            bindings.extend(carrier.frontier().bindings(registry));
            // A CONDITION IN THE ROW reaches what a correlation attached
            // to the carrier may: a set operation's arms answer `x.k` in
            // `x ; y, x.k = y.k` and `x.*` in `x ; y, x.* = y.*`. A form
            // over the carrier does not.
            if matches!(reach, Reach::Row | Reach::Local) {
                bindings.extend(carrier.frontier().correlation_bindings(registry));
            }
        }
        bindings
    }
}

/// Where, among one position's own frames, an answered port stands.
enum Holding {
    Innermost,
    Earlier,
    Nowhere,
}

/// THE FOLD'S LEXICAL POSITION: the relations under the reader's finger,
/// innermost last, and the enclosing fold's position behind them.
///
/// Frames are OWNED carriers, entered and left by value. The enclosing
/// position is BORROWED for exactly as long as the interior fold that
/// sees it lives, so an interior never holds a copy of what encloses it
/// and the enclosing fold cannot move while it is seen.
pub(crate) struct Position<'e> {
    frames: Vec<Frame>,
    enclosing: Option<&'e Position<'e>>,
    /// The innermost frame is read as part of the row, not as a scope of
    /// its own: an anonymous literal's headers and cells, and a call's
    /// authored arguments, are decided over everything in view at once.
    flat: bool,
    /// WHAT THE LOOKUPS MADE HERE, OR NESTED HERE, REACHED. Written by the
    /// one address judgment for every spelling, from the frame the answer
    /// stood in: a nested position marks every position it escapes
    /// through, so an interior learns that something inside it — a
    /// witness's body, a scalar subquery — read the row enclosing it. A
    /// cell, because a nested position holds this one by shared borrow.
    /// The operation judging one construct resets it before and reads it
    /// after; nothing else reads it.
    marks: std::cell::Cell<Witness>,
    /// WHERE THIS POSITION IS EVALUATED relative to the one enclosing it:
    /// in place, as the target's own subquery, or at the enclosing join,
    /// which is what makes leaving this position a crossing the judgment
    /// counts.
    mode: super::Correlations,
    /// THE LOOKUP RESTATES: the reference being answered is a publication
    /// item that carries its position rather than a value that reads it,
    /// so a position the enclosing join computes may answer — it continues,
    /// unread. Set for exactly one lookup by the publication that makes it.
    carrying: std::cell::Cell<bool>,
}

impl<'e> Position<'e> {
    /// A position with nothing behind it — the prompt, or a closed world
    /// such as a definition body or a relation actual.
    pub(crate) fn root() -> Self {
        Position {
            frames: Vec::new(),
            enclosing: None,
            flat: false,
            marks: std::cell::Cell::new(Witness::none()),
            mode: super::Correlations::InPlace,
            carrying: std::cell::Cell::new(false),
        }
    }

    /// A position INSIDE another: an interior expression sees the row it
    /// is correlated to through this borrow and through nothing else, and
    /// states where it is evaluated relative to that row.
    pub(crate) fn enclosed_by(outer: &'e Position<'e>, mode: super::Correlations) -> Self {
        Position {
            frames: Vec::new(),
            enclosing: Some(outer),
            flat: false,
            marks: std::cell::Cell::new(Witness::none()),
            mode,
            carrying: std::cell::Cell::new(false),
        }
    }

    /// STATE THAT THE NEXT LOOKUPS RESTATE a position rather than read it —
    /// a publication item that is a bare reference — answering the prior
    /// setting so the publication can put it back.
    pub(crate) fn set_carrying(&self, carrying: bool) -> bool {
        self.carrying.replace(carrying)
    }

    /// THE ONE JUDGMENT AFTER SELECTION, for every spelling and every
    /// selection road: which frame the answer stood in decides what the
    /// reference reached. Landing on the relation under the reader's finger
    /// anchors this position; landing anywhere behind it — an earlier frame
    /// here, or an enclosing row — is an escape, and a position the answer
    /// lies entirely outside of is escaped THROUGH, so the enclosing
    /// positions are judged in turn until one holds the answer. A
    /// publication that selects an ordinal's occurrence by its own
    /// enumeration ([`Position::in_order`]) records the judgment here too.
    pub(crate) fn judged(
        &self,
        occurrence: &ColumnOccurrence,
        registry: &crate::relation::Planning,
    ) -> Result<()> {
        self.mark(occurrence.column, registry)
    }

    fn mark(&self, port: PortId, registry: &crate::relation::Planning) -> Result<()> {
        // The positions the answer lies outside of, innermost first, and
        // the one holding it.
        let mut passed: Vec<&Position<'_>> = Vec::new();
        let mut position: Option<&Position<'_>> = Some(self);
        let mut holder: Option<(&Position<'_>, Holding)> = None;
        while let Some(here) = position {
            match here.holding(port, registry)? {
                Holding::Nowhere => {
                    passed.push(here);
                    position = here.enclosing;
                }
                held => {
                    holder = Some((here, held));
                    break;
                }
            }
        }
        if let Some((here, held)) = holder {
            let mut marks = here.marks.get();
            match held {
                Holding::Innermost => marks.anchored = true,
                Holding::Earlier | Holding::Nowhere => marks.escaped = true,
            }
            here.marks.set(marks);
        }
        // EACH POSITION PASSED COUNTS THE HOISTED BOUNDARIES between itself
        // and the answer: its own, if the enclosing join evaluates it, and
        // every one it is nested in up to the holder.
        let mut crossings: u8 = 0;
        let mut positions_out: u8 = 0;
        for here in passed.iter().rev() {
            if here.mode == super::Correlations::Hoisted {
                crossings = crossings.saturating_add(1);
            }
            positions_out = positions_out.saturating_add(1);
            let mut marks = here.marks.get();
            marks.escaped = true;
            marks.crossings = marks.crossings.max(crossings);
            marks.positions_out = marks.positions_out.max(positions_out);
            here.marks.set(marks);
        }
        Ok(())
    }

    /// Which of this position's own frames reaches a port: the innermost
    /// (every frame, when the row is read flat), an earlier one, or none.
    fn holding(&self, port: PortId, registry: &crate::relation::Planning) -> Result<Holding> {
        let count = self.frames.len();
        for (index, frame) in self.frames.iter().enumerate().rev() {
            if Self::frame_reaches(frame, port, registry)? {
                return Ok(if self.flat || index + 1 == count {
                    Holding::Innermost
                } else {
                    Holding::Earlier
                });
            }
        }
        Ok(Holding::Nowhere)
    }

    /// Whether a frame reaches a port: the positions its carriers publish,
    /// their witness ports, and every position of a relation the frame's
    /// routes name — a join member's own arm, which a qualified reference
    /// reaches beneath the join's republication of it, and the operands a
    /// correlation attached to the carrier may name.
    fn frame_reaches(
        frame: &Frame,
        port: PortId,
        registry: &crate::relation::Planning,
    ) -> Result<bool> {
        if Self::ports_of(frame, registry)?.contains(&port) {
            return Ok(true);
        }
        for binding in frame.bindings(frame.reach, registry) {
            let reached = match &binding.ports {
                Some(ports) => ports.contains(&port),
                None => registry
                    .authority()
                    .interface(&binding.relation)
                    .is_ok_and(|interface| interface.ports().contains(&port)),
            };
            if reached {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Enter a frame: the operation about to resolve stands over this
    /// relation, reaching as far as `reach` says.
    pub(crate) fn enter(&mut self, standing: ResolvedRelation, reach: Reach) {
        self.frames.push(Frame {
            standing: Standing::Carrier(standing),
            reach,
        });
    }

    /// Leave the innermost frame, handing its carrier back to the
    /// operation that entered it. Leaving a frame nobody entered, or a
    /// row of carriers, is a resolver defect, not a case.
    pub(crate) fn leave(&mut self) -> ResolvedRelation {
        match self
            .frames
            .pop()
            .expect("a frame is left by the operation that entered it")
            .standing
        {
            Standing::Carrier(carrier) => carrier,
            Standing::Row(_) => unreachable!("a row of carriers is left by leave_carriers"),
        }
    }

    /// STAND OVER THE CARRIERS A CALL BOUND, as one row: every formal the
    /// call's record holds, read with row reach — a name any carrier
    /// publishes is the row's, a name two publish is ambiguous in it.
    /// The frame is minted here from the record's own carriers. Answers
    /// whether a row was entered at all: a call that bound no carrier has
    /// no row to stand over.
    pub(crate) fn enter_carriers(
        &mut self,
        record: &crate::defuse::carriers::CarrierRecord,
        registry: &crate::relation::Planning,
    ) -> Result<bool> {
        let rows = record.formal_rows();
        if rows.is_empty() {
            return Ok(false);
        }
        let carriers = rows
            .into_iter()
            .map(|row| read_of(row, registry))
            .collect::<Result<Vec<_>>>()?;
        self.frames.push(Frame {
            standing: Standing::Row(carriers),
            reach: Reach::Row,
        });
        Ok(true)
    }

    /// STAND OVER THE CARRIER THE CALLER ROW BECAME, as a stage: a bare
    /// name selects over that carrier alone. Which carrier that is, the
    /// record says. Answers `false` when the record names none.
    pub(crate) fn enter_landing(
        &mut self,
        record: &crate::defuse::carriers::CarrierRecord,
        registry: &crate::relation::Planning,
    ) -> Result<bool> {
        let Some(row) = record.landing_row() else {
            return Ok(false);
        };
        self.frames.push(Frame {
            standing: Standing::Carrier(read_of(row, registry)?),
            reach: Reach::Stage,
        });
        Ok(true)
    }

    /// Leave a frame entered over a call's carriers. Nothing comes back:
    /// the record still holds the carriers.
    pub(crate) fn leave_carriers(&mut self) {
        self.frames
            .pop()
            .expect("a frame is left by the operation that entered it");
    }

    /// READ THE ROW FLAT for the duration of one operation: an anonymous
    /// literal's headers and cells, and a call's authored arguments, are
    /// decided over everything in view at once, with no frame of their own
    /// to shadow the rest.
    pub(crate) fn flatly<R>(&mut self, operation: impl FnOnce(&mut Self) -> R) -> R {
        let was = self.set_flat(true);
        let out = operation(self);
        self.set_flat(was);
        out
    }

    /// Set whether the row is read flat, answering the prior setting so the
    /// operation that changed it can restore it.
    pub(crate) fn set_flat(&mut self, flat: bool) -> bool {
        std::mem::replace(&mut self.flat, flat)
    }

    /// The relation under the reader's finger, to ask questions of. A
    /// row of carriers is no one relation and answers with none.
    pub(crate) fn current(&self) -> Option<&ResolvedRelation> {
        match self.frames.last().map(|frame| &frame.standing) {
            Some(Standing::Carrier(carrier)) => Some(carrier),
            Some(Standing::Row(_)) | None => None,
        }
    }

    /// WHETHER THE ROW IN VIEW ANSWERS A SPELLING — a bare name some
    /// position publishes, or a qualifier some relation answers to. A
    /// diagnostic question: it decides which refusal to teach, never what
    /// a reference means.
    pub(crate) fn answers_spelling(
        &self,
        reference: &str,
        registry: &crate::relation::Planning,
    ) -> bool {
        let sym = |text: &str| registry.canonical(registry.intern(text, false));
        match reference.split_once('.') {
            Some((qualifier, _)) => {
                let wanted = sym(qualifier);
                self.all_visible(registry)
                    .iter()
                    .any(|binding| binding.answer == Some(wanted))
            }
            None => {
                let wanted = sym(reference);
                self.all_ports(registry).is_ok_and(|ports| {
                    ports
                        .iter()
                        .any(|port| registry.published_sym(port.column()) == Some(wanted))
                })
            }
        }
    }

    /// Whether anything encloses the innermost frame — an outer row an
    /// interior may be correlated to.
    pub(crate) fn has_enclosing(&self) -> bool {
        self.frames.len() > 1 || self.enclosing.is_some()
    }

    /// Whether a row stands here at all — for a relation about to be
    /// entered, whether it will have an enclosing row to look left into.
    pub(crate) fn encloses_a_row(&self) -> bool {
        !self.frames.is_empty() || self.enclosing.is_some()
    }

    /// EVERY POSITION IN VIEW — the frames standing here and everything
    /// enclosing them, sibling truth witnesses included — for the row a
    /// binder may reuse. Publication, not permission: these are the
    /// positions the relations publish to anyone.
    pub(crate) fn ports_in_view(
        &self,
        registry: &crate::relation::Planning,
    ) -> Result<Vec<PortId>> {
        self.all_ports(registry)
    }

    /// THE ROW IN VIEW — every position the relations standing here and
    /// enclosing them PUBLISH — for the dequalifying correlation that pairs
    /// a read's columns with the row it looks left into by name. A sibling
    /// truth witness (`+orders(...)` beside the read) is addressable by
    /// name but is not a row: a `.(id)` pairs with the row's `id`, never
    /// with a witness's.
    pub(crate) fn row_in_view(&self, registry: &crate::relation::Planning) -> Result<Vec<PortId>> {
        let mut ports = Vec::new();
        let mut positions: Vec<&Position<'_>> = vec![self];
        let mut outer = self.enclosing;
        while let Some(position) = outer {
            positions.push(position);
            outer = position.enclosing;
        }
        for position in positions {
            for frame in position.frames.iter().rev() {
                for carrier in frame.standing.carriers() {
                    for port in
                        crate::relation::published_ports(registry, &carrier.semantic_relation())?
                    {
                        if !ports.contains(&port) {
                            ports.push(port);
                        }
                    }
                }
            }
        }
        Ok(ports)
    }

    /// The innermost frame and the reach it is read with, unless the row is
    /// being read flat.
    fn local(&self) -> Option<(&Frame, Reach)> {
        if self.flat {
            return None;
        }
        self.frames.last().map(|frame| (frame, frame.reach))
    }

    /// Every frame behind the innermost one, here and in every enclosing
    /// position, outermost last.
    fn enclosing_frames(&self) -> Vec<&Frame> {
        let mut frames: Vec<&Frame> = Vec::new();
        let own = if self.flat {
            self.frames.len()
        } else {
            self.frames.len().saturating_sub(1)
        };
        frames.extend(self.frames[..own].iter().rev());
        let mut outer = self.enclosing;
        while let Some(position) = outer {
            frames.extend(position.frames.iter().rev());
            outer = position.enclosing;
        }
        frames
    }

    fn ports_of(frame: &Frame, registry: &crate::relation::Planning) -> Result<Vec<PortId>> {
        let mut ports = Vec::new();
        for carrier in frame.standing.carriers() {
            ports.extend(crate::relation::published_ports(
                registry,
                &carrier.semantic_relation(),
            )?);
            ports.extend(carrier.frontier().witness_ports(registry)?);
        }
        Ok(ports)
    }

    /// THE BARE INTERFACE of the innermost frame: the positions a bare
    /// reference or a publication may select over. Publication, not
    /// permission — the same positions the relation publishes to anyone.
    pub(crate) fn local_ports(&self, registry: &crate::relation::Planning) -> Result<Vec<PortId>> {
        match self.local() {
            Some((frame, _)) => Self::ports_of(frame, registry),
            None => Ok(Vec::new()),
        }
    }

    fn local_visible(&self, registry: &crate::relation::Planning) -> Vec<Binding> {
        self.local()
            .map(|(frame, reach)| frame.bindings(reach, registry))
            .unwrap_or_default()
    }

    fn enclosing_ports(&self, registry: &crate::relation::Planning) -> Result<Vec<PortId>> {
        let mut ports = Vec::new();
        for frame in self.enclosing_frames() {
            for port in Self::ports_of(frame, registry)? {
                if !ports.contains(&port) {
                    ports.push(port);
                }
            }
        }
        Ok(ports)
    }

    fn all_visible(&self, registry: &crate::relation::Planning) -> Vec<Binding> {
        let mut bindings = self.local_visible(registry);
        for frame in self.enclosing_frames() {
            for binding in frame.bindings(frame.reach, registry) {
                if !bindings
                    .iter()
                    .any(|seen| seen.relation == binding.relation && seen.answer == binding.answer)
                {
                    bindings.push(binding);
                }
            }
        }
        bindings
    }

    fn all_ports(&self, registry: &crate::relation::Planning) -> Result<Vec<PortId>> {
        let mut ports = self.local_ports(registry)?;
        for port in self.enclosing_ports(registry)? {
            if !ports.contains(&port) {
                ports.push(port);
            }
        }
        Ok(ports)
    }

    /// Whether a relation in view publishes dimensions the target never
    /// described — for the refusal that must not call a name absent from
    /// an enumeration that never happened.
    pub(crate) fn any_opaque(&self, registry: &crate::relation::Planning) -> Result<bool> {
        let relations: Vec<SemanticRelation> = self
            .all_visible(registry)
            .into_iter()
            .map(|binding| binding.relation)
            .collect();
        crate::relation::any_interface_opaque(registry, &relations)
    }

    /// THE ONE ADDRESS JUDGMENT. A written reference — bare, qualified, or
    /// ordinal — is decided over the frames standing here and answers with
    /// a port, an exhaustive ambiguity, an absence, or a refusal. The set
    /// it is decided over is assembled here and reaches no caller. What
    /// the answer reached is judged once, after selection, the same way
    /// for every spelling, and recorded on this position and on every
    /// position the answer lies outside of.
    pub(crate) fn address(
        &self,
        reference: ColumnReference,
        in_correlation: bool,
        registry: &crate::relation::Planning,
    ) -> Result<UnificationResult> {
        let result = self.select(reference, in_correlation, registry)?;
        if let UnificationResult::Resolved(occurrence) = &result {
            // A POSITION THE ENCLOSING JOIN COMPUTES is published by the
            // interior and readable only past its boundary; a reference
            // answered by that very position stands inside the interior.
            if !self.carrying.get()
                && crate::relation::evaluated_at_boundary(registry, occurrence.column)
            {
                return Ok(UnificationResult::Refused(
                    crate::relation::enclosing_position_refusal("a reference inside the interior"),
                ));
            }
            self.mark(occurrence.column, registry)?;
        }
        Ok(result)
    }

    /// THE SELECTION: which occurrence a spelling names, over the frames
    /// standing here. A name and an ordinal select differently — one by
    /// publication, one by position within the scope its qualifier chooses
    /// — and take the same tiers: the reach of the frame under the reader's
    /// finger, and under row reach the interior relation first, the whole
    /// view only where the interior proves the spelling absent.
    fn select(
        &self,
        reference: ColumnReference,
        in_correlation: bool,
        registry: &crate::relation::Planning,
    ) -> Result<UnificationResult> {
        let Some((local, reach)) = self.local() else {
            // Nothing stands here: only what encloses this position can
            // answer, and it answers as one row.
            return Ok(lookup::unify_single_column(
                reference,
                &self.enclosing_ports(registry)?,
                &self.all_visible(registry),
                registry,
            ));
        };
        let local_ports = Self::ports_of(local, registry)?;
        let qualifier = match &reference {
            ColumnReference::Named { qualifier, .. }
            | ColumnReference::Ordinal { qualifier, .. } => qualifier.as_deref(),
        };
        match reach {
            // A PIPE FORM selects over its own input: a bare name reaches
            // that heading and nothing beyond it; a qualified name reaches
            // every relation in view.
            Reach::Stage => Ok(lookup::unify_single_column(
                reference,
                &local_ports,
                &self.all_visible(registry),
                registry,
            )),
            Reach::Local => Ok(lookup::unify_single_column(
                reference,
                &local_ports,
                &self.local_visible(registry),
                registry,
            )),
            Reach::Row => {
                // An interior relation is the lexical scope under the
                // reader's finger. Search it before the enclosing context,
                // including for a qualified reference: a second `addresses`
                // interior shadows an earlier sibling named `addresses`. A
                // different qualifier widens only after the local heading
                // proves it absent. An ordinal counts positions within the
                // scope its qualifier chooses, and counts the interior's
                // first: a position past its width is absent from it.
                //
                // `_` is exempt. Narrowing is LEXICAL SHADOWING — an inner
                // relation named `addresses` hides an outer one — and
                // shadowing needs a name to shadow. `_` has none: it points
                // at the one unnamed pipe output in view, and deciding
                // whether there is exactly one means enumerating them all.
                let points_at_a_pipe = qualifier == Some("_");
                let has_enclosing = !self.enclosing_ports(registry)?.is_empty();
                let narrowed =
                    !points_at_a_pipe && (has_enclosing || (in_correlation && qualifier.is_none()));
                if !narrowed {
                    return Ok(lookup::unify_single_column(
                        reference,
                        &self.all_ports(registry)?,
                        &self.all_visible(registry),
                        registry,
                    ));
                }
                match lookup::unify_single_column(
                    reference.clone(),
                    &local_ports,
                    &self.local_visible(registry),
                    registry,
                ) {
                    // ABSENT from the inner relation is not a miss. A
                    // correlated subquery stands inside a statement, and
                    // a name the subquery's own source does not publish
                    // is what the enclosing row is there to answer.
                    // Widen only on absence: a name the inner relation
                    // claims ambiguously is still the inner relation's.
                    UnificationResult::Unresolved(_) | UnificationResult::Refused(_) => {
                        Ok(lookup::unify_single_column(
                            reference,
                            &self.all_ports(registry)?,
                            &self.all_visible(registry),
                            registry,
                        ))
                    }
                    settled => Ok(settled),
                }
            }
        }
    }

    /// EVERY REFERENCE A FORM OWES, decided at once over the frame it will
    /// consume, each answered with its port or the refusal it earned.
    pub(crate) fn resolve_all(
        &self,
        references: Vec<ColumnReference>,
        registry: &crate::relation::Planning,
        error_context: &str,
    ) -> Result<Vec<PortId>> {
        use crate::error::DelightQLError;
        if references.is_empty() {
            return Ok(Vec::new());
        }
        let ports = self.local_ports(registry)?;
        let visible = self.all_visible(registry);
        let mut resolved = Vec::with_capacity(references.len());
        for reference in references {
            match lookup::unify_single_column(reference, &ports, &visible, registry) {
                UnificationResult::Resolved(occurrence) => resolved.push(occurrence.column),
                UnificationResult::Unresolved(name) => {
                    // A name cannot be reported absent from an enumeration
                    // that never happened.
                    let relations: Vec<SemanticRelation> =
                        visible.iter().map(|binding| binding.relation).collect();
                    if crate::relation::any_interface_opaque(registry, &relations)? {
                        return Err(
                            crate::pipeline::resolver::resolving::domain_expressions::simple::opaque_heading_refusal(),
                        );
                    }
                    return Err(DelightQLError::from(Resolution::Column {
                        column: name.to_string(),
                        context: error_context.to_string(),
                    }));
                }
                UnificationResult::Opaque => {
                    return Err(crate::pipeline::resolver::opaque_reference_refusal());
                }
                UnificationResult::Refused(refusal) => return Err(refusal),
                UnificationResult::Ambiguous { column, tables } => {
                    return Err(DelightQLError::from(Constraint::General {
                        message: format!(
                            "Column '{}' {} is ambiguous. Could refer to: {}",
                            column,
                            error_context,
                            tables.join(", ")
                        ),
                    }));
                }
            }
        }
        Ok(resolved)
    }

    /// `q.*` — every position the qualifier reaches, in the order the
    /// relation it names publishes them. A qualifier that names no
    /// relation in view is the same refusal a qualified name meets.
    pub(crate) fn qualified_glob(
        &self,
        qualifier: &SqlIdentifier,
        registry: &crate::relation::Planning,
    ) -> Result<Vec<ColumnOccurrence>> {
        let reached = lookup::qualify_ports(qualifier, &self.all_visible(registry), registry)?;
        // ONE COLUMN OFFERED AT TWO LEVELS IS ONE ANSWER. A relation the
        // qualifier names may have been republished by the relation
        // standing here — a drilled context, a join's carried operand — and
        // the position the glob means is the one standing here. The carry
        // record says which; nothing is paired by name or order.
        let standing = self.all_ports(registry)?;
        let reached: Vec<PortId> = reached
            .into_iter()
            .filter_map(|port| {
                if standing.contains(&port) {
                    return Some(port);
                }
                standing
                    .iter()
                    .copied()
                    .find(|here| crate::relation::stands_where(registry, *here, port))
            })
            .collect();
        // THE JUDGMENT IS THE SAME FOR EVERY SPELLING: a glob over the
        // enclosing row reaches it exactly as each name would.
        for port in &reached {
            self.mark(*port, registry)?;
        }
        Ok(reached
            .into_iter()
            .map(|port| ColumnOccurrence::addressed(port, true, Terminal::judged()))
            .collect())
    }

    /// THE FRAME'S OWN HEADING, as the occurrences a bare `*` addresses:
    /// every position the relation standing here publishes, in order.
    pub(crate) fn heading(
        &self,
        registry: &crate::relation::Planning,
    ) -> Result<Vec<ColumnOccurrence>> {
        let mut ports = Vec::new();
        if let Some((frame, _)) = self.local() {
            for carrier in frame.standing.carriers() {
                ports.extend(crate::relation::published_ports(
                    registry,
                    &carrier.semantic_relation(),
                )?);
            }
        }
        Ok(ports
            .into_iter()
            .map(|port| ColumnOccurrence::addressed(port, false, Terminal::judged()))
            .collect())
    }

    /// EVERY POSITION IN ORDER, as the occurrences an authored ordinal or
    /// range addresses: under a qualifier, what the qualifier reaches;
    /// bare, the frame's own heading without its support positions.
    pub(crate) fn in_order(
        &self,
        qualifier: Option<&SqlIdentifier>,
        registry: &crate::relation::Planning,
    ) -> Result<Vec<ColumnOccurrence>> {
        match qualifier {
            Some(qualifier) => self.qualified_glob(qualifier, registry),
            None => Ok(self
                .heading(registry)?
                .into_iter()
                .filter(|occurrence| {
                    !crate::relation::is_higher_order_support(registry, occurrence.column)
                })
                .collect()),
        }
    }

    /// THE POSITIONS A SPREAD NAMES: every position of the frame's heading
    /// whose published name the spread's pattern matches, as the
    /// occurrences the spread addresses.
    pub(crate) fn spread(
        &self,
        matches: impl Fn(&str) -> bool,
        registry: &crate::relation::Planning,
    ) -> Result<Vec<ColumnOccurrence>> {
        Ok(self
            .heading(registry)?
            .into_iter()
            .filter(|occurrence| {
                registry
                    .published(occurrence.column.column())
                    .map(|name| registry.identifier_of(name))
                    .is_some_and(|name| matches(name.as_str()))
            })
            .collect())
    }

    /// A SET OPERATION'S WHOLE-HEADING CORRELATION names an OPERAND's own
    /// heading, so the scopes the statement still names answer first.
    pub(crate) fn correlation_owner(
        &self,
        qualifier: &SqlIdentifier,
        registry: &crate::relation::Planning,
    ) -> Result<SemanticRelation> {
        use crate::error::DelightQLError;
        let spelling = registry.intern(qualifier.as_str(), qualifier.is_stropped());
        let wanted = registry.canonical(spelling);
        let named: Vec<SemanticRelation> = self
            .local_visible(registry)
            .into_iter()
            .filter(|binding| binding.answer == Some(wanted))
            .map(|binding| binding.relation)
            .collect();
        match named.as_slice() {
            [relation] => Ok(*relation),
            [] => Err(DelightQLError::from(ResolutionSetop::CorrelationOwner {
                message: format!(
                    "set-operation correlation qualifier '{}' does not name a visible operand",
                    qualifier
                ),
            })),
            _ => Err(DelightQLError::from(ResolutionSetop::CorrelationOwner {
                message: format!(
                    "set-operation correlation qualifier '{}' names more than one visible operand",
                    qualifier
                ),
            })),
        }
    }
}

/// THE WITNESS THAT A COMPILER-OWNED ROW IS BEING READ BY THE LEXICAL
/// AUTHORITY. A proof reads itself only when handed one, and only this
/// module constructs one: the identity inside the proof never leaves it
/// on the way to a frame.
pub struct RowRead(());

impl ResolvedRelation {
    /// STANDING OVER A COMPILER-OWNED ROW, by its proof: the authority's
    /// ground read of the row — the preserve law, so the read publishes
    /// the row's own positions — and what answers over it is the row's
    /// birth answer, which a compiler-owned row records as nothing.
    pub(crate) fn over(
        row: crate::defuse::carriers::CompilerRow,
        identities: &crate::relation::Planning,
    ) -> Result<ResolvedRelation> {
        read_of(row, identities)
    }
}

/// The read of a proof, wrapped as what answers for itself — the one way
/// a frame over a compiler-owned row is minted, private to the lexical
/// authority, and the only holder of the witness the proof reads under.
fn read_of(
    row: crate::defuse::carriers::CompilerRow,
    identities: &crate::relation::Planning,
) -> Result<ResolvedRelation> {
    Ok(ResolvedRelation::answering_for_itself(
        row.read(RowRead(()), identities)?,
    ))
}

#[cfg(test)]
mod judged_tests {
    //! The consuming judgments of a judged product read the witness THAT
    //! product owns and nothing else. Witnesses are built here, in the one
    //! module that can, because no road outside `extent` mints one; the
    //! stating split is not exercised here because its license is minted
    //! only by `relation::pending` — the lateral-addressing corpus pins it
    //! end to end.
    use super::*;
    use crate::pipeline::asts::core::{Resolved, TruthExpression};
    use crate::pipeline::resolver::resolver_fold::Restriction;
    use crate::pipeline::resolver::Correlations;

    fn judged<T>(value: T, witness: Witness, correlations: Correlations) -> Judged<T> {
        Judged {
            value,
            witness,
            correlations,
        }
    }

    fn witness(anchored: bool, escaped: bool, crossings: u8, positions_out: u8) -> Witness {
        Witness {
            anchored,
            escaped,
            crossings,
            positions_out,
        }
    }

    fn truth() -> TruthExpression<Resolved> {
        use crate::pipeline::asts::core::{
            Comparison, DomainExpression, FunctionApplication, LiteralValue,
        };
        let one = || {
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Boolean(true)))
        };
        TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(one()),
            right: Box::new(one()),
        })
    }

    /// HERE-ONLY: a value whose lookups stayed inside the statement its
    /// level emits is answered; one that reached past the interior
    /// boundary the enclosing join evaluates is refused, and the value
    /// never comes out.
    #[test]
    fn the_here_only_judgment_reads_the_products_own_witness() {
        let hoisted = Correlations::Hoisted;
        assert_eq!(
            judged(7, witness(true, false, 0, 0), hoisted)
                .here("probe")
                .unwrap(),
            7
        );
        assert_eq!(
            judged(7, witness(false, true, 0, 1), hoisted)
                .here("probe")
                .unwrap(),
            7
        );
        let refused = judged(7, witness(false, true, 1, 1), hoisted)
            .here("probe")
            .unwrap_err();
        assert!(refused.to_string().contains("probe"), "{refused}");
    }

    /// RESTRICTION: the verdict is decided by the witness the condition
    /// earned and the evaluation point the product carries from the fold
    /// that ran it — and the refusing verdicts carry no condition out.
    #[test]
    fn the_restriction_verdict_reads_the_products_own_witness_and_mode() {
        let hoisted = Correlations::Hoisted;
        let plain = judged(truth(), witness(true, false, 0, 0), hoisted).restriction();
        assert!(matches!(plain, Restriction::Plain(_)));
        let nothing = judged(truth(), witness(false, true, 0, 1), hoisted).restriction();
        assert!(matches!(nothing, Restriction::ConstrainsNothing));
        let correlated = judged(truth(), witness(true, true, 1, 1), hoisted).restriction();
        assert!(matches!(correlated, Restriction::Correlated(_)));
        let beyond = judged(truth(), witness(true, true, 2, 2), hoisted).restriction();
        assert!(matches!(beyond, Restriction::BeyondReach));
        let in_place =
            judged(truth(), witness(true, true, 1, 1), Correlations::InPlace).restriction();
        assert!(matches!(in_place, Restriction::Plain(_)));
    }
}
