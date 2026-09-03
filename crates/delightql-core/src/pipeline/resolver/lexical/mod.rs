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

mod join;
mod lookup;
mod pattern;
mod standing;

pub(in crate::pipeline::resolver) use join::shared_using_names;
pub(crate) use pattern::StrictPhaseConverter;
pub(crate) use standing::{
    AnonRouting, PatternOperand, PatternOwner, ResolvedQuery, ResolvedRelation,
};

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
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Witness {
    /// A name bound inside the interior relation.
    pub anchored: bool,
    /// A name found nothing there and was answered by the enclosing row.
    pub escaped: bool,
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
}

impl<'e> Position<'e> {
    /// A position with nothing behind it — the prompt, or a closed world
    /// such as a definition body or a relation actual.
    pub(crate) fn root() -> Self {
        Position {
            frames: Vec::new(),
            enclosing: None,
            flat: false,
        }
    }

    /// A position INSIDE another: an interior expression sees the row it
    /// is correlated to through this borrow and through nothing else.
    pub(crate) fn enclosed_by(outer: &'e Position<'e>) -> Self {
        Position {
            frames: Vec::new(),
            enclosing: Some(outer),
            flat: false,
        }
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
    /// enclosing them — for the USING correlation that pairs an interior's
    /// columns with the row it looks left into by name. Publication, not
    /// permission: these are the positions the relations publish to anyone.
    pub(crate) fn ports_in_view(
        &self,
        registry: &crate::relation::Planning,
    ) -> Result<Vec<PortId>> {
        self.all_ports(registry)
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
    /// it is decided over is assembled here and reaches no caller.
    pub(crate) fn address(
        &self,
        reference: ColumnReference,
        in_correlation: bool,
        witness: &mut Witness,
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
        let (qualifier, is_named) = match &reference {
            ColumnReference::Named { qualifier, .. } => (qualifier.as_deref(), true),
            ColumnReference::Ordinal { qualifier, .. } => (qualifier.as_deref(), false),
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
                if !is_named {
                    // An ordinal counts positions within the scope its
                    // qualifier chooses, over everything in view.
                    return Ok(lookup::unify_single_column(
                        reference,
                        &self.all_ports(registry)?,
                        &self.all_visible(registry),
                        registry,
                    ));
                }
                // An interior relation is the lexical scope under the
                // reader's finger. Search it before the enclosing context,
                // including for a qualified reference: a second `addresses`
                // interior shadows an earlier sibling named `addresses`. A
                // different qualifier widens only after the local heading
                // proves it absent.
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
                let result = if narrowed {
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
                            if in_correlation {
                                witness.escaped = true;
                            }
                            lookup::unify_single_column(
                                reference,
                                &self.all_ports(registry)?,
                                &self.all_visible(registry),
                                registry,
                            )
                        }
                        settled => settled,
                    }
                } else {
                    lookup::unify_single_column(
                        reference,
                        &self.all_ports(registry)?,
                        &self.all_visible(registry),
                        registry,
                    )
                };
                // Anchoring is decided by which relation the reference
                // LANDED on, not by how it was spelled. Escaping is not its
                // complement: only the widening above is an escape.
                if in_correlation {
                    if let UnificationResult::Resolved(occurrence) = &result {
                        if local_ports.contains(&occurrence.column) {
                            witness.anchored = true;
                        }
                    }
                }
                Ok(result)
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
                    return Err(DelightQLError::column_not_found_error(name, error_context));
                }
                UnificationResult::Opaque => {
                    return Err(crate::pipeline::resolver::opaque_reference_refusal());
                }
                UnificationResult::Refused(refusal) => return Err(refusal.into_error()),
                UnificationResult::Ambiguous { column, tables } => {
                    return Err(DelightQLError::ValidationError {
                        message: format!(
                            "Column '{}' {} is ambiguous. Could refer to: {}",
                            column,
                            error_context,
                            tables.join(", ")
                        ),
                        context: error_context.to_string(),
                        subcategory: None,
                    });
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
        Ok(reached
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
            [] => Err(DelightQLError::validation_error_categorized(
                "resolution/setop/correlation_owner",
                format!(
                    "set-operation correlation qualifier '{}' does not name a visible operand",
                    qualifier
                ),
                "qualify each whole-heading reference by an operand name or alias",
            )),
            _ => Err(DelightQLError::validation_error_categorized(
                "resolution/setop/correlation_owner",
                format!(
                    "set-operation correlation qualifier '{}' names more than one visible operand",
                    qualifier
                ),
                "use distinct operand aliases",
            )),
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
