// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A SEMANTIC OPERATION STATED ONCE, BEFORE ITS PORTS EXIST.
//!
//! A caller reaches construction holding what it resolved and nothing the
//! authority decides: the operand, the positions the author wrote, the
//! expressions standing at them. It does not hold an interface, because the
//! operation has not been derived yet, and it does not hold a law, because
//! the law is the operation's rather than a second thing to choose.
//!
//! One value carries all of it. From that value the authority derives the
//! semantic slots, selects the exact form, mints the ports, and writes the
//! stored payload OVER those ports — so the description that decides what
//! the relation publishes is the same description the tree stores. There is
//! no arrangement of a call site in which a filter's law meets a
//! projection's payload, because there is only one description and it
//! reaches the authority whole.
//!
//! The vocabulary is RESOLUTION's: every pending operation is stated in the
//! resolved phase, because that is the phase in which operations are born.
//! Refinement rebuilds nodes; it states those rebuilds here too, in the
//! same resolved vocabulary the nodes carry.

use crate::pipeline::asts::core::{DomainExpression, Resolved};

/// ONE POSITION A PUBLICATION STATES.
///
/// The author's own positions and the engine's expansion of a glob are
/// different acts. An expansion restates the operand's heading, and under
/// an edit that heading is the edit's own leading run rather than a slot
/// the publication mints. Which act it is decides whether the position
/// contributes a slot, so it is STATED here — recovering it from the
/// expression's shape would make a carried reference the author wrote
/// indistinguishable from one the engine wrote for it.
pub(crate) enum Position {
    /// A position the author wrote: the value it publishes and the name it
    /// asks to answer to.
    Authored {
        expr: DomainExpression<Resolved>,
        naming: Option<delightql_types::SqlIdentifier>,
    },
    /// A position the ENGINE wrote, expanding a glob the author wrote.
    Expanded {
        expr: DomainExpression<Resolved>,
        naming: Option<delightql_types::SqlIdentifier>,
    },
    /// A glob standing for a whole operand it does not expand. It publishes
    /// nothing of its own: what it stands for is published by the positions
    /// beside it.
    Whole,
}

impl Position {
    /// The value standing at this position, where one stands there.
    pub(crate) fn value(&self) -> Option<&DomainExpression<Resolved>> {
        match self {
            Self::Authored { expr, .. } | Self::Expanded { expr, .. } => Some(expr),
            Self::Whole => None,
        }
    }

    /// The name this position asks its output to answer to.
    pub(super) fn naming(&self) -> Option<&delightql_types::SqlIdentifier> {
        match self {
            Self::Authored { naming, .. } | Self::Expanded { naming, .. } => naming.as_ref(),
            Self::Whole => None,
        }
    }

    /// Whether the engine wrote this position expanding a glob.
    pub(crate) fn is_engine_expansion(&self) -> bool {
        matches!(self, Self::Expanded { .. })
    }
}

/// WHAT A PUBLICATION DOES TO THE HEADING STANDING TO ITS LEFT.
///
/// Two acts, and the variant fixes BOTH the output law and the operator the
/// tree stores. A caller states which act; there is no second spelling for
/// it to disagree with.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Publishes {
    /// `|> (a, b)` — the stated positions are the whole heading.
    Anew,
    /// `|> $$(a)` — the operand's heading, edited at the stated positions.
    Edited,
}

/// ONE REDUCTION EVENT of a grouping, stated before its port exists.
///
/// The variant fixes what the event publishes: a value publishes the slot
/// its expression asks for, a metadata group publishes one interior
/// position, a pivot publishes one position per value its membership
/// predicate named and none of its own.
pub(crate) enum Reduction {
    /// One reduced value, or a record interior standing as a group.
    Out(Position),
    /// `~> { … }` — the class's rows as an interior relation.
    Metadata {
        group: crate::pipeline::asts::resolved::MetadataGroup,
        naming: Option<delightql_types::SqlIdentifier>,
    },
    /// One column per value the pivot's membership predicate named, with
    /// its values already expanded by the resolution that read them.
    Pivot(crate::pipeline::asts::core::PivotSpec<Resolved>),
}

/// One delegate of a reduction: a representative row's payload, in payload
/// order, with the ordering that selects the representative.
pub(crate) struct Delegate {
    pub(crate) payload: Vec<Position>,
    pub(crate) order: Vec<crate::pipeline::asts::resolved::OrderingSpec>,
}

/// Which act a grouping is.
pub(crate) enum GroupShape {
    /// `%(keys)` — each class publishes its keys, once.
    Distinct,
    /// `%(keys ~> reductions)` — keys, then the reduction events, then
    /// every delegate's payload, in that published order.
    Reduce {
        reductions: Vec<Reduction>,
        delegates: Vec<Delegate>,
    },
}

/// One authored move of a reposition: the occurrence the author addressed
/// and the position it asked for, counted as authored (1-based, negative
/// from the end).
pub(crate) struct Move {
    pub(crate) reference: crate::pipeline::asts::core::Reference<Resolved>,
    pub(crate) position: i32,
}

/// One item of a transform: the expression standing at a covered position,
/// the name it answers to, and the operand position it covers.
pub(crate) struct TransformItem {
    pub(crate) expr: DomainExpression<Resolved>,
    pub(crate) naming: delightql_types::SqlIdentifier,
    pub(crate) qualifier: Option<delightql_types::SqlIdentifier>,
    /// The position of the operand this item writes, as resolution
    /// answered it.
    pub(crate) covered: super::PortId,
}

/// A ROW-PRODUCING OPERATION, STATED BEFORE ITS PORTS EXIST.
///
/// What an authority act is given. Each variant carries what the caller
/// resolved and nothing else — no interface, no law, and no relation the
/// authority would otherwise derive. One exhaustive
/// [`super::SemanticBuilder::bind`] match derives the exact form, the
/// output law, the ports, and the stored payload from each.
pub(crate) enum Pending {
    /// A publication over one operand.
    Publication {
        input: super::SemanticRelation,
        publishes: Publishes,
        /// Which operation this publication is: the authored `|>` stage,
        /// or a compiler republication (a narrowing to the binders, a
        /// receipt restatement). The operation decides the output
        /// boundary; the caller has no spelling for addressability.
        why: super::form::ProjectWhy,
        positions: Vec<Position>,
    },
    /// A caller pattern's read: the dimensions the written parens ask of the
    /// relation to their left, answering to the owner the call site wrote.
    ///
    /// This is the pre-node case. The pattern must be resolved before the
    /// read exists, so there is no node to hand over — but nothing here is
    /// an interface either. The row says what each written slot DOES, and
    /// the two acts the authority performs (asking the dimensions, then
    /// exporting them under the written owner) are read off it. The row is
    /// a [`SlotRow`]: judged whole by the lexical authority, never
    /// assembled here from an operand and somebody else's positions.
    CallerPattern(SlotRow),
    /// `(*)`, `.(a, b)`, `.*` — the dimensions a read asks of its operand.
    /// Heading-preserving in content: it publishes exactly the operand's
    /// positions, as a fresh occurrence. The authored access IS the stored
    /// payload, and the shape of the ask is read off it.
    Access {
        input: super::SemanticRelation,
        access: crate::pipeline::asts::resolved::Access,
    },
    /// `|> -( … )` — the heading minus the addressed positions.
    ///
    /// The selector and the removals are ONE resolution act's answer: the
    /// stored selector may keep an unexpanded spread (a docket hold), so
    /// the expansion's result rides beside it rather than being recovered
    /// from it.
    ProjectOut {
        input: super::SemanticRelation,
        selector: Vec<crate::pipeline::asts::resolved::SelectorItem>,
        removed: Vec<super::PortId>,
    },
    /// `|> *( a as b )` — the positions are untouched; the names change.
    Rename {
        input: super::SemanticRelation,
        renames: Vec<super::form::RenameSlot>,
    },
    /// `*[c as n]` — the names are untouched; the positions change. The
    /// layout — which position every column ends at — is the authority's
    /// arithmetic, not a second list the caller writes.
    Reposition {
        input: super::SemanticRelation,
        moves: Vec<Move>,
    },
    /// `+` / `\+` — existence reified as the one-row, one-column result.
    /// One authored polarity decides the law and the stored form both.
    Witness {
        input: super::SemanticRelation,
        polarity: crate::pipeline::asts::core::Polarity,
    },
    /// `+-` — the input's heading with `met` appended last.
    SignedWitness { input: super::SemanticRelation },
    /// `^` — the relation's schema as data, with the fixed heading.
    Meta { input: super::SemanticRelation },
    /// `.col(…)` — explode an interior relation column into rows. The
    /// bound drill states the interior column, the selected positions and
    /// the groundings; the semantic form is read off it.
    Drill {
        input: super::SemanticRelation,
        drill: crate::pipeline::asts::core::operators::BoundDrill,
    },
    /// `|> .nest{…}` — iterate the array a nest carries. The pattern is
    /// the one description: the keys it reads and the names it publishes
    /// are extracted from it in the act that mints their ports.
    Narrow {
        input: super::SemanticRelation,
        nest: crate::pipeline::asts::core::ColumnOccurrence,
        pattern: crate::pipeline::asts::unresolved::RecordPattern,
    },
    /// `col ~= {…}` — read fields out of a document, or iterate and
    /// explode rows. The pattern is the one description, exactly as for
    /// the narrowing.
    Destructure {
        input: super::SemanticRelation,
        source: crate::pipeline::asts::resolved::DomainExpression,
        mode: crate::pipeline::asts::core::DestructureMode,
        pattern: crate::pipeline::asts::core::TreePattern<crate::pipeline::asts::core::Unresolved>,
    },
    /// `$(f)` — a callable applied over each covered cell, writing the
    /// cell in place. The applied cells are the one description: each
    /// names the position it covers and the closed expression standing
    /// there.
    MapCover {
        input: super::SemanticRelation,
        selector: Vec<crate::pipeline::asts::resolved::SelectorItem>,
        guard: Option<Box<crate::pipeline::asts::resolved::TruthExpression>>,
        cells: Vec<crate::pipeline::asts::core::operators::AppliedCell<Resolved>>,
    },
    /// `+$(f)` — a callable applied over each covered cell, appended
    /// beside the operand's heading under the authored naming template.
    EmbedMapCover {
        input: super::SemanticRelation,
        naming: Option<crate::pipeline::asts::core::operators::ColumnAlias>,
        selector: Vec<crate::pipeline::asts::resolved::SelectorItem>,
        cells: Vec<crate::pipeline::asts::core::operators::AppliedCell<Resolved>>,
    },
    /// `$$( … )` — authored expressions written over covered cells.
    Transform {
        input: super::SemanticRelation,
        items: Vec<TransformItem>,
        guard: Option<Box<crate::pipeline::asts::resolved::TruthExpression>>,
    },
    /// `%(keys)` / `%(keys ~> reductions)`.
    Group {
        input: super::SemanticRelation,
        keys: Vec<Position>,
        shape: GroupShape,
    },
    /// An ordering, with the bound that consumes it if one stands beside
    /// it. It re-orders rows — and, bounded, chooses which of them the
    /// relation keeps: no occurrence is created and what the step
    /// publishes IS the relation standing to its left, row-bounded when
    /// the bound is present.
    Ordering {
        input: super::SemanticRelation,
        specs: Vec<crate::pipeline::asts::resolved::OrderingSpec>,
        bound: Option<crate::pipeline::asts::core::TupleOrdinalClause>,
    },
    /// A whole-heading access of a pattern that selects no slot: `(*)`,
    /// `()`, `.*`, `.(a, b)` over what the pattern was handed. Publishes
    /// its operand; the authored access is the stored payload.
    Requalify {
        input: super::SemanticRelation,
        access: crate::pipeline::asts::resolved::Access,
    },
    /// A projection REBUILT to carry correlation columns a hoisted
    /// predicate still reads. The rebuild publishes the projection it
    /// replaces, whole, with the carriers as dependencies; the stored
    /// items are the projection's own, relanded through the carry edges
    /// this act writes, and the replacement is recorded in the same act.
    CarrierInjection {
        /// The projection this rebuild replaces — its operand and its
        /// obligation at once.
        replaces: super::SemanticRelation,
        carriers: Vec<super::PortId>,
        items: Vec<crate::pipeline::asts::resolved::OutItem>,
        /// Which operator the tree stored for the original: an embed keeps
        /// the operand whole and a projection states a heading, and the
        /// rebuild stores the one that was there.
        stored: Publishes,
    },
    /// A hygienic support position that crosses as part of a closed value.
    /// Unlike a predicate dependency, a later join must be able to spend
    /// this position, so it belongs to the semantic interface until the
    /// residual lifecycle removes it.
    CrossingCarrierInjection {
        replaces: super::SemanticRelation,
        carriers: Vec<super::PortId>,
        items: Vec<crate::pipeline::asts::resolved::OutItem>,
        stored: Publishes,
    },
    /// A compiler-written ranking witness appended beside the operand's
    /// whole heading: `row_number()` over the stated partition and
    /// ordering, standing at the one hygienic position this act mints.
    WindowWitness {
        input: super::SemanticRelation,
        partition: Vec<crate::pipeline::asts::resolved::DomainExpression>,
        ordering: Vec<crate::pipeline::asts::resolved::OrderingSpec>,
    },
}

/// A SLOT ROW, JUDGED. The operand the row reads, the owner it answers
/// to, what each written slot does, and the support positions the read
/// carries — one value, minted only by the lexical authority's whole-
/// relation argumentative operation. Its fields are private and its one
/// constructor spends a [`crate::pipeline::resolver::Terminal`] proof, so
/// a caller cannot pair a valid operand with positions judged over another
/// row, a reuse edge nobody decided, or an owner nobody wrote.
pub(crate) struct SlotRow {
    input: super::SemanticRelation,
    /// The owner the row answers to: an authored name, or NONE for a row
    /// nobody named.
    answers_to: Option<crate::names::Spelling>,
    positions: Vec<PatternPosition>,
    /// Hygienic support positions of the operand the read carries beside
    /// the pattern — correlation carriers, injected discriminators. Not
    /// part of the declared heading the pattern addresses; they ride as
    /// dependencies of the read.
    carriers: Vec<super::PortId>,
}

impl SlotRow {
    /// The row as the lexical authority judged it.
    pub(crate) fn judged(
        input: super::SemanticRelation,
        answers_to: Option<crate::names::Spelling>,
        positions: Vec<PatternPosition>,
        carriers: Vec<super::PortId>,
        _judged: crate::pipeline::resolver::Terminal,
    ) -> Self {
        SlotRow {
            input,
            answers_to,
            positions,
            carriers,
        }
    }

    /// The row's parts, for the one bind act that performs it.
    pub(super) fn into_parts(
        self,
    ) -> (
        super::SemanticRelation,
        Option<crate::names::Spelling>,
        Vec<PatternPosition>,
        Vec<super::PortId>,
    ) {
        (self.input, self.answers_to, self.positions, self.carriers)
    }
}

/// WHAT ONE WRITTEN SLOT OF A CALLER PATTERN DOES.
///
/// A slot that publishes has no stored occurrence here, because the
/// occurrence IS the port this act mints for it: there is no arrangement in
/// which the pattern binds one position and the tree stores another. A slot
/// that publishes nothing carries what it stores, because no port of this
/// operation stands at it.
pub(crate) enum PatternPosition {
    /// `(k)` / `(t.k)` — the slot BINDS. It publishes under the name
    /// written at it, and the tree stores the port this act mints.
    Binds {
        source: super::PortId,
        naming: super::form::Naming,
        /// The author wrote somebody else's column here, so the stored
        /// occurrence says the qualifier was explicit.
        qualified: bool,
        /// The exactly-one live BARE port this spelling reuses, decided by
        /// resolution while the complete live bare interface was in hand.
        /// The bind act records the edge between that port and the port it
        /// mints; the join that owns the left port consumes the record. No
        /// later phase re-derives it from characters or ancestry.
        reuses: Option<super::PortId>,
    },
    /// A slot that publishes but names nothing: a literal or a computed
    /// slot constrains the source column and offers no name for the
    /// output, so what it stores is about the SOURCE and not about the
    /// position this act mints.
    Publishes {
        source: super::PortId,
        stored: crate::pipeline::asts::core::Slot<Resolved>,
    },
    /// A slot that CONSTRAINS: it publishes nothing, and the source column
    /// it constrains is a dependency of the read.
    Constrains {
        source: super::PortId,
        stored: crate::pipeline::asts::core::Slot<Resolved>,
    },
    /// `_` — a slot the pattern disregards. Neither published nor depended
    /// on.
    Skips {
        stored: crate::pipeline::asts::core::Slot<Resolved>,
    },
}
