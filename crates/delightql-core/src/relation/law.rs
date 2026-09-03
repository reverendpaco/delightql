// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What each exact form does to outputs.
//!
//! Many semantic forms, few output laws. [`law_of`] is the ONE judgment
//! between them and it has no wildcard arm, so adding a row-producing form
//! is a compile error until somebody says what its heading is. That tax is
//! the mechanism: it is what stops a new operator from silently inheriting
//! an unrelated output policy.
//!
//! A caller states "this is a witness" or "this is a drill". It has no
//! spelling for "preserve this owner, mint into that destination, copy
//! these facts, and call the edge transparent".
//!
//! A law carries what the law NEEDS, and no law carries a plan a caller
//! supplied. A set's contribution matrix and a minus's exact-heading map
//! are products of executing the law over the arms named here — which is
//! why `Merge` names arms and an alignment rather than a finished table.

use super::carrier::SemanticRelation;
use super::form::*;
use super::port::PortId;

/// The finite output algebra.
#[derive(Debug)]
pub(super) enum InterfaceLaw<'a> {
    /// A heading built from slots. Nothing is carried; every position is
    /// new.
    New,
    /// The input's interface, exactly. No occurrence is created, which is
    /// what makes the operation transparent rather than merely
    /// name-preserving.
    Preserve { input: &'a SemanticRelation },
    /// The input's whole heading, one-to-one, in order, under a new
    /// occurrence.
    Export {
        input: &'a SemanticRelation,
        why: ExportWhy,
    },
    /// New ordered positions, each with an exact edge to what it came
    /// from. The input's heading is NOT carried unless a slot says so.
    Project {
        input: &'a SemanticRelation,
        slots: &'a [ProjectSlot],
    },
    /// The endpoints' positions, each answering to the endpoint it came
    /// from. Ownership is PRESERVED: an edge consumes nothing, so the
    /// columns still belong to the relations they came from and a composed
    /// path can still ask which endpoint each one is.
    ErBoundary {
        input: &'a SemanticRelation,
        exports: &'a [super::form::ErExport],
    },
    /// The whole heading, one-to-one, with one exact edit applied.
    Edit {
        input: &'a SemanticRelation,
        edit: HeadingEdit<'a>,
    },
    /// The keys, then one position per reduction, in that order.
    Group {
        input: &'a SemanticRelation,
        kind: GroupKind,
        keys: &'a [ProjectSlot],
        reductions: &'a [ReductionSlot],
    },
    /// Both operands' headings, in operand order. A merged key publishes
    /// one position standing for a port of each.
    Concatenate {
        left: &'a SemanticRelation,
        right: &'a SemanticRelation,
        merged: &'a [MergedKey],
    },
    /// One fresh position per result slot, over a total contribution
    /// matrix the authority builds from these arms.
    Merge {
        alignment: SetAlignment,
        arms: &'a [SetArm],
    },
    /// The left heading exported one-to-one, plus an exact anti-match the
    /// authority builds. The right side publishes nothing.
    MinusLeft {
        left: &'a SemanticRelation,
        right: &'a SemanticRelation,
    },
    /// One definition's heading read at one occurrence of it: fresh
    /// relation, fresh ports, per use.
    Instantiate { template: &'a SemanticRelation },
    /// The input context carried forward without the consumed container,
    /// plus the selected positions of its interior relation.
    Explode {
        input: &'a SemanticRelation,
        interior_of: PortId,
        selected: &'a [PortId],
        selection: super::form::DrillSelection,
    },
    /// A payload-only narrowing: the addressed nest belongs to the input,
    /// while the pattern publishes only the positions it computes.
    Narrow {
        input: &'a SemanticRelation,
        nest: PortId,
        bound: &'a [ProjectSlot],
    },
    /// An interior relation owned by one port. Atomic with the back-link:
    /// the owning port records this relation as its interior, and a port
    /// owns exactly one.
    Interior {
        owner: PortId,
        body: &'a SemanticRelation,
    },
    /// A plan-lifetime object standing for a relation's rows, or for none.
    Materialize {
        why: ScratchWhy,
        publishes: &'a super::form::ScratchInterface<'a>,
    },
    /// A heading the form determines by what it is.
    Fixed { shape: FixedShape<'a> },
    /// Identity without enumerable dimensions.
    Opaque,
}

/// One exact edit to a whole heading.
///
/// Closed, and each member says what it leaves alone. That is the fact a
/// consumer needs: a rename keeps every position, a reposition keeps every
/// name, a removal keeps both for what survives, a cover keeps the covered
/// positions' identities, and an extension keeps the entire operand.
#[derive(Debug)]
pub(super) enum HeadingEdit<'a> {
    /// Positions keep their places and take new names.
    Rename(&'a [RenameSlot]),
    /// Positions keep their names and take new places.
    Reposition(&'a [RepositionSlot]),
    /// The addressed positions do not reach the result; every other
    /// position keeps its name and its relative order.
    Remove(&'a [PortId]),
    /// The covered positions keep their identities and take a new value;
    /// an embed cover appends the applied values instead.
    Cover {
        kind: CoverKind,
        cells: &'a [CoverCell],
    },
    /// The operand's whole heading rides in front of the added positions.
    Extend(&'a [ProjectSlot]),
}

/// How many positions a `New`-law heading has.
/// The headings a form determines by what it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FixedShape<'a> {
    /// One row, one column: `met`.
    Witness,
    /// The input's heading with `met` appended last. A NO arm contributes
    /// one all-NULL proxy row with `met = 0`.
    SignedWitness { input: &'a SemanticRelation },
    /// `(scope, column_name, ordinal)` over the interface received. The
    /// subject's exact owner, published name, and ordinal are reported —
    /// never a heading re-derived from anywhere else.
    Meta { subject: &'a SemanticRelation },
}

/// THE ONE JUDGMENT. No wildcard arm — a new [`RelForm`] member does not
/// compile until its output law is stated here.
pub(super) fn law_of<'a>(form: &'a RelForm<'a>) -> InterfaceLaw<'a> {
    match form {
        RelForm::Source(_) => InterfaceLaw::New,
        RelForm::Anonymous(spec) => match spec.shape {
            AnonymousShape::Tabular | AnonymousShape::ArgumentRow => InterfaceLaw::New,
        },
        RelForm::Opaque => InterfaceLaw::Opaque,

        RelForm::Order(input) => InterfaceLaw::Preserve { input },
        RelForm::Export(spec) => InterfaceLaw::Export {
            input: &spec.input,
            why: spec.why,
        },

        // An access publishes what the parens asked for; a projection
        // publishes what the items say. Neither carries the operand's
        // heading unless a slot carries it.
        RelForm::Access(spec) => match spec.shape {
            AccessShape::Whole | AccessShape::Named | AccessShape::Empty => InterfaceLaw::Project {
                input: &spec.input,
                slots: spec.slots,
            },
        },
        RelForm::Project(spec) => InterfaceLaw::Project {
            input: &spec.input,
            slots: spec.slots,
        },
        RelForm::ErBoundary(spec) => InterfaceLaw::ErBoundary {
            input: &spec.input,
            exports: spec.exports,
        },
        // An embed's leading run IS the operand's heading, supplied by the
        // shared projection algorithm rather than authored — so the edit
        // vocabulary states the extension instead of pretending the author
        // wrote a leading glob.
        RelForm::Embed(spec) => InterfaceLaw::Edit {
            input: &spec.input,
            edit: HeadingEdit::Extend(spec.slots),
        },

        RelForm::Rename(spec) => InterfaceLaw::Edit {
            input: &spec.input,
            edit: HeadingEdit::Rename(spec.renames),
        },
        RelForm::Reposition(spec) => InterfaceLaw::Edit {
            input: &spec.input,
            edit: HeadingEdit::Reposition(spec.moves),
        },
        RelForm::ProjectOut(spec) => InterfaceLaw::Edit {
            input: &spec.input,
            edit: HeadingEdit::Remove(spec.removed),
        },
        RelForm::Cover(spec) => InterfaceLaw::Edit {
            input: &spec.input,
            edit: HeadingEdit::Cover {
                kind: spec.kind,
                cells: spec.cells,
            },
        },

        RelForm::Group(spec) => InterfaceLaw::Group {
            input: &spec.input,
            kind: spec.kind,
            keys: spec.keys,
            reductions: spec.reductions,
        },

        RelForm::Join(spec) => match spec.kind {
            JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter | JoinKind::FullOuter => {
                InterfaceLaw::Concatenate {
                    left: &spec.left,
                    right: &spec.right,
                    merged: spec.merged,
                }
            }
        },
        RelForm::Set(spec) => InterfaceLaw::Merge {
            alignment: spec.alignment,
            arms: spec.arms,
        },
        RelForm::Minus(spec) => InterfaceLaw::MinusLeft {
            left: &spec.left,
            right: &spec.right,
        },

        RelForm::Witness(spec) => match spec.polarity {
            WitnessPolarity::Positive | WitnessPolarity::Negative => InterfaceLaw::Fixed {
                shape: FixedShape::Witness,
            },
        },
        RelForm::SignedWitness(spec) => InterfaceLaw::Fixed {
            shape: FixedShape::SignedWitness { input: &spec.input },
        },

        RelForm::Instantiate(spec) => InterfaceLaw::Instantiate {
            template: &spec.template,
        },
        RelForm::PlanRead(spec) => InterfaceLaw::Instantiate {
            template: &spec.template,
        },
        RelForm::Destructure(spec) => match spec.mode {
            DestructureMode::Scalar | DestructureMode::Aggregate => InterfaceLaw::Edit {
                input: &spec.input,
                edit: HeadingEdit::Extend(spec.bound),
            },
        },
        RelForm::Drill(spec) => InterfaceLaw::Explode {
            input: &spec.input,
            interior_of: spec.interior_of,
            selected: spec.selected,
            selection: spec.selection,
        },
        // A narrowing publishes the pattern's positions and nothing else:
        // no context rides through, which is the whole difference from a
        // drill.
        RelForm::Narrow(spec) => InterfaceLaw::Narrow {
            input: &spec.input,
            nest: spec.nest,
            bound: spec.bound,
        },
        RelForm::Interior(spec) => InterfaceLaw::Interior {
            owner: spec.owner,
            body: &spec.body,
        },
        RelForm::Meta(spec) => InterfaceLaw::Fixed {
            shape: FixedShape::Meta {
                subject: &spec.subject,
            },
        },

        RelForm::Scratch(spec) => InterfaceLaw::Materialize {
            why: spec.why,
            publishes: spec.interface(),
        },
    }
}
