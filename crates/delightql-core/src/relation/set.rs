// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The total contribution matrix.
//!
//! A set result's heading is not a claim about names; it is a table. One
//! row per result port, one cell per arm, and every cell says exactly what
//! that arm contributes: a port of its own, or a padding. There is no
//! absent cell, so there is no reader that has to decide what an absence
//! meant — a missing entry is a malformed matrix and refuses at
//! construction, while a genuinely non-contributing arm is a `Padding`.
//!
//! The dimensions are checked once, here, against the arm count and the
//! result width. Every alignment road produces one of these, which is what
//! makes positional, corresponding, smart, and correlated sets one law
//! rather than four that agree by inspection.

use super::port::{PaddingId, PortId};

/// Two or more, structurally.
///
/// A set with one arm is not a set, and a matrix row with one cell cannot
/// be a set output. Saying so in the type removes the runtime check and
/// the arm every consumer would otherwise write for the impossible case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Vec2<T> {
    first: T,
    second: T,
    rest: Vec<T>,
}

impl<T> Vec2<T> {
    #[cfg(test)]
    pub(super) fn of(first: T, second: T, rest: Vec<T>) -> Self {
        Vec2 {
            first,
            second,
            rest,
        }
    }

    /// Two or more from a vector, or `None`.
    pub(super) fn try_from_vec(values: Vec<T>) -> Option<Self> {
        let mut values = values.into_iter();
        let first = values.next()?;
        let second = values.next()?;
        Some(Vec2 {
            first,
            second,
            rest: values.collect(),
        })
    }

    pub fn len(&self) -> usize {
        2 + self.rest.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.first)
            .chain(std::iter::once(&self.second))
            .chain(self.rest.iter())
    }
}

/// Which alignment law produced the matrix.
///
/// Recorded rather than inferred: the cells alone cannot say whether they
/// were aligned by ordinal, by stable published name, or by a proved exact
/// agreement, and the refusal each mode owes differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetMode {
    /// `||` — widths agree; every cell is a `Port`, aligned by ordinal.
    Positional,
    /// `;` — stable published-name order decides the outputs; a cell is
    /// the exact matching port or an explicit padding.
    Corresponding,
    /// `|;|` — exact stable-name agreement proved before construction;
    /// every cell is a `Port`, in the recorded order.
    Smart,
}

/// What one arm contributes to one result port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Contribution {
    /// This arm publishes this port into the result slot.
    Port(PortId),
    /// This arm contributes nothing here, and the typed null that stands
    /// for it has its own identity.
    Padding(PaddingId),
}

/// ONE SET STEP: the operator the author wrote and the relation it
/// produced, as one value.
///
/// The authority mints it in the same act that derives the result, so the
/// two halves have no separate existence to be paired wrongly. A carrier
/// cannot be attached to an operator it was not derived for, a positional
/// result cannot be handed to a corresponding step, and a relation the set
/// road never built cannot stand as a set result at all.
///
/// Private fields and no constructor outside this module: the AST accepts
/// this value, not an operator and a relation side by side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SetStep {
    operator: crate::pipeline::asts::core::SetOperator,
    result: super::carrier::SemanticRelation,
}

impl SetStep {
    pub(super) fn of(
        operator: crate::pipeline::asts::core::SetOperator,
        result: super::carrier::SemanticRelation,
    ) -> Self {
        SetStep { operator, result }
    }

    /// What the author wrote.
    pub fn operator(&self) -> crate::pipeline::asts::core::SetOperator {
        self.operator
    }

    /// What that operator produced over its arms.
    pub fn result(&self) -> super::carrier::SemanticRelation {
        self.result
    }
}

/// One arm of a set, as its construction saw it.
///
/// The ordered interface is RECORDED, not re-read. A branch emits an
/// ordered list, and the place a port holds in this heading is the place
/// that branch emits it at — which is what binds a semantic port to a
/// physical output without searching for it. Asking the arm's scope again
/// at lowering would answer with whatever an unrelated caller has since
/// grown into it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetArmRecord {
    relation: super::port::RelationId,
    ports: Vec<PortId>,
}

impl SetArmRecord {
    pub(super) fn of(relation: super::port::RelationId, ports: Vec<PortId>) -> Self {
        SetArmRecord { relation, ports }
    }

    pub fn relation(&self) -> super::port::RelationId {
        self.relation
    }

    /// The ports this arm published, in the order a branch emits them.
    pub fn ports(&self) -> &[PortId] {
        &self.ports
    }
}

/// One result port and every arm's contribution to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetOutput {
    result: PortId,
    by_arm: Vec2<Contribution>,
}

impl SetOutput {
    pub(super) fn of(result: PortId, by_arm: Vec2<Contribution>) -> Self {
        SetOutput { result, by_arm }
    }

    /// The fresh port this slot publishes. Never one of the arms' ports:
    /// a set result owns its heading.
    pub fn result(&self) -> PortId {
        self.result
    }

    /// One entry per arm, in the matrix's arm order.
    pub fn by_arm(&self) -> &Vec2<Contribution> {
        &self.by_arm
    }
}

/// Why a matrix refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatrixError {
    /// A row's cell count does not match the arm count.
    ArmCountMismatch {
        row: usize,
        cells: usize,
        arms: usize,
    },
    /// A positional or smart set produced a padding, which those modes
    /// cannot: both prove agreement before construction.
    PaddingUnderExactMode { mode: SetMode, row: usize },
    /// Two result rows claim one port.
    DuplicateResultPort { row: usize },
    /// One arm publishes a port twice, so its recorded heading cannot say
    /// which physical output a branch emits for it.
    DuplicateArmPort { arm: usize },
}

/// The whole table.
///
/// Fields are private and the constructor is the authority's, so the
/// dimension check cannot be skipped by assembling one field at a time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContributionMatrix {
    mode: SetMode,
    arms: Vec2<SetArmRecord>,
    outputs: Vec<SetOutput>,
}

impl ContributionMatrix {
    /// Build and check in one act.
    ///
    /// Every row must carry exactly one cell per arm, in arm order. An arm
    /// that contributes nothing to a slot is `Padding` in that cell; an
    /// omitted cell is a malformed matrix, which is the distinction the
    /// whole structure exists to keep.
    pub(super) fn build(
        mode: SetMode,
        arms: Vec2<SetArmRecord>,
        outputs: Vec<SetOutput>,
    ) -> Result<Self, MatrixError> {
        let arm_count = arms.len();
        for (arm, record) in arms.iter().enumerate() {
            let mut ports = record.ports.clone();
            ports.sort_unstable();
            ports.dedup();
            if ports.len() != record.ports.len() {
                return Err(MatrixError::DuplicateArmPort { arm });
            }
        }
        let mut seen: Vec<PortId> = Vec::with_capacity(outputs.len());
        for (row, output) in outputs.iter().enumerate() {
            let cells = output.by_arm.len();
            if cells != arm_count {
                return Err(MatrixError::ArmCountMismatch {
                    row,
                    cells,
                    arms: arm_count,
                });
            }
            if matches!(mode, SetMode::Positional | SetMode::Smart)
                && output
                    .by_arm
                    .iter()
                    .any(|cell| matches!(cell, Contribution::Padding(_)))
            {
                return Err(MatrixError::PaddingUnderExactMode { mode, row });
            }
            if seen.contains(&output.result) {
                return Err(MatrixError::DuplicateResultPort { row });
            }
            seen.push(output.result);
        }
        Ok(ContributionMatrix {
            mode,
            arms,
            outputs,
        })
    }

    /// The arms, in the order every row's cells are in.
    pub fn arms(&self) -> &Vec2<SetArmRecord> {
        &self.arms
    }

    pub fn outputs(&self) -> &[SetOutput] {
        &self.outputs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::Addressing;

    /// Real identities: the authority's newtypes wrap registry occurrences,
    /// so a structural test asks a registry for them rather than forging an
    /// index the production road could never produce.
    struct Fixture {
        registry: crate::relation::Planning,
        scope: crate::names::ScopeId,
        next_relation: std::cell::Cell<u32>,
    }

    impl Fixture {
        fn new() -> Self {
            let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
            let scope = registry.anonymous_scope(None);
            Fixture {
                registry,
                scope,
                next_relation: std::cell::Cell::new(0),
            }
        }

        fn port(&self, _position: u32) -> PortId {
            PortId(self.registry.sql_column(self.scope, None, Addressing::Bare))
        }

        fn relation(&self) -> super::super::port::RelationId {
            let id = self.next_relation.get();
            self.next_relation.set(id + 1);
            super::super::port::RelationId(id)
        }

        fn arm(&self) -> SetArmRecord {
            SetArmRecord::of(self.relation(), vec![])
        }
    }

    fn two_arms(f: &Fixture) -> Vec2<SetArmRecord> {
        Vec2::of(f.arm(), f.arm(), Vec::new())
    }

    /// An arm that publishes one port twice cannot say which physical
    /// output a branch emits for it.
    #[test]
    fn an_arm_cannot_publish_one_port_twice() {
        let f = Fixture::new();
        let repeated = f.port(9);
        let arms = Vec2::of(
            SetArmRecord::of(f.relation(), vec![repeated, repeated]),
            f.arm(),
            Vec::new(),
        );
        assert_eq!(
            ContributionMatrix::build(SetMode::Positional, arms, vec![]),
            Err(MatrixError::DuplicateArmPort { arm: 0 })
        );
    }

    #[test]
    fn every_row_carries_one_cell_per_arm() {
        let f = Fixture::new();
        let short = SetOutput::of(
            f.port(0),
            Vec2::of(
                Contribution::Port(f.port(1)),
                Contribution::Port(f.port(2)),
                vec![],
            ),
        );
        let three = Vec2::of(f.arm(), f.arm(), vec![f.arm()]);
        assert_eq!(
            ContributionMatrix::build(SetMode::Positional, three, vec![short]),
            Err(MatrixError::ArmCountMismatch {
                row: 0,
                cells: 2,
                arms: 3
            })
        );
    }

    #[test]
    fn an_exact_mode_cannot_pad() {
        let f = Fixture::new();
        let padded = SetOutput::of(
            f.port(0),
            Vec2::of(
                Contribution::Port(f.port(1)),
                Contribution::Padding(PaddingId(0)),
                vec![],
            ),
        );
        assert_eq!(
            ContributionMatrix::build(SetMode::Smart, two_arms(&f), vec![padded.clone()]),
            Err(MatrixError::PaddingUnderExactMode {
                mode: SetMode::Smart,
                row: 0
            })
        );
        assert!(
            ContributionMatrix::build(SetMode::Corresponding, two_arms(&f), vec![padded]).is_ok()
        );
    }

    #[test]
    fn two_rows_cannot_claim_one_result_port() {
        let f = Fixture::new();
        let result = f.port(0);
        let row = || {
            SetOutput::of(
                result,
                Vec2::of(
                    Contribution::Port(f.port(1)),
                    Contribution::Port(f.port(2)),
                    vec![],
                ),
            )
        };
        assert_eq!(
            ContributionMatrix::build(SetMode::Positional, two_arms(&f), vec![row(), row()]),
            Err(MatrixError::DuplicateResultPort { row: 1 })
        );
    }

    #[test]
    fn a_set_has_two_or_more_arms() {
        assert!(Vec2::try_from_vec(vec![1]).is_none());
        assert_eq!(Vec2::try_from_vec(vec![1, 2, 3]).map(|v| v.len()), Some(3));
    }
}

/// THE OCCURRENCE EFFECT OF A SET SLOT IS DECIDED FROM EVERY ARM BEFORE
/// THE SLOT IS BORN — and never revised. A row oracle is identical either
/// way, so these witnesses ask the recorded relationship directly.
#[cfg(test)]
mod occurrence_tests {
    use crate::relation::form::{
        AnonymousShape, AnonymousSlot, AnonymousSpec, ExportSpec, ExportWhy, SetAlignment, SetArm,
        SetSpec,
    };
    use crate::relation::{published_ports, Planning, PortId, RelForm, SemanticRelation};

    /// One anonymous relation publishing the named positions — each an
    /// origin of its own.
    fn rows(planning: &Planning, names: &[&str]) -> SemanticRelation {
        let slots: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(position, name)| AnonymousSlot::Binder {
                position: position as u32,
                named: planning.intern(name, false),
                declared_type: None,
                shape: crate::names::ValueShape::Unknown,
            })
            .collect();
        planning
            .authority()
            .derive(RelForm::Anonymous(AnonymousSpec {
                shape: AnonymousShape::Tabular,
                slots: &slots,
                answers_to: None,
            }))
            .expect("an anonymous relation derives")
    }

    /// A stage over `input`: every position continues the input's.
    fn stage(planning: &Planning, input: SemanticRelation) -> SemanticRelation {
        planning
            .authority()
            .derive(RelForm::Export(ExportSpec {
                input,
                why: ExportWhy::Stage,
            }))
            .expect("a stage derives")
    }

    fn set(
        planning: &Planning,
        alignment: SetAlignment,
        arms: &[SemanticRelation],
    ) -> SemanticRelation {
        let arms: Vec<_> = arms
            .iter()
            .map(|relation| SetArm {
                relation: *relation,
                correlated: false,
            })
            .collect();
        planning
            .authority()
            .derive(RelForm::Set(SetSpec {
                alignment,
                arms: &arms,
            }))
            .expect("a set derives")
    }

    fn ports(planning: &Planning, relation: &SemanticRelation) -> Vec<PortId> {
        published_ports(planning, relation).expect("a derived interface")
    }

    /// Two arms whose positions are DIFFERENT origins: the slot they fill
    /// is an occurrence of its own — it continues neither, and no
    /// first-arm edge survives underneath.
    #[test]
    fn a_set_slot_with_disagreeing_origins_is_its_own_occurrence() {
        let planning = Planning::open(crate::names::Registry::new(&[]));
        let a = rows(&planning, &["x"]);
        let b = rows(&planning, &["x"]);
        let result = set(&planning, SetAlignment::Positional, &[a, b]);
        let [slot] = ports(&planning, &result)[..] else {
            panic!("one slot")
        };
        let a_x = ports(&planning, &a)[0];
        let b_x = ports(&planning, &b)[0];
        assert!(
            !planning.continues_occurrence(slot, a_x),
            "a disagreeing slot does not continue the first arm"
        );
        assert!(
            !planning.continues_occurrence(slot, b_x),
            "a disagreeing slot does not continue the second arm"
        );
    }

    /// Every arm continues ONE origin — two stages over the same relation
    /// — so the slot continues it too.
    #[test]
    fn a_set_slot_every_arm_of_which_continues_one_origin_continues_it() {
        let planning = Planning::open(crate::names::Registry::new(&[]));
        let base = rows(&planning, &["x"]);
        let left = stage(&planning, base);
        let right = stage(&planning, base);
        let result = set(&planning, SetAlignment::Positional, &[left, right]);
        let [slot] = ports(&planning, &result)[..] else {
            panic!("one slot")
        };
        let origin = ports(&planning, &base)[0];
        assert!(
            planning.continues_occurrence(slot, origin),
            "an all-arm continuation continues the shared origin"
        );
    }

    /// A corresponding slot one arm pads: the position the first arm
    /// opened is an occurrence of its own — a padded cell is not the
    /// origin's row.
    #[test]
    fn a_corresponding_slot_with_padding_is_its_own_occurrence() {
        let planning = Planning::open(crate::names::Registry::new(&[]));
        let base = rows(&planning, &["x", "y"]);
        let wide = stage(&planning, base);
        let narrow = stage(&planning, rows(&planning, &["x"]));
        let result = set(&planning, SetAlignment::Corresponding, &[wide, narrow]);
        let slots = ports(&planning, &result);
        assert_eq!(slots.len(), 2, "x and y");
        let base_y = ports(&planning, &base)[1];
        assert!(
            !planning.continues_occurrence(slots[1], base_y),
            "the padded slot does not continue the one arm that filled it"
        );
        let base_x = ports(&planning, &base)[0];
        assert!(
            !planning.continues_occurrence(slots[0], base_x),
            "a slot whose arms are different origins continues neither"
        );
    }
}
