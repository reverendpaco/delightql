// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The set law's alignment, and nothing broader.
//!
//! "Arms align by name" — one question decides a correspondence here: do
//! the two positions answer to the same stable published name.
//!
//! A MINT ALIGNS WITH NOTHING for the same reason, and it needs no special
//! case: a minted or poisoned name is a real name, unique by construction,
//! so it matches no other arm's. A position answering to no name at all —
//! latent, hygienic, a caller's own bare binding — aligns with nothing
//! because there is no name to compare, never because a weaker tier
//! declined to fire.

use crate::names::{ColId, CorrespondenceError, Registry, Sym};

/// Which candidate, if any, answers each output slot.
///
/// One entry per output, in output order. `None` is "no arm column answers
/// to this slot's name", which a corresponding set pads and a smart set
/// refuses. Every candidate is consumed at most once.
///
/// Repeated names are ranked: when a name occurs more than once on either
/// side, the kth occurrence answers the kth. Both sides refuse outright
/// when one scope binds one name twice argumentatively, because there the
/// author wrote an ambiguity no ranking can resolve.
pub(super) fn stable_name_alignment(
    registry: &Registry,
    outputs: &[ColId],
    candidates: &[ColId],
) -> Result<Vec<Option<ColId>>, CorrespondenceError> {
    registry.refuse_duplicate_bound_names(outputs)?;
    registry.refuse_duplicate_bound_names(candidates)?;

    let named = |column: &ColId| registry.published_sym(*column);
    let rank_of = |columns: &[ColId], index: usize, name: Sym| {
        columns[..index]
            .iter()
            .filter(|column| named(column) == Some(name))
            .count()
    };
    let occurrences = |columns: &[ColId], name: Sym| {
        columns
            .iter()
            .filter(|column| named(column) == Some(name))
            .count()
    };

    let mut matched = vec![None; outputs.len()];
    let mut consumed = vec![false; candidates.len()];
    for (output_index, output) in outputs.iter().enumerate() {
        // NO NAME, NO ALIGNMENT. The position holds its place and answers
        // to nothing, so nothing in another arm continues it.
        let Some(name) = named(output) else {
            continue;
        };
        let repeated = occurrences(outputs, name) > 1 || occurrences(candidates, name) > 1;
        let wanted_rank = rank_of(outputs, output_index, name);
        for (candidate_index, candidate) in candidates.iter().enumerate() {
            if consumed[candidate_index] || named(candidate) != Some(name) {
                continue;
            }
            if repeated && rank_of(candidates, candidate_index, name) != wanted_rank {
                continue;
            }
            matched[output_index] = Some(*candidate);
            consumed[candidate_index] = true;
            break;
        }
    }
    Ok(matched)
}

#[cfg(test)]
mod tests {
    use super::super::form::{
        AnonymousShape, AnonymousSlot, AnonymousSpec, SetAlignment, SetArm, SetSpec,
    };
    use super::super::set::Contribution;
    use super::super::RelForm;
    use super::stable_name_alignment;
    use crate::names::{Addressing, ColId};

    struct Fixture {
        registry: crate::relation::Planning,
    }

    impl Fixture {
        fn new() -> Self {
            Fixture {
                registry: crate::relation::Planning::open(crate::names::Registry::new(&[])),
            }
        }

        fn scope(&self) -> crate::names::ScopeId {
            self.registry.anonymous_scope(None)
        }

        fn column(
            &self,
            scope: crate::names::ScopeId,
            name: Option<&str>,
            addressing: Addressing,
        ) -> ColId {
            self.registry.sql_column(
                scope,
                name.map(|name| self.registry.intern(name, false)),
                addressing,
            )
        }

        /// The same occurrence carried across a boundary and baptised anew:
        /// one republication chain, two names.
        fn renamed(&self, source: ColId, into: crate::names::ScopeId, name: &str) -> ColId {
            let spelling = self.registry.intern(name, false);
            self.registry
                .rebind_sql_column(source, into, Some(spelling))
        }

        /// A second occurrence of one value, answering to no name.
        fn latent(&self, source: ColId, into: crate::names::ScopeId) -> ColId {
            self.registry.rebind_sql_column(source, into, None)
        }
    }

    /// TWO DIFFERENTLY NAMED POSITIONS ON ONE CHAIN DO NOT ALIGN.
    #[test]
    fn a_republication_chain_does_not_align_two_names() {
        let f = Fixture::new();
        let left_scope = f.scope();
        let right_scope = f.scope();
        let a = f.column(left_scope, Some("a"), Addressing::Published);
        let carried = f.renamed(a, right_scope, "renamed");

        assert_eq!(
            stable_name_alignment(&f.registry, &[a], &[carried]).expect("the set law answers"),
            vec![None],
            "`a` and `renamed` are two names, so the set law aligns nothing"
        );
    }

    /// TWO UNNAMED POSITIONS SHARING A VALUE DO NOT ALIGN.
    #[test]
    fn a_shared_value_does_not_align_two_unnamed_positions() {
        let f = Fixture::new();
        let source_scope = f.scope();
        let left_scope = f.scope();
        let right_scope = f.scope();
        let source = f.column(source_scope, Some("a"), Addressing::Published);
        let left = f.latent(source, left_scope);
        let right = f.latent(source, right_scope);

        assert_eq!(
            stable_name_alignment(&f.registry, &[left], &[right]).expect("the set law answers"),
            vec![None],
            "two positions answering to no name are two slots, however their \
             values are related"
        );
    }

    /// ONE UNNAMED OCCURRENCE DOES NOT ALIGN BY NAME, EVEN WITH ITSELF.
    #[test]
    fn one_unnamed_occurrence_in_both_arms_does_not_align() {
        let f = Fixture::new();
        let scope = f.scope();
        let shared = f.column(scope, None, Addressing::Latent);

        assert_eq!(
            stable_name_alignment(&f.registry, &[shared], &[shared]).expect("the set law answers"),
            vec![None],
            "an unnamed position answers no slot, its own included"
        );
    }

    /// The law it DOES apply: one name, one slot, and repeats ranked.
    #[test]
    fn one_name_aligns_and_repeats_rank() {
        let f = Fixture::new();
        let left_scope = f.scope();
        let right_scope = f.scope();
        let left_first = f.column(left_scope, Some("a"), Addressing::Published);
        let left_second = f.column(left_scope, Some("a"), Addressing::Published);
        let right_first = f.column(right_scope, Some("a"), Addressing::Published);
        let right_second = f.column(right_scope, Some("a"), Addressing::Published);

        assert_eq!(
            stable_name_alignment(
                &f.registry,
                &[left_first, left_second],
                &[right_first, right_second]
            )
            .expect("the set law answers"),
            vec![Some(right_first), Some(right_second)],
            "the kth occurrence of a name answers the kth"
        );
    }

    /// SMART REFUSES WHERE CORRESPONDING PADS. Same two arms, different
    /// operator, and the operator is the whole answer.
    #[test]
    fn smart_refuses_the_names_corresponding_pads() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let arm = |name: &str| {
            let slots = [AnonymousSlot::Declared {
                position: 0,
                named: Some(registry.intern(name, false)),
            }];
            registry
                .authority()
                .derive(RelForm::Anonymous(AnonymousSpec {
                    shape: AnonymousShape::Tabular,
                    slots: &slots,
                    answers_to: None,
                }))
                .expect("an anonymous relation is built")
        };
        let left = arm("a");
        let right = arm("b");
        let arms = [
            SetArm {
                relation: left,
                correlated: false,
            },
            SetArm {
                relation: right,
                correlated: false,
            },
        ];
        assert!(
            registry
                .authority()
                .derive(RelForm::Set(SetSpec {
                    alignment: SetAlignment::Smart,
                    arms: &arms,
                }))
                .is_err(),
            "smart proves exact name agreement before construction"
        );
        let padded = registry
            .authority()
            .derive(RelForm::Set(SetSpec {
                alignment: SetAlignment::Corresponding,
                arms: &arms,
            }))
            .expect("corresponding pads instead");
        let matrix = registry
            .relations()
            .contributions(padded.relation())
            .expect("a set result records its table");
        assert_eq!(matrix.outputs().len(), 2, "`a` and `b` are two slots");
        assert_eq!(
            matrix
                .outputs()
                .iter()
                .flat_map(|output| output.by_arm().iter())
                .filter(|cell| matches!(cell, Contribution::Padding(_)))
                .count(),
            2,
            "each arm pads the other's slot"
        );
    }
}
