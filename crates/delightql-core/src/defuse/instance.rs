// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The compilation-local definition instance table — the one recursion
//! detector for definition use.
//!
//! An instantiation installs its instance BEFORE opening the family body,
//! so a self-reference reached while the body resolves finds the
//! in-progress instance by IDENTITY: the family's definition identity (its
//! namespace and canonical name, as selected from the statement's one
//! catalog state) plus the semantic actual key. A same-key self-use re-enters the active
//! fixpoint; a different-key self-use of the in-progress family is the
//! ruled terminal refusal `semantic/recursion/parameter-widening` and
//! never begins another expansion. The detector is family-aware as well as
//! key-aware: the widening judgment is made against every open instance of
//! the family, never by missing an exact-key lookup and expanding fresh.
//!
//! The table is compilation-local and shared across resolver config clones
//! the way the instantiation-depth budget is; entries are removed by the
//! frame guard when the instantiation that installed them completes or
//! unwinds.

use std::cell::RefCell;
use std::rc::Rc;

/// The durable half of an instance key: which family, by identity — the
/// declaring namespace plus the canonical name the catalog stores (never a
/// rendered call spelling). One statement reads one catalog state, so this
/// pair names exactly one current family for the statement's extent.
/// Constructible only FROM a selected family: a caller cannot choose an
/// identity independently of the family it selected.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FamilyIdentity {
    /// The declaring namespace; empty for a query-scoped binding, which no
    /// namespace publishes.
    namespace: String,
    name: String,
    /// Fully qualified authored identity, carried only for cycle evidence.
    display: String,
}

/// The durable identity sealed into a closed consulted rule value. Its
/// fields remain private to the instance authority; later admission spends
/// this exact identity without selecting by name again.
#[derive(Debug, Clone)]
pub(in crate::defuse) struct ClosedFamilyIdentity(FamilyIdentity);

impl ClosedFamilyIdentity {
    pub(in crate::defuse) fn of(family: &super::select::LinkedFamily) -> Self {
        ClosedFamilyIdentity(FamilyIdentity::of(family))
    }
}

impl FamilyIdentity {
    pub(in crate::defuse) fn of(family: &super::select::LinkedFamily) -> Self {
        let name = if family.name().is_stropped() {
            family.name().as_str().to_string()
        } else {
            family.name().as_str().to_ascii_lowercase()
        };
        FamilyIdentity {
            namespace: family.namespace().to_string(),
            name,
            display: format!("{}::{}", family.namespace(), family.name()),
        }
    }
}

/// One open (in-progress) instance.
#[derive(Debug, Clone)]
struct OpenInstance {
    /// The entry's own identity: which installation this row IS, so the
    /// frame that installed it removes exactly this row however frames
    /// are moved or dropped out of lexical order.
    entry: u64,
    family: FamilyIdentity,
    /// The semantic actual key: one canonical structural serialization per
    /// declared parameter, in declaration order. Structural identity of the
    /// normalized actual — never authored bytes (the normalizer already
    /// spent spelling), and empty for an unparameterized use.
    actuals: Vec<String>,
    /// The frontier this instance minted for its own clauses, once the
    /// opened definition minted one. A same-key re-entry receives it BY
    /// IDENTITY and never reconstructs it from spelling.
    frontier: Option<DefinitionFrontier>,
}

/// Exact identity of one open definition instance. The instance authority
/// mints it and a compiler-built recursive frontier carries it unchanged;
/// spelling is never accepted as substitute evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DefinitionInstance(u64);

impl crate::lispy::ToLispy for DefinitionInstance {
    fn to_lispy(&self) -> String {
        format!("instance#{}", self.0)
    }
}

/// The recursive frontier of one exact opened definition use.
///
/// The fields and minting act stay inside the instance authority: the family
/// identity, authored target, badge, and live instance are captured from one
/// held frame. Downstream phases may preserve this value or ask its lexical
/// and badge questions, but cannot transplant its instance evidence onto
/// another target.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DefinitionFrontier {
    instance: DefinitionInstance,
    family: FamilyIdentity,
    name: delightql_types::SqlIdentifier,
    fixpoint: crate::pipeline::asts::vocabulary::Fixpoint,
}

impl DefinitionFrontier {
    pub(crate) fn name(&self) -> &delightql_types::SqlIdentifier {
        &self.name
    }

    pub(crate) fn fixpoint(&self) -> crate::pipeline::asts::vocabulary::Fixpoint {
        self.fixpoint
    }
}

impl crate::lispy::ToLispy for DefinitionFrontier {
    fn to_lispy(&self) -> String {
        format!(
            "(definition_frontier (name {}) (instance {}))",
            crate::lispy::ToLispy::to_lispy(&self.name),
            crate::lispy::ToLispy::to_lispy(&self.instance)
        )
    }
}

/// The closed outcome of asking to begin an instantiation.
pub(crate) enum Admission {
    /// No open instance of this family: the caller may expand, holding the
    /// frame for the duration.
    Fresh(InstanceFrame),
    /// An open instance of this family with the SAME semantic actual key:
    /// the use re-enters the active fixpoint, whose frontier — when the
    /// open instance minted one — travels by identity.
    Reenter {
        frontier: Option<DefinitionFrontier>,
    },
    /// The requested family is open, but it is not the immediately enclosing
    /// family. This is mutual/indirect recursion, not self-reentry.
    Cycle { chain: Vec<String> },
    /// An open instance of this family with a DIFFERENT semantic actual
    /// key: the ruled terminal refusal. Parameters configure the fixpoint;
    /// changing recursive state belongs in ordinary relation columns.
    Widening {
        building: Vec<String>,
        requested: Vec<String>,
    },
}

/// The one recursion detector. Cloning shares the table, so every nested
/// resolver configuration consults the same open set.
#[derive(Debug, Clone, Default)]
pub(crate) struct InstanceTable {
    open: Rc<RefCell<Vec<OpenInstance>>>,
    next_entry: Rc<std::cell::Cell<u64>>,
}

impl InstanceTable {
    /// Judge a lexical CTE read that may be the established frontier of an
    /// open relation definition. The immediately enclosing family owns its
    /// own frontier. Reaching an earlier family's frontier while a different
    /// family is open closes a cycle in the definition-instance graph.
    pub(in crate::defuse) fn frontier_cycle(
        &self,
        frontier: &DefinitionFrontier,
    ) -> Option<Vec<String>> {
        let open = self.open.borrow();
        let position =
            open.iter().enumerate().rev().find_map(|(position, open)| {
                (open.entry == frontier.instance.0).then_some(position)
            })?;
        if position + 1 == open.len() {
            return None;
        }
        let mut chain = open[position..]
            .iter()
            .map(|instance| instance.family.display.clone())
            .collect::<Vec<_>>();
        chain.push(open[position].family.display.clone());
        Some(chain)
    }

    /// The frame-level admission for the BOUND-USE carrier: the identity
    /// derives from the family, the key from the carrier's own actuals.
    pub(in crate::defuse) fn admit_identity(
        &self,
        family: &super::select::LinkedFamily,
        actuals: Vec<String>,
    ) -> Admitted {
        match self.enter(FamilyIdentity::of(family), actuals) {
            Admission::Fresh(frame) => Admitted::Fresh(frame),
            Admission::Reenter { frontier } => Admitted::Reenter { frontier },
            Admission::Cycle { chain } => Admitted::Cycle { chain },
            Admission::Widening {
                building,
                requested,
            } => Admitted::Widening {
                building,
                requested,
            },
        }
    }

    pub(in crate::defuse) fn admit_closed(
        &self,
        family: &ClosedFamilyIdentity,
        actuals: Vec<String>,
    ) -> Admitted {
        match self.enter(family.0.clone(), actuals) {
            Admission::Fresh(frame) => Admitted::Fresh(frame),
            Admission::Reenter { frontier } => Admitted::Reenter { frontier },
            Admission::Cycle { chain } => Admitted::Cycle { chain },
            Admission::Widening {
                building,
                requested,
            } => Admitted::Widening {
                building,
                requested,
            },
        }
    }

    /// ADMIT one QUERY-SCOPED definition use (an inline `cfes` binding —
    /// no publication, no namespace; its identity is the binding name
    /// within the compilation, under the identifier law's folding).
    pub(in crate::defuse) fn admit_scoped(
        &self,
        name: &delightql_types::SqlIdentifier,
        actuals: Vec<String>,
    ) -> ScopedAdmission {
        let folded = if name.is_stropped() {
            name.as_str().to_string()
        } else {
            name.as_str().to_ascii_lowercase()
        };
        match self.enter(
            FamilyIdentity {
                namespace: String::new(),
                name: folded,
                display: name.to_string(),
            },
            actuals,
        ) {
            Admission::Fresh(frame) => ScopedAdmission::Fresh(frame),
            Admission::Reenter { .. } => ScopedAdmission::Reenter,
            // Query-scoped forward references make mutual recursion
            // unwritable; retain the closed variant if a constructed tree
            // nevertheless reaches the instance authority.
            Admission::Cycle { chain } => ScopedAdmission::Cycle { chain },
            Admission::Widening {
                building,
                requested,
            } => ScopedAdmission::Widening {
                building,
                requested,
            },
        }
    }

    fn enter(&self, family: FamilyIdentity, actuals: Vec<String>) -> Admission {
        {
            let open = self.open.borrow();
            for (position, instance) in open.iter().enumerate().rev() {
                if instance.family == family {
                    if position + 1 != open.len() {
                        let mut chain = open[position..]
                            .iter()
                            .map(|open| open.family.display.clone())
                            .collect::<Vec<_>>();
                        chain.push(family.display.clone());
                        return Admission::Cycle { chain };
                    }
                    if instance.actuals == actuals {
                        return Admission::Reenter {
                            frontier: instance.frontier.clone(),
                        };
                    }
                    return Admission::Widening {
                        building: instance.actuals.clone(),
                        requested: actuals,
                    };
                }
            }
        }
        let entry = self.next_entry.get();
        self.next_entry.set(entry + 1);
        let frame_family = family.clone();
        self.open.borrow_mut().push(OpenInstance {
            entry,
            family,
            actuals,
            frontier: None,
        });
        Admission::Fresh(InstanceFrame {
            open: Rc::clone(&self.open),
            entry,
            family: frame_family,
        })
    }
}

/// The frame-level closed admission outcome the bound-use carrier
/// consumes.
pub(in crate::defuse) enum Admitted {
    Fresh(InstanceFrame),
    Reenter {
        frontier: Option<DefinitionFrontier>,
    },
    Cycle {
        chain: Vec<String>,
    },
    Widening {
        building: Vec<String>,
        requested: Vec<String>,
    },
}

/// The closed outcome of admitting a QUERY-SCOPED definition use: the
/// frame is the held instance (there is no family to open — the binding
/// already carries its shaped body).
pub(in crate::defuse) enum ScopedAdmission {
    Fresh(InstanceFrame),
    Reenter,
    Cycle {
        chain: Vec<String>,
    },
    Widening {
        building: Vec<String>,
        requested: Vec<String>,
    },
}

/// RAII frame: the instance stays open exactly as long as its expansion,
/// unwinding included. The frame removes EXACTLY the entry it installed —
/// identified, not positional — so frames moved or dropped out of lexical
/// order can never remove another family's instance.
pub(crate) struct InstanceFrame {
    open: Rc<RefCell<Vec<OpenInstance>>>,
    entry: u64,
    family: FamilyIdentity,
}

impl InstanceFrame {
    /// Mint THIS instance's frontier and record it on the open entry, so a
    /// same-key re-entry receives the same value by identity.
    pub(in crate::defuse) fn frontier(
        &self,
        group: &crate::pipeline::asts::ddl::DefinitionGroup,
    ) -> DefinitionFrontier {
        let frontier = DefinitionFrontier {
            instance: DefinitionInstance(self.entry),
            family: self.family.clone(),
            name: delightql_types::SqlIdentifier::new(group.name()),
            fixpoint: group.fixpoint(),
        };
        for open in self.open.borrow_mut().iter_mut() {
            if open.entry == self.entry {
                open.frontier = Some(frontier.clone());
            }
        }
        frontier
    }
}

impl Drop for InstanceFrame {
    fn drop(&mut self) {
        self.open
            .borrow_mut()
            .retain(|instance| instance.entry != self.entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family(name: &str) -> FamilyIdentity {
        FamilyIdentity {
            namespace: "lib::t".to_string(),
            name: name.to_string(),
            display: name.to_string(),
        }
    }

    fn frontier(frame: &InstanceFrame, name: &str) -> DefinitionFrontier {
        DefinitionFrontier {
            instance: DefinitionInstance(frame.entry),
            family: family(name),
            name: name.into(),
            fixpoint: crate::pipeline::asts::vocabulary::Fixpoint::Bag,
        }
    }

    #[test]
    fn same_key_reenters_and_different_key_widens() {
        let table = InstanceTable::default();
        let _frame = match table.enter(family("reach"), vec!["\"b\"".into()]) {
            Admission::Fresh(frame) => frame,
            _ => panic!("first entry is fresh"),
        };
        assert!(matches!(
            table.enter(family("reach"), vec!["\"b\"".into()]),
            Admission::Reenter { .. }
        ));
        assert!(matches!(
            table.enter(family("reach"), vec!["\"x\"".into()]),
            Admission::Widening { .. }
        ));
        // A different family is untouched by the open instance.
        assert!(matches!(
            table.enter(family("other"), vec![]),
            Admission::Fresh(_)
        ));
    }

    #[test]
    fn frontier_cycle_uses_the_exact_open_instance() {
        let table = InstanceTable::default();
        let first = match table.enter(family("foo"), Vec::new()) {
            Admission::Fresh(frame) => frame,
            _ => panic!("first entry is fresh"),
        };
        let second = match table.enter(family("bar"), Vec::new()) {
            Admission::Fresh(frame) => frame,
            _ => panic!("second family is fresh"),
        };

        let first_frontier = frontier(&first, "foo");
        let second_frontier = frontier(&second, "bar");
        let cycle = table
            .frontier_cycle(&first_frontier)
            .expect("an earlier exact instance closes a mutual cycle");
        assert_eq!(cycle, vec!["foo", "bar", "foo"]);
        assert_eq!(table.frontier_cycle(&second_frontier), None);

        let planning = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let local_relation = crate::relation::any_scratch(&planning).relation();
        let mut local = crate::defuse::environment::Environment::Use(
            crate::defuse::environment::UseEnvironment::detached(),
        );
        let mut names = crate::pipeline::asts::core::QueryLocalNames::default();
        names
            .declare(
                "foo".into(),
                crate::pipeline::asts::core::QueryLocalKind::Relation,
            )
            .expect("one local claim");
        local.push_query_names(names);
        local.register_query_local(
            crate::defuse::environment::QueryLocalRegistration::Relation {
                name: "foo".into(),
                relation: local_relation,
            },
        );
        assert_eq!(
            local
                .select_query_local(
                    &"foo".into(),
                    crate::pipeline::asts::core::QueryLocalDemand::Relation,
                    None,
                )
                .expect("local selection")
                .and_then(|selected| match selected {
                    crate::defuse::environment::QueryLocalSelection::Relation(cte) => {
                        cte.frontier()
                    }
                    _ => unreachable!("relation demand returns a relation"),
                }),
            None,
            "an unrelated lexical CTE with the same spelling carries no instance evidence"
        );
    }

    #[test]
    fn a_frame_dropped_out_of_order_removes_only_its_own_entry() {
        let table = InstanceTable::default();
        let outer = match table.enter(family("outer"), vec![]) {
            Admission::Fresh(frame) => frame,
            _ => panic!("fresh"),
        };
        let inner = match table.enter(family("inner"), vec![]) {
            Admission::Fresh(frame) => frame,
            _ => panic!("fresh"),
        };
        // Dropping the OUTER frame first must not evict the inner family.
        drop(outer);
        assert!(matches!(
            table.enter(family("inner"), vec![]),
            Admission::Reenter { .. }
        ));
        assert!(matches!(
            table.enter(family("outer"), vec![]),
            Admission::Fresh(_)
        ));
        drop(inner);
        assert!(matches!(
            table.enter(family("inner"), vec![]),
            Admission::Fresh(_)
        ));
    }

    #[test]
    fn reentering_a_non_current_family_reports_the_complete_cycle() {
        let table = InstanceTable::default();
        let _even = match table.enter(family("even"), vec![]) {
            Admission::Fresh(frame) => frame,
            _ => panic!("even is fresh"),
        };
        let _odd = match table.enter(family("odd"), vec![]) {
            Admission::Fresh(frame) => frame,
            _ => panic!("odd is fresh"),
        };
        match table.enter(family("even"), vec![]) {
            Admission::Cycle { chain } => assert_eq!(chain, ["even", "odd", "even"]),
            _ => panic!("the non-current reentry is a mutual cycle"),
        }
    }

    #[test]
    fn the_frame_closes_the_instance_on_drop() {
        let table = InstanceTable::default();
        {
            let _frame = match table.enter(family("v"), vec![]) {
                Admission::Fresh(frame) => frame,
                _ => panic!("fresh"),
            };
            assert!(matches!(
                table.enter(family("v"), vec![]),
                Admission::Reenter { .. }
            ));
        }
        assert!(matches!(
            table.enter(family("v"), vec![]),
            Admission::Fresh(_)
        ));
    }
}
