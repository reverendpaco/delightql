// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What a column reference IS, in the two states a phase can select.
//!
//! One node names a column — `DomainExpression::Lvar` — and the phase says
//! whether it holds characters nobody has looked up or the occurrence
//! something bound it to. There is no authored/resolved variant pair, so a
//! consumer cannot ask a resolved tree for a spelling that is gone, and a
//! walk cannot carry an unresolved name past the pass that was supposed to
//! bind it.

use super::metadata::NamespacePath;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// A column reference while the tree is still the authored one: the
/// characters as written, and nothing else.
///
/// There is no second state here. A pass that has already decided which
/// column a position names does not write that decision back into an
/// authored tree — it either produces a resolved expression at its own
/// boundary, or it leaves the binding to the caller pattern the language
/// binds with.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("column:spelled")]
pub struct AuthoredColumn {
    ///
    /// # Field Semantics (CRITICAL)
    ///
    /// - `name`: Column name (e.g., "last_name")
    /// - `qualifier`: Table/alias reference (e.g., "u" in `u.last_name`)
    ///   - This identifies WHICH table's column (could be alias!)
    ///   - NOT part of namespace path - it's a table reference
    /// - `namespace_path`: WHERE to find the table (catalog/schema levels)
    ///   - In `catalog.schema.table.column`: namespace_path = [schema, catalog]
    ///   - Separate from qualifier because qualifier can be an alias
    ///
    /// Example: `prod.dbo.users.name as u` then `u.email` later
    /// - First reference: namespace_path=[dbo, prod], qualifier=Some("users"), name="name"
    /// - Second reference: namespace_path=[], qualifier=Some("u"), name="email"
    pub name: SqlIdentifier,
    /// Table qualifier/reference (table name or alias)
    pub qualifier: Option<SqlIdentifier>,
    /// Namespace path (WHERE to find table: schema, database, catalog, etc.)
    pub namespace_path: NamespacePath,
}

/// The column a reference names, once something has bound it.
///
/// CONSTRUCTED ONLY THROUGH ITS OWN DOORS. A resolved reference is one of
/// two things, and the door says which: the lexical frontier's terminal
/// answer to an AUTHORED spelling ([`ColumnOccurrence::addressed`], which
/// only that authority can present the proof for), or a position the
/// ENGINE derived from a port it minted or carried
/// ([`ColumnOccurrence::engine`]). Nothing holding a port identity can
/// state that an author addressed it: that statement is a lookup, and the
/// frontier is the one road a lookup takes.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnOccurrence {
    pub column: crate::relation::PortId,
    /// Whether the source reference contained an explicit qualifier.
    ///
    /// Resolution erases qualifier characters, but later ambiguity laws
    /// still distinguish explicit references from bare ones.
    pub explicit_qualifier: bool,
    /// The door this occurrence came through. Private, so no struct
    /// literal outside this module can build one.
    minted: Minted,
}

/// The mark of a door. Zero-sized, private: its only value is that a
/// struct literal elsewhere cannot supply it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Minted;

impl ColumnOccurrence {
    /// THE FRONTIER'S TERMINAL ANSWER to an authored spelling: which live
    /// occurrence the reference addresses, and whether the author qualified
    /// it. The proof is minted by the lexical authority alone, at the
    /// judgment, so an addressed occurrence cannot be forged from a port.
    pub(crate) fn addressed(
        column: crate::relation::PortId,
        explicit_qualifier: bool,
        _judged: crate::pipeline::resolver::Terminal,
    ) -> Self {
        Self {
            column,
            explicit_qualifier,
            minted: Minted,
        }
    }

    /// A POSITION THE ENGINE DERIVED — a port the construction authority
    /// minted or carried, referenced by the compiler's own rewrite. Never
    /// the answer to an authored spelling: no spelling was searched.
    pub fn engine(column: crate::relation::PortId) -> Self {
        Self {
            column,
            explicit_qualifier: false,
            minted: Minted,
        }
    }

    /// An engine-derived position the lowering must SPELL qualified — a
    /// correlation between two operands' ports, where a bare spelling
    /// would be ambiguous in the emitted statement. Qualification here is
    /// a rendering fact about the engine's own reference; it is not an
    /// authored qualifier and grants no addressing.
    pub fn engine_qualified(column: crate::relation::PortId) -> Self {
        Self {
            column,
            explicit_qualifier: true,
            minted: Minted,
        }
    }

    /// THE SAME REFERENCE, standing on another port: a republication moved
    /// the position and the reference follows it, keeping whether the
    /// author qualified it. Continuity of an occurrence already judged,
    /// never a new judgment.
    pub fn rebound(&self, column: crate::relation::PortId) -> Self {
        Self {
            column,
            explicit_qualifier: self.explicit_qualifier,
            minted: Minted,
        }
    }
}

impl crate::lispy::ToLispy for ColumnOccurrence {
    fn to_lispy(&self) -> String {
        format!(
            "(column:occurrence (column {}) (explicit_qualifier {}))",
            self.column.to_lispy(),
            self.explicit_qualifier.to_lispy()
        )
    }
}

/// The name a caller-pattern slot offers, as authored.
///
/// A slot binds by being nothing but a bare name: `t(a, _, 30)` offers `a`
/// and nothing else. The qualification a written name may carry travels with
/// it, because a binder that dropped it would not reconstruct the term it
/// was classified from.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("binder:written")]
pub struct WrittenBinder {
    pub name: SqlIdentifier,
    pub namespace_path: NamespacePath,
}

/// `@` in an invocation's argument list: the formal that receives the piped
/// relation.
///
/// It carries nothing. Which formal it marks is its POSITION, and the
/// invocation that reads it records that as an argument role — so the mark
/// is consumed where it is read and never reaches a resolved tree.
#[derive(Debug, Clone, PartialEq, Eq, ToLispy)]
#[lispy("at_sign")]
pub struct AtSign;

/// `..` in a call's argument row: the context calling mode of a
/// context-aware definition.
///
/// It carries nothing. Standing FIRST in the row is what selects the mode,
/// and instantiation consumes it — so the mark never reaches a resolved
/// tree, and no value position can hold one at all.
#[derive(Debug, Clone, PartialEq, Eq, ToLispy)]
#[lispy("context_marker")]
pub struct ContextMarker;

#[cfg(test)]
mod lifecycle {
    use super::*;
    use crate::pipeline::asts::core::{Phase, Refined, Resolved, Slot, Unresolved};

    /// The authored column is a SPELLING and nothing else. A pass that has
    /// already chosen a column does not get to say so here.
    #[test]
    fn an_authored_column_carries_only_characters() {
        let written = AuthoredColumn {
            name: "x".into(),
            qualifier: None,
            namespace_path: NamespacePath::empty(),
        };
        // The type has one shape; there is no `Bound` arm to take.
        let AuthoredColumn { name, .. } = &written;
        assert_eq!(name.as_str(), "x");
    }

    /// A slot that binds stays a slot that binds. Only the payload changes.
    #[test]
    fn a_binding_slot_survives_resolution_as_a_binding_slot() {
        let authored: Slot<Unresolved> = Unresolved::classify_column(AuthoredColumn {
            name: "x".into(),
            qualifier: None,
            namespace_path: NamespacePath::empty(),
        });
        assert!(matches!(authored, Slot::Bind(_)), "a bare name binds");

        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let named = registry.intern("x", false);
        let relation = registry
            .authority()
            .derive(crate::relation::RelForm::Anonymous(
                crate::relation::form::AnonymousSpec {
                    shape: crate::relation::form::AnonymousShape::Tabular,
                    slots: &[crate::relation::form::AnonymousSlot::Binder {
                        position: 0,
                        named,
                        declared_type: None,
                        shape: crate::names::ValueShape::Unknown,
                    }],
                    answers_to: None,
                },
            ))
            .unwrap();
        let port = crate::relation::published_ports(&registry, &relation).unwrap()[0];
        let bound: Slot<Resolved> = Slot::Bind(port);
        assert_eq!(bound.binder().copied(), Some(port));
    }

    /// A qualified name REUSES somebody else's column: it addresses the
    /// enclosing logical value rather than offering a name for it, and reuse
    /// is its own slot — not the constraint a term computes.
    #[test]
    fn a_qualified_name_does_not_bind() {
        let slot = Unresolved::classify_column(AuthoredColumn {
            name: "x".into(),
            qualifier: Some("t".into()),
            namespace_path: NamespacePath::empty(),
        });
        assert!(matches!(slot, Slot::Reuse(_)));
        assert!(slot.binder().is_none());
        assert!(slot.reuse().is_some());
        assert!(slot.constraint().is_none());
    }

    /// `@` has two meanings and two carriers, and only one is a value leaf.
    /// The landing is a higher-order argument-row mark whose payload is
    /// uninhabited after resolution; the composition input is the open
    /// leaf, travelling to whatever instantiation applies its body — in
    /// every phase, including the one the lowering reads.
    #[test]
    fn the_two_at_carriers_have_two_lifecycles() {
        use crate::pipeline::asts::core::operators::HoArgument;

        // The landing exists while authored...
        let landing: HoArgument<Unresolved> = HoArgument::Landing(AtSign);
        assert!(matches!(landing, HoArgument::Landing(_)));

        // ...and after resolution its payload is uninhabited, so no resolved
        // or refined argument row can hold one. These bindings compile only
        // because `Never` is what each phase selects.
        fn landing_is_impossible<
            P: Phase<Placeholder = crate::pipeline::asts::vocabulary::Never>,
        >() {
        }
        landing_is_impossible::<Resolved>();
        landing_is_impossible::<Refined>();

        // The context marker is spent at instantiation the same way.
        fn marker_is_impossible<
            P: Phase<ContextMarker = crate::pipeline::asts::vocabulary::Never>,
        >() {
        }
        marker_is_impossible::<Resolved>();
        marker_is_impossible::<Refined>();

        // The open leaf is spent by the invocation that applies its body:
        // no REFINED phase can carry one — not an unapplied hole, none.
        // (The resolved phase carries the closed-callable FormalHole,
        // substituted before refinement.)
        fn leaf_is_impossible<P: Phase<OpenLeaf = crate::pipeline::asts::vocabulary::Never>>() {}
        leaf_is_impossible::<Refined>();
    }
}
