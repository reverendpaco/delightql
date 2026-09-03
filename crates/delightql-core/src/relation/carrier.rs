// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The atomic result: a relation occurrence and the exact ordered interface
//! it publishes, as ONE value.
//!
//! Two directions of drift are forbidden and both are forbidden the same
//! way. A valid relation cannot be paired with a foreign interface, and a
//! valid interface cannot be attached to a different relation, because
//! neither half is ever handed out on its own and the pair has no
//! constructor outside [`super::SemanticBuilder`].
//!
//! An unforgeable heading alone would not do it: a caller who can attach a
//! valid heading to the wrong node has still built the wrong relation.
//!
//! The interface lives ON the relation record the authority owns, not in a
//! second store a caller could index. There is therefore nothing to pair:
//! asking a relation what it publishes goes through the authority, and the
//! authority answers from the record it built.

use super::port::RelationId;

/// One row-producing semantic result.
///
/// `Copy`, because the PRESERVE law is real: a restriction, a bound, and
/// a whole-heading correlation continue the relation standing to their
/// left rather than starting a new one, and the way they say so is by
/// carrying the same result. What is forbidden is MANUFACTURE, not
/// carriage — there is no constructor, no `Default`, no `From<ScopeId>`,
/// no `From<(RelationId, Interface)>`, and no setter for either half.
#[derive(Clone, Copy)]
pub struct SemanticRelation {
    relation: RelationId,
    scope: crate::names::ScopeId,
    /// Which compilation produced this. The authority checks it at its one
    /// entrance, so a result built against another compilation's registry
    /// refuses instead of being read against identities this one never
    /// issued.
    origin: BuilderMark,
}

/// THE NAME A BODY ADDRESSES A CARRIER BY. Deliberately not a semantic
/// relation and deliberately not a relationship: a landing is reserved by
/// the bind that also derives the carrier it names, as one act, so there
/// is never a landing waiting for a body that some other code could pair
/// with one. A proffer placeholder is reserved alone, and no carrier ever
/// answers to it. Holding a landing manufactures nothing; a record answers
/// for the carrier it names, or does not.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StructuralRelation {
    pub(super) id: u32,
    pub(super) mark: BuilderMark,
    pub(super) part: super::form::HoPart,
}

impl StructuralRelation {
    /// Which part of a higher-order call this landing was reserved for.
    pub(crate) fn part(&self) -> super::form::HoPart {
        self.part
    }
}

impl std::fmt::Debug for StructuralRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "structural#{}", self.id)
    }
}

impl crate::lispy::ToLispy for StructuralRelation {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}

/// A STRUCTURAL CARRIER, BOUND: the landing a body addresses it by and the
/// relation it publishes, as ONE value. Minted only by the authority's
/// bind, which reserves the landing and instantiates the body under it in
/// the same act — so a landing and a carrier are never handed out on their
/// own, and there is nothing to pair. `Copy`, because carriage of a fact
/// is not manufacture of one: nothing accepts this value beside another
/// landing, body, subject or role.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CarrierRow {
    landing: StructuralRelation,
    relation: SemanticRelation,
}

impl CarrierRow {
    pub(super) fn bound(landing: StructuralRelation, relation: SemanticRelation) -> Self {
        CarrierRow { landing, relation }
    }

    /// The name the body addresses this carrier by.
    pub fn landing(&self) -> StructuralRelation {
        self.landing
    }

    /// The relation the carrier publishes.
    pub fn relation(&self) -> SemanticRelation {
        self.relation
    }
}

impl std::fmt::Debug for CarrierRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "carrier({:?} = {:?})", self.landing, self.relation)
    }
}

/// A SCRATCH ROW A PLAN ALLOCATED, as the receipt of that allocation.
/// Minted only by the authority's scratch derivation. A plan reads its
/// own scratch by this receipt; a copied identity is not one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ScratchRow {
    relation: SemanticRelation,
}

impl ScratchRow {
    pub(super) fn minted(relation: SemanticRelation) -> Self {
        ScratchRow { relation }
    }

    pub fn relation(&self) -> SemanticRelation {
        self.relation
    }

}

impl std::fmt::Debug for ScratchRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "scratch({:?})", self.relation)
    }
}

impl crate::lispy::ToLispy for ScratchRow {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}

/// A scratch row and the authored name it answers to, paired by the plan
/// that placed the row where the name was written. The only spelling a
/// compiler-owned row ever carries into resolution.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NamedScratch {
    row: ScratchRow,
    name: delightql_types::SqlIdentifier,
}

impl NamedScratch {
    /// THE ROW UNDER THE NAME ITS OWNER WROTE: a directive's receipt under
    /// its `as`, a mutation target read through its own snapshot. The
    /// spelling is paired with the receipt only by the plan that places
    /// the row where the name is written, whose witness this takes; the
    /// pair travels as one value from then on.
    pub fn under(
        row: ScratchRow,
        name: delightql_types::SqlIdentifier,
        _witness: crate::pipeline::effect_transformer::ReceiptNaming,
    ) -> NamedScratch {
        NamedScratch { row, name }
    }

    #[cfg(test)]
    pub(crate) fn for_test(row: ScratchRow, name: delightql_types::SqlIdentifier) -> NamedScratch {
        NamedScratch { row, name }
    }

    pub fn row(&self) -> ScratchRow {
        self.row
    }

    pub fn name(&self) -> &delightql_types::SqlIdentifier {
        &self.name
    }
}

impl crate::lispy::ToLispy for NamedScratch {
    fn to_lispy(&self) -> String {
        format!("{:?} as {}", self.row, self.name)
    }
}

/// One compilation's identity within the process.
///
/// A runtime discriminator rather than a generative lifetime: branding by
/// invariant lifetime would have to ride every relation-bearing AST node
/// and every phase-parameterised type between here and SQL lowering, and
/// the misuse it prevents — handing one compilation's relation to another
/// compilation's authority — is caught by one comparison at the entrance.
///
/// Keyed to the REGISTRY, not to a builder object: the identities a
/// relation names belong to the compilation, so two authorities over one
/// registry are one epoch and must agree.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct BuilderMark(pub(super) u64);

impl SemanticRelation {
    /// The ONE producer. Private to the authority: `derive` calls it after
    /// it has judged the form and built the interface, and nothing else
    /// can reach it.
    pub(super) fn pair(
        relation: RelationId,
        scope: crate::names::ScopeId,
        origin: BuilderMark,
    ) -> Self {
        SemanticRelation {
            relation,
            scope,
            origin,
        }
    }

    /// The occurrence.
    pub fn relation(&self) -> RelationId {
        self.relation
    }

    pub(super) fn origin(&self) -> BuilderMark {
        self.origin
    }

    /// The registry occurrence this relation is stored under.
    ///
    /// READ-ONLY, and there is no inverse. The phases that have not yet
    /// moved onto the authority still ask the registry structural
    /// questions about a relation, and this is how they name it; a scope
    /// cannot become a relation, so holding one manufactures nothing.
    pub(crate) fn scope(&self) -> crate::names::ScopeId {
        self.scope
    }
}

/// Two relations are the same result when they are the same occurrence of
/// the same compilation.
///
/// The interface is derived from the occurrence by one authority, so
/// comparing it as well would be comparing one fact twice.
impl PartialEq for SemanticRelation {
    fn eq(&self, other: &Self) -> bool {
        self.relation == other.relation && self.origin == other.origin
    }
}

impl Eq for SemanticRelation {}

impl std::hash::Hash for SemanticRelation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.relation.hash(state);
    }
}

impl std::fmt::Debug for SemanticRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.relation)
    }
}

impl crate::lispy::ToLispy for SemanticRelation {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::super::form::{AnonymousShape, AnonymousSpec, ExportSpec, ExportWhy};
    use super::super::RelForm;

    /// THE BRANDING QUESTION, answered with code.
    ///
    /// Two compilations, two registries. A relation built against one and
    /// handed to the other's authority is caught at the one entrance — a
    /// relation names identities its own compilation issued, and reading
    /// it against another's would index a store that never heard of it.
    ///
    /// A generative lifetime would make this a compile error instead. It
    /// would also have to ride every relation-bearing AST node and every
    /// phase-parameterised type between here and SQL lowering, which is
    /// the cost the comparison exists to weigh.
    #[test]
    fn a_relation_from_another_compilation_refuses_at_the_entrance() {
        let first = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let second = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let theirs = first
            .authority()
            .derive(RelForm::Anonymous(AnonymousSpec {
                shape: AnonymousShape::Tabular,
                slots: &[],
                answers_to: None,
            }))
            .expect("an anonymous relation is built");

        let ours = second.authority();
        let refused = ours.derive(RelForm::Export(ExportSpec {
            input: theirs,
            why: ExportWhy::EmissionAlias,
        }));
        assert!(
            refused.is_err(),
            "another compilation's relation cannot be an operand here"
        );
        assert!(
            ours.interface(&theirs).is_err(),
            "another compilation's relation cannot be read here"
        );
        assert!(
            ours.report_replacement_for_test(theirs, theirs).is_err(),
            "another compilation's relation cannot be refined here"
        );
    }

    /// The same authority reached twice is ONE epoch: the mark is the
    /// registry's, so a second builder over one compilation is the same
    /// road, not a second one.
    #[test]
    fn two_authorities_over_one_registry_are_one_epoch() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let built = registry
            .authority()
            .derive(RelForm::Anonymous(AnonymousSpec {
                shape: AnonymousShape::Tabular,
                slots: &[],
                answers_to: None,
            }))
            .expect("an anonymous relation is built");
        let elsewhere = registry.authority();
        assert!(elsewhere.interface(&built).is_ok());
        assert!(elsewhere.derive(RelForm::Order(built)).is_ok());
    }

    /// A sealed store reads and cannot construct, and it checks the epoch
    /// the same way — against the registry it holds, which a caller has no
    /// road to substitute.
    #[test]
    fn a_sealed_store_reads_only_its_own_epoch() {
        let first = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let second = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let theirs = first
            .authority()
            .derive(RelForm::Anonymous(AnonymousSpec {
                shape: AnonymousShape::Tabular,
                slots: &[],
                answers_to: None,
            }))
            .expect("an anonymous relation is built");
        let sealed = second.seal();
        assert!(sealed.interface(&theirs).is_err());
    }
}
