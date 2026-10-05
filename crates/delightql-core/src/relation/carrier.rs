// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

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
/// with one. Holding a landing manufactures nothing; a record answers for
/// the carrier it names, or does not.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StructuralRelation {
    pub(super) id: u32,
    pub(super) mark: BuilderMark,
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

/// A SCRATCH ROW A PLAN ALLOCATED, as the receipt of that allocation.
/// Minted only by the authority's scratch derivation. A plan reads its
/// own scratch by this receipt; a copied identity is not one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ScratchRow {
    relation: SemanticRelation,
}

impl ScratchRow {
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
    /// The occurrence.
    pub fn relation(&self) -> RelationId {
        self.relation
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

