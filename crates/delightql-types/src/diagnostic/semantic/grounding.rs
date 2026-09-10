// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `semantic/ground/…` and `semantic/grounding/…` — binding worlds and
//! selecting edges by spelling.

use super::Semantic;
use crate::diagnostic::{DelightQLError, Taxon};

/// `semantic/ground/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Ground))]
pub enum Ground {
    /// The ground namespace and groundable namespace cannot share entity
    /// names: if both define the name, every use of it is ambiguous, so
    /// grounding refuses whole and creates nothing (No intersection). Rename
    /// the library's entity or ground against a data namespace that does
    /// not define it.
    #[leaf("name_intersection", class = Syntax, summary = "ground!'s library and data namespace share an entity name.")]
    #[error("Validation error: {message}")]
    NameIntersection { message: String },

    /// ground! validates ALL of the library's references — a qualified
    /// reference must resolve where it points, and an unqualified free
    /// reference must exist in the data namespace. If any reference
    /// dangles, the entire operation fails and nothing is created.
    #[leaf("unresolved_reference", class = Syntax, summary = "ground!'s strict validation found a dangling qualified reference.")]
    #[error("Validation error: {message}")]
    UnresolvedReference { message: String },
}

/// `semantic/grounding/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Grounding))]
pub enum Grounding {
    /// A consulted body reads its own definitions and the data world an
    /// explicit grounding published — never the caller's tables, CTEs, or
    /// session database ambiently. ground! binds the world's data holes to a
    /// data namespace, or enlist! inside the consulted file links the
    /// namespaces it reads.
    #[leaf("data_hole_unbound", class = Syntax, summary = "A free data name of a consulted world that no ground! has bound.")]
    #[error("Validation error: {message}")]
    DataHoleUnbound { message: String },

    /// The ER family: edges are pair-sets selected by their ground terms'
    /// exact canonical spellings in a declared context.
    #[family("er", summary = "An entity-relationship edge operation failed.")]
    #[error(transparent)]
    Er(Er),

    /// A provable miss is an error, not an empty relation. The argument is
    /// written at the call site (a literal or mention), the parameter
    /// position is ground in every clause, and no clause head spells that
    /// value — emptiness by absent DECLARATION, which the catalog proves at
    /// compile time. The message enumerates the declared spellings. A
    /// data-borne value (a column, not a literal) keeps relational
    /// semantics and misses to empty; a free clause at the position makes
    /// every call satisfiable.
    #[leaf("head/provable_miss", class = Syntax, summary = "A literal ground argument matches no clause head.")]
    #[error("Validation error: {message}")]
    HeadProvableMiss { message: String },
}

/// `semantic/grounding/er/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Grounding, Grounding::Er))]
pub enum Er {
    /// Declaration-side terms are naked: the alias is call-site export
    /// vocabulary (people(*) as p & …), never part of the term that names
    /// the edge.
    #[leaf("alias_in_declaration", class = Syntax, summary = "An alias inside an edge-declaration term.")]
    #[error("Validation error: {message}")]
    AliasInDeclaration { message: String },

    /// An edge body reads exactly its two endpoints, each spelled as its
    /// term, and carries only simple conditions: at least one connection
    /// — a comparison between the two endpoints standing as a top-level
    /// conjunct, of any comparison kind — and filters over the endpoints'
    /// columns. A helper relation, an aliased or outer-marked or
    /// dequalifying read, a second read of an endpoint, an aggregate,
    /// window, probe, subquery or binding in a condition, a connection
    /// only inside a disjunction, or any stage puts the body outside the
    /// shape; it refuses when the edge is declared, naming the part.
    #[leaf("body_shape", class = Syntax, summary = "An edge body is not the simple shape.")]
    #[error("Validation error: {message}")]
    BodyShape { message: String },

    /// An edge is selected by its ground terms' exact CANONICAL spellings —
    /// people(, age >= 18) is a different term from people(, 18 <= age),
    /// deliberately, because identity decidable by bytes is the point of
    /// matching by encoding. Whitespace and identifier case normalize;
    /// semantics never do. Restriction is downstream: select a declared
    /// edge (in practice declared over glob terms), then filter its
    /// relation. The message lists the declared edges.
    #[leaf("edge_miss", class = Syntax, summary = "No edge declared for this pair of terms.")]
    #[error("Validation error: {message}")]
    EdgeMiss { message: String },

    /// One chain, one context. Split the chain into separate expressions,
    /// or declare the edges in one context.
    #[leaf("mixed_contexts", class = Syntax, summary = "One chain names more than one context.")]
    #[error("Validation error: {message}")]
    MixedContexts { message: String },

    /// Edges are selected by their terms' canonical spellings, so each
    /// operand of & and && must be a relation-access term — people(*),
    /// people(, age >= 18). The alias stays OUTSIDE the term (people(*) as
    /// p selects by people(*), exports answer to p), and so does the outer
    /// marker (orders?(*)).
    #[leaf("operand_term", class = Syntax, summary = "An ER operand is not a relation-access term.")]
    #[error("Validation error: {message}")]
    OperandTerm { message: String },

    /// The edge publishes the same table at both endpoints, so the two
    /// sides share every column name: the endpoint exports would bind one
    /// operand twice and the pairs would come back silently self-joined.
    /// Until the boundary can mask the sides apart, spell one side as a
    /// renamed rule view — boss(*) :- employees(*) — and declare the edge
    /// over the distinct terms; the call site then reads employees(*)
    /// &(::mgr) boss(*), each side addressable by its own name.
    /// A context exists exactly where an edge declares it — the edge set
    /// per context is finite and declared, so an unknown context is a hard
    /// error at first use, never an empty result. The message lists the
    /// contexts that DO have declared edges in the enlisted scope. Declare
    /// edges with A(*) &(::ctx) B(*) :- body, and enlist!() the namespace
    /// that declares them.
    #[leaf("unknown_context", class = Syntax, summary = "No edge is declared in this context.")]
    #[error("Validation error: {message}")]
    UnknownContext { message: String },

    /// An edge's endpoint term does not name a relation the edge body
    /// publishes, or names one on both sides.
    #[leaf("endpoint", class = Syntax, summary = "An edge endpoint is ill-determined.")]
    #[error("Validation error: {message}")]
    Endpoint { message: String },

    /// An outer marker (`?`) on an ER endpoint: the edge's pair-set decides
    /// which side is optional, and the marker's side is not one the
    /// declared edge admits.
    #[leaf("outer_endpoint", class = Syntax, summary = "An outer marker on an ER endpoint the edge does not admit.")]
    #[error("Validation error: {message}")]
    OuterEndpoint { message: String },

    /// A transitive chain (&&) cannot carry an outer marker: its merged
    /// join has no one side to keep.
    #[leaf("transitive_outer", class = Syntax, summary = "A transitive chain carried an outer marker.")]
    #[error("Validation error: {message}")]
    TransitiveOuter { message: String },
}
