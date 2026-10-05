// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `semantic/ddl/…` — definition assembly.

use super::Semantic;
use crate::diagnostic::{DelightQLError, Taxon};

/// `semantic/ddl/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Ddl))]
pub enum Ddl {
    /// The head laws: a head is an ordered projection of its body's heading
    /// (HEADS.md) and a grounding surface. Members refuse heads that
    /// disagree across clauses, mix forms, compute, or conflict with what
    /// the body publishes.
    #[family("head", summary = "A definition head violated a head law.")]
    #[error(transparent)]
    Head(DdlHead),

    /// One namespace holds one entity per name: a functor `foo` and an
    /// effect `foo!`, or a relation and a function of one spelling, are
    /// two entities claiming one name.
    #[leaf("family/one_name_one_entity", class = Syntax, summary = "One name names two entities.")]
    #[error("Validation error: {message}")]
    FamilyOneNameOneEntity { message: String },

    /// The clauses assembled as one group name different subjects.
    #[leaf("group/mixed_subject", class = Syntax, summary = "A clause group names more than one subject.")]
    #[error("Validation error: {message}")]
    GroupMixedSubject { message: String },
}

/// `semantic/ddl/head/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Ddl, Ddl::Head))]
pub enum DdlHead {
    /// The clauses of one definition declare heads of different widths.
    #[leaf("arity", class = Syntax, summary = "A family's clause heads differ in arity.")]
    #[error("Validation error: {message}")]
    Arity { message: String },

    /// A context marker stood in a head position that takes none.
    #[leaf("context_position", class = Syntax, summary = "A context marker stood in a head position.")]
    #[error("Validation error: {message}")]
    ContextPosition { message: String },

    /// A head carried the same context marker twice.
    #[leaf("duplicate_context_marker", class = Syntax, summary = "A head repeated a context marker.")]
    #[error("Validation error: {message}")]
    DuplicateContextMarker { message: String },

    /// A fact table's header row and a rule head cannot share one family:
    /// a header names columns for ground rows, a head projects a body.
    #[leaf("fact_header", class = Syntax, summary = "A fact header stood among rule heads.")]
    #[error("Validation error: {message}")]
    FactHeader { message: String },

    /// The clauses of one definition mix head forms (positional and named,
    /// or open and closed) that cannot assemble into one heading.
    #[leaf("mixed_forms", class = Syntax, summary = "A family's clauses mix head forms.")]
    #[error("Validation error: {message}")]
    MixedForms { message: String },

    /// The clauses of one name declare different definition kinds — a
    /// relation beside a function, a rule beside a fact function.
    #[leaf("mixed_kind", class = Syntax, summary = "A family's clauses declare different kinds.")]
    #[error("Validation error: {message}")]
    MixedKind { message: String },

    /// A head names a column twice.
    #[leaf("name_collision", class = Syntax, summary = "A head names one column twice.")]
    #[error("Validation error: {message}")]
    NameCollision { message: String },

    /// A head names a position differently from the body row that offers
    /// it: the head's name and the row's disagree.
    #[leaf("name_conflict", class = Syntax, summary = "A head's name conflicts with its body's.")]
    #[error("Validation error: {message}")]
    NameConflict { message: String },

    /// The clauses of one parameterized definition declare different
    /// parameter rows.
    #[leaf("param_arity", class = Syntax, summary = "A family's clauses declare different parameter rows.")]
    #[error("Validation error: {message}")]
    ParamArity { message: String },

    /// A fact offer (a ground row) was written under a parameterized head;
    /// facts take no parameters.
    #[leaf("parameterized_fact_offer", class = Syntax, summary = "A fact offer stood under a parameterized head.")]
    #[error("Validation error: {message}")]
    ParameterizedFactOffer { message: String },

    /// A rule's contract — its declared mode or signature — is not one the
    /// definition kind admits.
    #[leaf("rule_contract", class = Syntax, summary = "A rule's declared contract is not admitted.")]
    #[error("Validation error: {message}")]
    RuleContract { message: String },

    /// A guarded clause family may hold at most one unguarded clause, and
    /// it must be last: an unguarded clause is the fallback, and two
    /// fallbacks (or a fallback before a guard) leave the selection
    /// undefined.
    #[leaf("unguarded_multiplicity", class = Syntax, summary = "More than one unguarded clause, or one not last.")]
    #[error("Validation error: {message}")]
    UnguardedMultiplicity { message: String },

    /// The unguarded clause of a guarded family must be the last clause.
    #[leaf("unguarded_position", class = Syntax, summary = "The unguarded clause is not last.")]
    #[error("Validation error: {message}")]
    UnguardedPosition { message: String },

    /// A ground position in a head carries no name, so nothing in the body
    /// can address it.
    #[leaf("unnamed_ground_position", class = Syntax, summary = "A head's ground position has no name.")]
    #[error("Validation error: {message}")]
    UnnamedGroundPosition { message: String },

    /// A head names a column its body does not publish.
    #[leaf("unresolved_reference", class = Syntax, summary = "A head names a column the body does not publish.")]
    #[error("Validation error: {message}")]
    UnresolvedReference { message: String },

    /// A relational or effect higher-order family names a scalar formal at
    /// a position no clause uses: no clause's `$.x` selects it, and no
    /// clause grounds that position. A bare name in the body is a column,
    /// never the formal — `$.x` is the parameter's spelling.
    #[leaf("unused_scalar", class = Syntax, summary = "A family's named scalar formal is used by no clause.")]
    #[error("Validation error: {message}")]
    UnusedScalar { message: String },
}
