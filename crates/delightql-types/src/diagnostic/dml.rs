// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `dml/…` — insert!/update!/delete! shape and marker rules.

use super::Taxon;

/// The DML family.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Dml))]
pub enum Dml {
    /// The `!!` mutation marker names the relation a mutation writes.
    /// Members: missing (an update!/delete! whose source marks no relation),
    /// multiple (two marks — a statement mutates one relation), forbidden
    /// (a mark where nothing is mutated, such as an insert! source),
    /// mismatch (the marked relation is not the target the terminal names).
    #[family(
        "marker",
        summary = "The `!!` mutation marker is missing, doubled, misplaced, or mismatched."
    )]
    #[error(transparent)]
    Marker(DmlMarker),

    /// What a mutation must and must not say. Members cover the bounded
    /// mutation, the cover an update! needs, a delete! that covers, the
    /// identity a joined update or a delete needs, and the ambiguous
    /// sources a statement cannot describe.
    #[family(
        "shape",
        summary = "A mutation is missing a required clause or carries a meaningless one."
    )]
    #[error(transparent)]
    Shape(DmlShape),

    /// What may feed a mutation.
    #[family("source", summary = "A mutation's source is not admitted.")]
    #[error(transparent)]
    Source(DmlSource),

    /// What each argument of a DML terminal stands for: the target
    /// relation, the source, and a rule value where one is admitted.
    #[family(
        "roles",
        summary = "A DML terminal's argument stands in the wrong role."
    )]
    #[error(transparent)]
    Roles(DmlRoles),

    /// insert!'s source publishes an unnamed column — a computed value with
    /// no `as` — so no target column can be addressed for it. Name every
    /// published column.
    #[leaf("insert/unnamed_column", class = Syntax, summary = "insert! received an unnamed source column.")]
    #[error("Validation error: {message}")]
    InsertUnnamedColumn { message: String },

    /// The plan compiled an obligation the runtime cannot evaluate on the
    /// routed connection, so the statement it guards may not run.
    #[leaf("plan/unrunnable_obligation", class = Syntax, summary = "A DML obligation cannot be evaluated where its statement runs.")]
    #[error("Validation error: {message}")]
    PlanUnrunnableObligation { message: String },

    /// update! and delete! reach the exact rows the `!!` occurrence
    /// selected through the target's own row identity; this target exposes
    /// none to a statement, so the mutation cannot be lowered for it.
    #[leaf("target/row_identity", class = Syntax, summary = "The target exposes no row identity for update!/delete! to reach the selected rows by.")]
    #[error("Validation error: {message}")]
    TargetRowIdentity { message: String },
}

/// `dml/marker/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Dml, Dml::Marker))]
pub enum DmlMarker {
    /// update! and delete! mutate the relation their source marks with `!!`;
    /// a source with no marked relation says nothing about what changes.
    #[leaf("missing", class = Syntax, summary = "No relation carries the `!!` mutation marker.")]
    #[error("Validation error: {message}")]
    Missing { message: String },

    /// One statement mutates one relation. Two `!!` marks make two claims.
    #[leaf("multiple", class = Syntax, summary = "More than one relation carries the `!!` marker.")]
    #[error("Validation error: {message}")]
    Multiple { message: String },

    /// insert! reads its source and writes the target; nothing in the source
    /// is mutated, so a `!!` mark there has no meaning.
    #[leaf("forbidden", class = Syntax, summary = "A `!!` marker stands where nothing is mutated.")]
    #[error("Validation error: {message}")]
    Forbidden { message: String },

    /// The relation marked `!!` is not the relation the terminal names as
    /// its target.
    #[leaf("mismatch", class = Syntax, summary = "The marked relation is not the terminal's target.")]
    #[error("Validation error: {message}")]
    Mismatch { message: String },
}

/// `dml/shape/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Dml, Dml::Shape))]
pub enum DmlShape {
    /// A row bound (`#<N`) on a mutation's source would make the set of
    /// rows changed depend on an unstated order. Filter the source instead.
    #[leaf("bounded_mutation", class = Syntax, summary = "A mutation's source is row-bounded.")]
    #[error("Validation error: {message}")]
    BoundedMutation { message: String },

    /// delete! removes rows; a cover (projection) on its source would
    /// describe values nothing receives.
    #[leaf("delete_with_cover", class = Syntax, summary = "delete! carries a cover it cannot use.")]
    #[error("Validation error: {message}")]
    DeleteWithCover { message: String },

    /// One statement ends in one DML terminal.
    #[leaf("multi_terminal", class = Syntax, summary = "More than one DML terminal in one statement.")]
    #[error("Validation error: {message}")]
    MultiTerminal { message: String },

    /// The update's cover assigns one target column from more than one
    /// source column.
    #[leaf("update_ambiguous_assignment", class = Syntax, summary = "update! assigns a column twice.")]
    #[error("Validation error: {message}")]
    UpdateAmbiguousAssignment { message: String },

    /// The source offers more than one row for a row of the relation being
    /// updated, so the statement does not say what that row becomes. Two
    /// rows that agree are still two rows — agreement is not evidence of
    /// one row. Narrow the source so each row of the target is described
    /// once.
    #[leaf("update_ambiguous_source", class = Permission, summary = "update!'s source describes a target row more than once.")]
    #[error("Validation error: {message}")]
    UpdateAmbiguousSource { message: String },

    /// update! changes columns; without a cover naming what changes there
    /// is nothing to write.
    #[leaf("update_no_cover", class = Syntax, summary = "update! has no cover to write.")]
    #[error("Validation error: {message}")]
    UpdateNoCover { message: String },

    /// The update's cover transforms nothing: every assignment writes a
    /// column back to itself.
    #[leaf("update_no_transform", class = Syntax, summary = "update! changes nothing.")]
    #[error("Validation error: {message}")]
    UpdateNoTransform { message: String },

    /// The update's cover publishes an unnamed column, so no target column
    /// can be addressed for it. Name every assigned value.
    #[leaf("update_unnamed_column", class = Syntax, summary = "update! received an unnamed cover column.")]
    #[error("Validation error: {message}")]
    UpdateUnnamedColumn { message: String },

    /// The incoming heading of update! is not the target's heading: every
    /// column of the target, by name, once, and nothing else.
    #[leaf("update_heading", class = Syntax, summary = "update!'s incoming heading is not the target's heading.")]
    #[error("Validation error: {message}")]
    UpdateHeading { message: String },
}

/// `dml/source/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Dml, Dml::Source))]
pub enum DmlSource {
    /// A mutation's source must publish rows of the target's own grain; an
    /// aggregate collapses that grain.
    #[leaf("aggregate", class = Syntax, summary = "A mutation's source aggregates.")]
    #[error("Validation error: {message}")]
    Aggregate { message: String },

    /// The rows a mutation touches are the `!!` occurrence's own rows,
    /// reached by their identity. A form standing between the occurrence
    /// and the terminal that does not preserve which rows are the
    /// occurrence's — a set operation, a witness, a reflection — leaves the
    /// terminal nothing to reach them by.
    #[leaf("occurrence", class = Syntax, summary = "The `!!` occurrence's rows do not reach the mutation terminal.")]
    #[error("Validation error: {message}")]
    Occurrence { message: String },
}

/// `dml/roles/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Dml, Dml::Roles))]
pub enum DmlRoles {
    /// The terminal's target position must name a mutable relation.
    #[leaf("target", class = Syntax, summary = "A DML target is not a mutable relation.")]
    #[error("Validation error: {message}")]
    Target { message: String },

    /// The terminal's source position must be a relation.
    #[leaf("source", class = Syntax, summary = "A DML source is not a relation.")]
    #[error("Validation error: {message}")]
    Source { message: String },

    /// A rule value stood where a DML terminal admits none.
    #[leaf("rule_value", class = Syntax, summary = "A rule value stood in a DML argument position.")]
    #[error("Validation error: {message}")]
    RuleValue { message: String },
}
