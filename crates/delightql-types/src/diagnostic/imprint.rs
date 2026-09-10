// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `imprint/…` — the linear lifecycle of imprint!/imprint_replace!.

use super::Taxon;

/// The imprint family.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Imprint))]
pub enum Imprint {
    /// imprint! is linear: the consumed source namespace archives as an
    /// inert {target}::_N_blueprint. Blueprint members refuse operations
    /// that would animate that archive — consult it, enlist it, or imprint
    /// it again.
    #[family(
        "blueprint",
        summary = "An archived blueprint namespace refused animation."
    )]
    #[error(transparent)]
    Blueprint(Blueprint),

    /// Manifest members cover the imprint!'s companion blocks: the
    /// schema/constraint/default sections a persistable namespace requires,
    /// and their agreement with the rules being imprinted.
    #[family("manifest", summary = "An imprint manifest is missing or malformed.")]
    #[error(transparent)]
    Manifest(Manifest),
}

/// `imprint/blueprint/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Imprint, Imprint::Blueprint))]
pub enum Blueprint {
    /// imprint! is linear: it consumed the source namespace into
    /// {target}::_N_blueprint and vacated the original path. The archive
    /// stays VISIBLE through the sys::meta catalog functor
    /// ({blueprint}::(*) still lists its entities) but is INERT — resolving
    /// an entity through it (blueprint.rule, blueprint::sub.rule), enlisting
    /// it, or grounding it is refused, because the archived blueprint and
    /// the live target tables it produced would otherwise drift. Re-consult
    /// the source path to obtain a fresh, live copy.
    #[leaf("inert", class = Syntax, summary = "An archived blueprint namespace is inert.")]
    #[error("Validation error: {message}")]
    Inert { message: String },
}

/// `imprint/manifest/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Imprint, Imprint::Manifest))]
pub enum Manifest {
    /// imprint entity names are interpolated into quoted SQL identifiers;
    /// the declared-table branch routes through the DDL generator, which
    /// does not escape an embedded double quote. Such a name (only
    /// reachable via a triple-quoted DQL literal) is refused at
    /// manifest-read.
    #[leaf("entity_name", class = Syntax, summary = "An imprint entity name contains a double quote.")]
    #[error("Validation error: {message}")]
    EntityName { message: String },

    /// The _internal imprinting() manifest declares each entity's extent;
    /// only "permanent" and "temporary" are valid. A typo (e.g. "temp") is
    /// rejected at manifest-read rather than silently meaning permanent.
    #[leaf("extent", class = Syntax, summary = "An imprinting() extent value is not recognized.")]
    #[error("Validation error: {message}")]
    Extent { message: String },

    /// The _internal imprinting() manifest declares each entity's
    /// materialization; only "table" and "view" are valid. A typo (e.g.
    /// "veiw") is rejected at manifest-read rather than silently falling
    /// through to a table.
    #[leaf("materialization", class = Syntax, summary = "An imprinting() materialization value is not recognized.")]
    #[error("Validation error: {message}")]
    Materialization { message: String },

    /// A companion row's key does not name an entity the manifest declares,
    /// or names one twice.
    #[leaf("companion_key", class = Syntax, summary = "A companion row's key names no declared entity.")]
    #[error("Validation error: {message}")]
    CompanionKey { message: String },

    /// A constraint row names a column the entity does not declare.
    #[leaf("constraint_column", class = Syntax, summary = "A constraint names an undeclared column.")]
    #[error("Validation error: {message}")]
    ConstraintColumn { message: String },

    /// A generated-column declaration names a generation kind the manifest
    /// vocabulary does not admit.
    #[leaf("generated_kind", class = Syntax, summary = "A generated-column kind is not recognized.")]
    #[error("Validation error: {message}")]
    GeneratedKind { message: String },

    /// A table constraint names columns the entity does not declare, or
    /// names none.
    #[leaf("table_constraint_columns", class = Syntax, summary = "A table constraint's columns are undeclared or empty.")]
    #[error("Validation error: {message}")]
    TableConstraintColumns { message: String },

    /// A foreign key's columns and its referenced columns disagree in
    /// count, or reference an entity the manifest does not declare.
    #[leaf("table_foreign_key", class = Syntax, summary = "A foreign key is ill-formed.")]
    #[error("Validation error: {message}")]
    TableForeignKey { message: String },
}
