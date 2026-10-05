// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `configuration/…` — what a host stated at boot, refused.

use super::Taxon;

/// A host states its settings once, at `open()`, before any DelightQL state
/// exists. The refusal belongs to neither a query nor one particular client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Configuration))]
pub enum Configuration {
    /// The boot table breaks core's settings contract: a required key was not
    /// stated, a key is not one core declares, or a value is malformed. Every
    /// problem is named in the one refusal, so a host author fixes them at
    /// once rather than one per run.
    #[leaf("boot_table", class = Connection, summary = "The host's boot table does not satisfy core's settings contract.")]
    #[error("the boot table does not satisfy core's settings contract: {problems}")]
    BootTable { problems: String },
}
