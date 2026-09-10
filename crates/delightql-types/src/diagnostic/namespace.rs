// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `namespace/…` — namespace-creation policy refusals.

use super::Taxon;

/// The namespace family.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Namespace))]
pub enum Namespace {
    /// Members: reserved (bare system names sys/std/home, sys*/std*
    /// prefixes, `_`-prefixed machinery segments), system_subtree (creation
    /// under sys::/std::). `main` is exempt; under home the prefix rule
    /// relaxes while the `_` reservation stays strict.
    #[family("name", summary = "A namespace name hit the reserved-name guard.")]
    #[error(transparent)]
    Name(NamespaceName),

    /// A plain (unqualified) namespace name that more than one namespace
    /// answers to in the current lexical reach; the message lists them.
    /// Qualify the name.
    #[leaf("plain/ambiguous", class = Syntax, summary = "A plain namespace name is ambiguous.")]
    #[error("Validation error: {message}")]
    PlainAmbiguous { message: String },
}

/// `namespace/name/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Namespace, Namespace::Name))]
pub enum NamespaceName {
    /// USER-facing namespace creation refused the target because it (a) IS
    /// a bare system name (sys/std/home), (b) begins with a reserved system
    /// prefix (sys*/std*, case-insensitive — sysinfo, stdlib, std2,
    /// SYS_foo), or (c) contains a segment beginning `_` (the
    /// _internal/_N_blueprint machinery convention, reserved on ANY segment
    /// EVERYWHERE, including under home). The message names the offending
    /// segment and the rule it hit. Fixes: choose a top-level name not
    /// beginning with sys/std and not equal to a system name; author scratch
    /// under home:: (where the sys*/std* prefix relaxes); never begin a
    /// segment with `_`. `main` is exempt.
    #[leaf("reserved", class = Syntax, summary = "A namespace-creation target used a reserved system name.")]
    #[error("Validation error: {message}")]
    Reserved { message: String },

    /// USER-facing namespace creation refused a target under the sys:: or
    /// std:: subtree — reserved for system machinery. Create your namespace
    /// at the top level, or under home::, instead.
    #[leaf("system_subtree", class = Syntax, summary = "A namespace-creation target nested under sys:: or std::.")]
    #[error("Validation error: {message}")]
    SystemSubtree { message: String },
}
