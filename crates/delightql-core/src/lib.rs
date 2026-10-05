// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
pub mod api;
pub(crate) mod bin_cartridge;
pub(crate) mod compiler_limits;
pub(crate) mod creation_target;
pub(crate) mod ddl;
pub(crate) mod ddl_pipeline;
pub(crate) mod definition_catalog;
pub(crate) mod defuse;
pub mod diagnostics;
pub(crate) mod enums;
pub(crate) mod external_effects;
pub(crate) mod host;
pub(crate) mod lispy;
pub(crate) mod names;
pub(crate) mod namespace;
pub(crate) mod pipeline;
pub(crate) mod refinement_budget;
pub(crate) mod relation;
pub(crate) mod resolution;
pub(crate) mod settings;
pub(crate) mod sexp_formatter;
pub(crate) mod stdlib_manifest;
pub mod term_spec;
pub mod uri_registry;

pub(crate) mod bootstrap;
pub(crate) mod bootstrap_schema;
pub(crate) mod import;

pub(crate) mod open;
pub(crate) mod relay;
pub(crate) mod system_vocabulary;

pub(crate) mod system;

#[cfg(test)]
mod mount_lifecycle_tests;

// Re-export error types from delightql-types (needed at crate root for macros/ergonomics)
pub use delightql_types::diagnostic;
pub use delightql_types::error;
pub use delightql_types::{DelightQLError, Result};

/// Whether `name` is a dialect family the compiler accepts (aliases
/// included: "postgresql" for postgres). The CLI's eager --dialect /
/// DQL_DIALECT validation consults this so flag validation and pipeline
/// behavior cannot drift — a function, not a type re-export, because
/// `pipeline` stays pub(crate) (the decoupling boundary).
pub fn is_known_dialect_family(name: &str) -> bool {
    pipeline::generator::SqlDialect::from_family_name(name).is_some()
}

// Re-export derive macros (crate-internal only — used by #[derive] on AST types)
pub(crate) use delightql_macros::ToLispy;
