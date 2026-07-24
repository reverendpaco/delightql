// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
pub mod uri_registry;
pub mod api;
pub mod diagnostics;
pub(crate) mod bin_cartridge;
pub(crate) mod ddl;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod ddl_pipeline;
pub(crate) mod enums;
pub(crate) mod lispy;
pub(crate) mod namespace;
pub(crate) mod pipeline;
pub(crate) mod resolution;
pub mod session_cwd;
pub mod term_spec;
pub(crate) mod seed_manifest;
pub(crate) mod sexp_formatter;
pub(crate) mod stdlib_manifest;

// Modules that depend on rusqlite (native only)
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod bootstrap;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod bootstrap_schema;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod import;

// open and relay: available on all targets (relay cfg-gates rusqlite internally)
pub(crate) mod open;
pub(crate) mod relay;

// System module: full version for native, minimal version for WASM
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod system;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm_system;
#[cfg(target_arch = "wasm32")]
pub(crate) use wasm_system as system;

// Re-export error types from delightql-types (needed at crate root for macros/ergonomics)
pub use delightql_types::error;
pub use delightql_types::{DelightQLError, Result};

/// Whether `name` is a dialect family the compiler accepts (aliases
/// included: "postgresql" for postgres). The CLI's eager --dialect /
/// DQL_DIALECT validation consults this so flag validation and pipeline
/// behavior cannot drift — a function, not a type re-export, because
/// `pipeline` stays pub(crate) (the decoupling boundary).
pub fn is_known_dialect_family(name: &str) -> bool {
    pipeline::generator_v3::SqlDialect::from_family_name(name).is_some()
}

// Re-export derive macros (crate-internal only — used by #[derive] on AST types)
pub(crate) use delightql_macros::PhaseConvert;
pub(crate) use delightql_macros::ToLispy;
