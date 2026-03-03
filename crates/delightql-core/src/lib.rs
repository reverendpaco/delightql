pub mod api;
pub mod session_cwd;
pub(crate) mod bin_cartridge;
pub(crate) mod ddl;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod ddl_pipeline;
pub(crate) mod enums;
pub(crate) mod lispy;
pub(crate) mod namespace;
pub(crate) mod pipeline;
pub(crate) mod resolution;
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

// Re-export derive macros (crate-internal only — used by #[derive] on AST types)
pub(crate) use delightql_macros::PhaseConvert;
pub(crate) use delightql_macros::ToLispy;
