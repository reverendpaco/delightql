//! DDL definition parsing, building, and analysis.
//!
//! - `body_parser`: Re-parses definition body text into unresolved DQL AST nodes
//! - `ddl_builder`: Builds typed `DdlDefinition` AST from DDL CST
//! - `analyzer`: Extracts entity references for dependency tracking

pub mod analyzer;
pub mod body_parser;
pub mod ddl_builder;
#[cfg(not(target_arch = "wasm32"))]
pub mod manifest;
