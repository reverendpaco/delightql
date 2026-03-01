//! Namespace management (REMOVED)
//!
//! This module has been replaced by the meta-circular bootstrap system.
//! The NEW system uses:
//! - `delightql_core::bootstrap` - Bootstrap schema and introspection
//! - `delightql_core::import` - Cartridge, namespace, and activation operations
//! - `delightql_core::system::resolve_namespace_path()` - Resolver using _bootstrap.* tables
//!
//! All old namespace functionality has been migrated to the bootstrap system.
