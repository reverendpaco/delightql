// DelightQL Import Module
//
// This module implements the REUSABLE import system for DelightQL.
// It provides generic operations for:
// - Installing cartridges (any source type: file, db, bin)
// - Creating and managing namespaces
// - Activating entities within namespaces
//
// This module is used by:
// - Bootstrap system (installing bootstrap://sys)
// - User imports (mount!, consult!, etc.)
//
// See: documentation/design/ddl/SYS-NS-CARTRIDGE-ER-DESIGN.md

pub mod activation;
pub mod cartridge;
pub mod connection;
pub mod namespace;

// Re-export main types and functions for convenience
pub use activation::{activate_bootstrap_entities, activate_entities_from_cartridge};
pub use cartridge::{install_cartridge, SourceType};
pub use connection::register_connection;
pub use namespace::{create_bootstrap_namespaces, create_namespace_from_path};
