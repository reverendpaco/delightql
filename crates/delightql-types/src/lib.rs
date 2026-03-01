//! DelightQL Shared Types
//!
//! This crate contains shared types used by both delightql-core and delightql-backends,
//! breaking the circular dependency between them.
//!
//! ## Architecture
//!
//! ```text
//! delightql-types (shared types, NO dependencies)
//!     ↓               ↓
//! delightql-core  delightql-backends
//! ```
//!
//! By extracting error types and core traits to this crate:
//! - delightql-core can define AST resolution logic
//! - delightql-backends can implement schema traits without depending on core
//! - delightql-core can then use backends for execution

pub mod db_traits;
pub mod error;
pub mod factory;
pub mod identifier;
pub mod introspect;
pub mod namespace;
pub mod schema;

// Test utilities (mock implementations for testing without real databases)
pub mod test_utils;

// Re-export commonly used types
pub use db_traits::{DatabaseConnection, DatabaseConnectionExt, DbValue, FromDbValue, Row, ToDbValue};
pub use error::{DelightQLError, KnownLimitationType, Result};
pub use identifier::SqlIdentifier;
pub use introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};
pub use namespace::{NamespaceItem, NamespacePath};
pub use factory::{ConnectionComponents, ConnectionFactory};
pub use schema::{ColumnInfo, DatabaseSchema};
