//! Bin Cartridge SPI (Service Provider Interface)
//!
//! This module defines the trait system for built-in (bin) cartridges - entities
//! that are compiled into the DelightQL engine itself rather than loaded from external sources.
//!
//! ## Overview
//!
//! Bin cartridges provide a mechanism for registering native Rust implementations
//! of DelightQL entities (functions, pseudo-predicates, etc.) into the bootstrap
//! metadata system. This allows them to be discovered, introspected, and executed
//! just like entities from other sources (databases, files).
//!
//! ## Architecture
//!
//! ```
//! BinCartridge (trait)
//!   └── provides metadata + entities
//!       ├── PreludeCartridge (pseudo-predicates)
//!       ├── StdStringCartridge (string functions)
//!       └── ... (other stdlib cartridges)
//!
//! BinEntity (trait)
//!   └── base trait for all bin entities
//!       ├── name, type, signature
//!       └── EffectExecutable (for pseudo-predicates)
//!           └── execute() method for Phase 1.X
//! ```
//!
//! ## Example: Pseudo-Predicate
//!
//! ```rust
//! struct MountPredicate;
//!
//! impl BinEntity for MountPredicate {
//!     fn name(&self) -> &str { "mount!" }
//!     fn entity_type(&self) -> EntityType { EntityType::BinPseudoPredicate }
//!     fn signature(&self) -> EntitySignature { /* ... */ }
//! }
//!
//! impl EffectExecutable for MountPredicate {
//!     fn execute(&self, args: &[...], alias: Option<String>, system: &mut DelightQLSystem) -> Result<EntityResult> {
//!         // Open database, register namespace, etc.
//!     }
//! }
//! ```
//!
//! See: documentation/design/pseudo_predicates/BIN-CARTRIDGE-SPI.md

pub mod predicates;
pub mod prelude;
pub mod registry;

use crate::enums::{EntityType, Language};
use crate::error::Result;
use crate::pipeline::asts::unresolved::{DomainExpression, Relation};
use crate::system::DelightQLSystem;
use std::sync::Arc;

// =============================================================================
// Core Traits
// =============================================================================

/// Metadata for a bin cartridge
///
/// Provides information needed to register the cartridge in the bootstrap database.
#[derive(Clone, Debug)]
pub struct BinCartridgeMetadata {
    /// Source URI (e.g., "bootstrap://prelude", "std://string")
    pub source_uri: String,

    /// Namespace path where entities should be activated
    /// (e.g., "std::prelude", "std::string")
    pub namespace_path: String,

    /// Whether this cartridge is universal (available without explicit borrow)
    pub is_universal: bool,

    /// Language for this cartridge (always Rust for bin cartridges in practice)
    pub language: Language,

    /// Optional: Human-readable description
    pub _description: Option<String>,
}

/// Bin Cartridge - A collection of built-in entities
///
/// Cartridges are registered during system initialization and provide
/// entities that are compiled into the DelightQL engine.
#[allow(dead_code)]
pub trait BinCartridge: Send + Sync {
    /// Get cartridge metadata
    ///
    /// Called during registration to insert cartridge record into bootstrap.
    fn metadata(&self) -> BinCartridgeMetadata;

    /// Get all entities provided by this cartridge
    ///
    /// Called during registration to insert entity records into bootstrap.
    /// Returns Arc so entities can be shared between registry and bootstrap sync.
    fn entities(&self) -> Vec<Arc<dyn BinEntity>>;

    /// Lifecycle hook: Called after cartridge is registered in bootstrap
    ///
    /// Use this to perform any initialization that requires the system
    /// to be fully bootstrapped (e.g., registering callback hooks).
    ///
    /// Default implementation does nothing.
    fn on_registered(&self, _system: &DelightQLSystem) -> Result<()> {
        Ok(())
    }

    /// Lifecycle hook: Called during system shutdown
    ///
    /// Use this to clean up any resources (e.g., close connections, free memory).
    ///
    /// Default implementation does nothing.
    fn on_shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// Parameter in an entity signature
#[derive(Clone, Debug)]
pub struct Parameter {
    /// Parameter name (e.g., "db_path", "namespace")
    pub name: String,

    /// Data type (e.g., "String", "Int", "Bool")
    pub data_type: String,

    /// Whether this parameter is optional
    pub _is_optional: bool,
}

/// Output schema for an entity
#[derive(Clone, Debug)]
pub enum OutputSchema {
    /// Entity returns a relation with columns
    Relation(Vec<(String, String)>), // (column_name, data_type)

    /// Entity doesn't return a value (used for sigma predicates with EXISTS semantics)
    /// Sigma predicates are always used with + prefix and never contribute rows
    Void,
}

/// Entity signature - parameters and return type
#[derive(Clone, Debug)]
pub struct EntitySignature {
    /// Input parameters
    pub parameters: Vec<Parameter>,

    /// Output schema
    pub output_schema: OutputSchema,
}

/// Bin Entity - Base trait for all built-in entities
///
/// Provides metadata about the entity (name, type, signature) without
/// defining how it executes. Execution semantics are defined by extension
/// traits (e.g., EffectExecutable for pseudo-predicates).
#[allow(dead_code)]
pub trait BinEntity: Send + Sync {
    /// Entity name (e.g., "mount!", "concat", "upper")
    fn name(&self) -> &str;

    /// Entity type classification
    fn entity_type(&self) -> EntityType;

    /// Entity signature (parameters + return type)
    fn signature(&self) -> EntitySignature;

    /// Whether this entity has side effects
    ///
    /// Side-effecting entities (pseudo-predicates) must be executed during
    /// Phase 1.X (Effect Executor) rather than deferred to later phases.
    fn has_side_effects(&self) -> bool {
        false // Most entities don't have side effects
    }

    /// Get this entity as an EffectExecutable trait object (if applicable)
    ///
    /// Returns Some if this entity implements EffectExecutable (i.e., can be executed
    /// at Phase 1.X). Returns None otherwise.
    ///
    /// Default implementation returns None. Override this for executable entities.
    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        None
    }

    /// Get this entity as an SqlGeneratable trait object (if applicable)
    ///
    /// Returns Some if this entity implements SqlGeneratable (i.e., can generate
    /// SQL directly at Phase 5). Returns None otherwise.
    ///
    /// Default implementation returns None. Override this for sigma predicates and functions.
    fn as_sql_generatable(&self) -> Option<&dyn SqlGeneratable> {
        None
    }
}

// =============================================================================
// Effect Execution (Phase 1.X)
// =============================================================================

/// Result from executing an entity
pub enum EntityResult {
    /// Entity returned a relation
    Relation(Relation),
}

/// Effect Executable - Entities that execute at Phase 1.X
///
/// Pseudo-predicates implement this trait to provide their execution logic.
/// The effect executor calls `execute()` when it encounters the pseudo-predicate
/// in the unresolved AST.
pub trait EffectExecutable: BinEntity {
    /// Execute the entity with the given arguments
    ///
    /// # Arguments
    ///
    /// * `arguments` - Argument expressions (literals in MVP, complex expressions in future)
    /// * `alias` - Optional alias for the result (enables dependency chains)
    /// * `system` - Mutable reference to DelightQL system (for side effects)
    ///
    /// # Returns
    ///
    /// * `Ok(EntityResult)` - Execution succeeded, returns result
    /// * `Err(...)` - Execution failed (fatal error, query stops)
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult>;
}

// =============================================================================
// SQL Generation (Phase 5 - Generator)
// =============================================================================

use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::DomainExpression as SqlDomainExpression;

/// Generator context provided to SQL generatable entities
pub struct GeneratorContext<'a> {
    /// SQL dialect being generated
    pub _dialect: SqlDialect,

    /// Function to render a SQL AST expression to a string
    /// This is provided by the transformer so entities can generate proper SQL
    pub render_expr: &'a dyn Fn(&SqlDomainExpression) -> String,
}

/// SQL Generatable - Entities that can generate SQL directly
///
/// Sigma predicates and functions implement this trait to generate
/// dialect-specific SQL strings during Phase 5 (SQL generator).
///
/// The entity has complete control over SQL generation - the transformer
/// does NOT interpret or map anything. It just asks the entity to generate SQL.
pub trait SqlGeneratable: BinEntity {
    /// Generate SQL for this entity with the given arguments
    ///
    /// # Arguments
    ///
    /// * `args` - SQL AST expressions for arguments (already transformed)
    /// * `context` - Generator context (dialect, etc.)
    /// * `negated` - Whether this is a negated call (\+ prefix)
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - Generated SQL expression
    /// * `Err(...)` - Generation failed (e.g., unsupported dialect, invalid args)
    ///
    /// # Example
    ///
    /// ```
    /// // For like(email, "%@gmail.com"):
    /// // Returns: "email LIKE '%@gmail.com'"
    ///
    /// // For between(age, 18, 65):
    /// // Returns: "age BETWEEN 18 AND 65"
    /// ```
    fn generate_sql<'a>(
        &self,
        args: &[SqlDomainExpression],
        context: &GeneratorContext<'a>,
        negated: bool,
    ) -> Result<String>;
}
