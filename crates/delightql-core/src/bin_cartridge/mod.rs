// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
//! ```text
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
//!           └── execute() method for the effect executor
//! ```
//!
//! ## Example: Pseudo-Predicate
//!
//! ```ignore
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
use crate::pipeline::asts::unresolved::{DomainExpression, GroundForm};
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

    /// Whether this cartridge is universal (available without explicit enlist!)
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
    #[allow(dead_code)]
    fn on_registered(&self, _system: &DelightQLSystem) -> Result<()> {
        Ok(())
    }

    /// Lifecycle hook: Called during system shutdown
    ///
    /// Use this to clean up any resources (e.g., close connections, free memory).
    ///
    /// Default implementation does nothing.
    #[allow(dead_code)]
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
pub trait BinEntity: Send + Sync {
    /// LOCAL entity name (e.g., "mount!", "concat", "upper") — never a
    /// namespace-qualified string. Identity is (namespace, name); see
    /// `namespace_override`.
    fn name(&self) -> &str;

    /// Identity namespace override. `None` (the default) means the entity
    /// lives in its cartridge's namespace. An override places the entity
    /// under a DIFFERENT deliberate identity (e.g. `compile` under
    /// `sys::execution` while shipped in the prelude cartridge) — and
    /// thereby outside the cartridge's universal unqualified visibility.
    fn namespace_override(&self) -> Option<&str> {
        None
    }

    /// Entity type classification
    fn entity_type(&self) -> EntityType;

    /// Entity signature (parameters + return type)
    fn signature(&self) -> EntitySignature;

    /// Whether this entity has side effects
    ///
    /// Side-effecting entities (pseudo-predicates) must be executed during
    /// the effect executor rather than deferred to a later stage.
    #[allow(dead_code)]
    fn has_side_effects(&self) -> bool {
        false // Most entities don't have side effects
    }

    /// Get this entity as an EffectExecutable trait object (if applicable)
    ///
    /// Returns Some if this entity implements EffectExecutable (i.e., can be executed
    /// in the effect executor). Returns None otherwise.
    ///
    /// Default implementation returns None. Override this for executable entities.
    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        None
    }

    /// Get this entity as an SqlGeneratable trait object (if applicable)
    ///
    /// Returns Some if this entity implements SqlGeneratable (i.e., can generate
    /// SQL directly in the generator). Returns None otherwise.
    ///
    /// Default implementation returns None. Override this for sigma predicates and functions.
    fn as_sql_generatable(&self) -> Option<&dyn SqlGeneratable> {
        None
    }
}

// =============================================================================
// Effect Execution
// =============================================================================

/// Result from executing an entity
pub enum EntityResult {
    /// Entity returned a relation: the chain HEAD it answers with — a
    /// written-out table for most receipts, a named form where the entity
    /// builds one.
    Relation(GroundForm),
}

/// Effect Executable - Entities that execute in the effect executor
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

    /// D3b (M2 — set-at-a-time application): the entity receives the
    /// LIFTED argument relation WHOLE and executes ONCE with ONE receipt.
    /// A piped relation is one demand of the argumentative functor
    /// (EFFECT-ALGEBRA; higher-order.md's scalar lifting) — never N
    /// separate calls.
    ///
    /// The default delegates a one-row lift to `execute`; EVERY other
    /// shape — empty included — refuses with the same not-yet (pipe is
    /// application: an empty relation reaches the callee once, never a
    /// silent no-op) until the entity opts into true setwise semantics
    /// by overriding. `doc!` is the first override.
    fn execute_lifted(
        &self,
        rows: &[Vec<DomainExpression>],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // Pipe is APPLICATION — an empty relation reaches the callee once,
        // exactly like a
        // three-row one. For a scalar-signature entity, both are lift
        // shapes it cannot yet receive, so both get the SAME not-yet
        // refusal (never a silent no-op, never a different error). A
        // setwise override (doc!, compile) gives them true semantics.
        match rows.len() {
            1 => self.execute(&rows[0], alias, system),
            n => Err(crate::error::DelightQLError::validation_error_categorized(
                "effect/pipe/lifted_not_yet",
                format!(
                    "{} received a {n}-row lifted argument: a piped relation \
                     is ONE set-at-a-time demand (the argumentative functor \
                     receives the whole relation, one receipt — even an empty \
                     one), and that execution is not built yet for this \
                     entity. Pipe a single row, or issue separate statements",
                    self.name()
                ),
                "lifted argument not yet",
            )),
        }
    }
}

// =============================================================================
// SQL Generation (the generator)
// =============================================================================

use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::DomainExpression as SqlDomainExpression;

/// Generator context provided to SQL generatable entities
pub struct GeneratorContext<'a> {
    /// SQL dialect being generated
    pub _dialect: SqlDialect,

    /// Fallible function for rendering a SQL AST expression.
    /// This is provided by the generator so entities can propagate rendering
    /// failures without manufacturing SQL or panicking.
    pub render_expr: &'a dyn Fn(&SqlDomainExpression) -> Result<String>,
}

/// SQL Generatable - Entities that can generate SQL directly
///
/// Sigma predicates and functions implement this trait to generate
/// dialect-specific SQL strings in the SQL generator.
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
