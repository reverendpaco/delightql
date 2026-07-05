// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Connection Factory
//!
//! Defines the trait for creating database connections from URIs.
//! The CLI implements this trait — core defines it but never implements it.

use crate::db_traits::DatabaseConnection;
use crate::introspect::DatabaseIntrospector;
use crate::schema::DatabaseSchema;
use std::sync::{Arc, Mutex};

/// Components produced by connecting to a database via URI.
pub struct ConnectionComponents {
    /// Connection for query execution
    pub connection: Arc<Mutex<dyn DatabaseConnection>>,
    /// Schema provider for column lookups
    pub schema: Box<dyn DatabaseSchema>,
    /// Entity introspector for discovery
    pub introspector: Box<dyn DatabaseIntrospector>,
    /// Database type string (for bootstrap metadata)
    pub db_type: String,
    /// How DelightQL reaches the resource: "in-process" | "fatboy" | "siso"
    /// (URI-DESIGN.md §4 — resource and mechanism are orthogonal facts).
    pub mechanism: String,
    /// What the resource asserts about itself, obtained at connect —
    /// method-prefixed so comparisons never cross tiers:
    /// `pg-system-id:<cluster id>`, `realpath:<canonical path>`. None when
    /// the resource asserts nothing reachable (`:memory:`, siso pipes).
    pub identity: Option<String>,
}

/// Factory that creates database connections from URIs.
///
/// The CLI implements this — it knows about file paths, pipe:// URIs,
/// DuckDB files, etc. Core defines the trait but never implements it.
pub trait ConnectionFactory: Send + Sync {
    fn create(
        &self,
        uri: &str,
    ) -> std::result::Result<ConnectionComponents, Box<dyn std::error::Error + Send + Sync>>;
}
