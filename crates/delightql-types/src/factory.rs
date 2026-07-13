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
    /// The engine SCHEMA this mount binds — the per-mount recorded fact
    /// behind durable placement and cross-schema queries
    /// (EFFECTS-ON-TARGETS-PLAN §4.1, schema-mount Phase A). `None` means
    /// "the engine's own default" (postgres `public`, duckdb `main`),
    /// resolved DOWNSTREAM at the lookup — never spelled at record time, so
    /// a bare `mount!` keeps its unqualified reads (behavior-identical to
    /// the pre-Phase-A derivation). A specific schema (Phase B's `#schema`
    /// fragment / Phase C's `mount_tree!`) travels here → the cartridge's
    /// `source_ns` → the namespace-keyed schema lookup. SQLite mounts leave
    /// it `None` (no schema concept, R-S5).
    pub mounted_schema: Option<String>,
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

    /// Enumerate the target's PERSISTENT schemas (EFFECTS-ON-TARGETS-PLAN
    /// §4.3 / R-S2) and produce one `ConnectionComponents` per schema, ALL
    /// backed by ONE underlying connection (one child / one relay) so that
    /// `mount_tree!`'s sub-namespaces share a connection_id (R-S1: a
    /// cross-schema `run!` is one bracket). Each pair is
    /// `(schema_name, components)` with `components.mounted_schema =
    /// Some(schema)` and a matching resource identity (so the bootstrap
    /// `connection` dedup folds them onto one row). The default REFUSES —
    /// only a schema-bearing target factory (the CLI's fatboy path)
    /// implements it; SQLite/siso targets refuse (R-S5).
    fn create_tree(
        &self,
        uri: &str,
    ) -> std::result::Result<Vec<(String, ConnectionComponents)>, Box<dyn std::error::Error + Send + Sync>>
    {
        let _ = uri;
        Err("mount_tree! is not supported by this connection factory".into())
    }
}
