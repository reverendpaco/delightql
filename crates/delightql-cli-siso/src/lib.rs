//! DelightQL CLI SISO (Standard In / Standard Out) Backend
//!
//! Provides pipe-based database connections that communicate with
//! external CLI tools (osqueryi, sqlite3, etc.) via stdin/stdout.
//!
//! The coprocess is kept alive for the duration of the connection,
//! and queries are framed using UUID-based sentinels to delimit
//! individual query results within the continuous output stream.

pub mod coprocess;
pub mod connection;
pub mod error;
pub mod introspect;
pub mod parse;
pub mod profile;
pub mod schema;

use std::sync::Arc;

use coprocess::{Coprocess, SharedCoprocess};
use connection::PipeConnection;
use error::{PipeError, Result};
use introspect::PipeIntrospector;
use profile::PipeProfile;
use schema::PipeSchema;

/// Top-level pipe connection manager.
///
/// Owns a single shared coprocess that all consumers (schema,
/// introspector, connection, query executor) share via Arc.
pub struct PipeConnectionManager {
    shared: Arc<SharedCoprocess>,
    target: Option<String>,
}

impl PipeConnectionManager {
    /// Create a new pipe connection manager from a URI.
    ///
    /// URI format: `pipe://profile_name` or `pipe://profile_name/target`
    ///
    /// Eagerly spawns the coprocess at construction time.
    pub fn from_uri(uri: &str) -> Result<Self> {
        let rest = uri
            .strip_prefix("pipe://")
            .ok_or_else(|| PipeError::QueryFailed(format!("Invalid pipe URI: {}", uri)))?;

        let (profile_name, target) = match rest.split_once('/') {
            Some((name, target)) => (name, Some(target.to_string())),
            None => (rest, None),
        };

        let profile = profile::resolve_profile(profile_name).ok_or_else(|| {
            PipeError::QueryFailed(format!("Unknown pipe profile: {}", profile_name))
        })?;

        let coprocess = Coprocess::spawn(&profile, target.as_deref())?;
        let shared = Arc::new(SharedCoprocess::new(coprocess, profile));

        Ok(Self { shared, target })
    }

    /// Get the profile name.
    pub fn profile_name(&self) -> &str {
        &self.shared.profile().name
    }

    /// Get the target (e.g. database path).
    pub fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }

    /// Get a PipeConnection backed by the shared coprocess.
    pub fn connect(&self) -> Result<PipeConnection> {
        Ok(PipeConnection::new(self.shared.clone()))
    }

    /// Get a PipeSchema provider backed by the shared coprocess.
    pub fn schema(&self) -> Result<PipeSchema> {
        Ok(PipeSchema::new(self.shared.clone()))
    }

    /// Get a PipeIntrospector backed by the shared coprocess.
    pub fn introspector(&self) -> Result<PipeIntrospector> {
        Ok(PipeIntrospector::new(self.shared.clone()))
    }

    /// Get a reference to the profile.
    pub fn profile(&self) -> &PipeProfile {
        self.shared.profile()
    }

    /// Get the shared coprocess handle.
    pub fn shared(&self) -> &Arc<SharedCoprocess> {
        &self.shared
    }

    /// Execute a raw SQL query through the shared coprocess.
    pub fn execute_query_raw(
        &self,
        sql: &str,
    ) -> std::result::Result<(Vec<String>, Vec<Vec<String>>), PipeError> {
        self.shared.execute_query_raw(sql)
    }
}
