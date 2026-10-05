// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The explicit service contract consumed by the shared compiler.
//!
//! Native and browser composition construct different hosts, but parsing
//! through SQL generation receives only this contract. Optional services are
//! properties of that host value; target identity is never a semantic input.

use crate::definition_catalog::DefinitionCatalog;
use crate::diagnostic::Runtime;
use crate::error::{DelightQLError, Result};
use crate::pipeline::aggregate_catalog::AggregateCatalog;
use crate::pipeline::dialect_pack::DialectPack;
use crate::pipeline::generator::SqlDialect;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Capability {
    SessionCatalog,
    NativeSqlite,
    Filesystem,
    ExternalEffects,
}

impl Capability {
    fn name(self) -> &'static str {
        match self {
            Capability::SessionCatalog => "session catalog",
            Capability::NativeSqlite => "native SQLite",
            Capability::Filesystem => "filesystem",
            Capability::ExternalEffects => "external effects",
        }
    }
}

/// Optional services carried by one concrete host value.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HostCapabilities {
    session_catalog: bool,
    native_sqlite: bool,
    filesystem: bool,
    external_effects: bool,
}

pub(crate) struct HostQueryResult {
    pub(crate) columns: Vec<String>,
    pub(crate) declared: Vec<Option<String>>,
    pub(crate) rows: Vec<Vec<delightql_types::DbValue>>,
}

pub(crate) enum CreatedObjectReconciliation {
    Complete,
    Unsupported(String),
}

impl HostCapabilities {
    pub(crate) const fn native() -> Self {
        Self {
            session_catalog: true,
            native_sqlite: true,
            filesystem: true,
            external_effects: true,
        }
    }

    /// The session catalog runs in the module on rusqlite's wasm SQLite; the
    /// user's database is the page's, so no native handle, file or process.
    pub(crate) const fn browser() -> Self {
        Self {
            session_catalog: true,
            native_sqlite: false,
            filesystem: false,
            external_effects: false,
        }
    }

    fn supplies(self, capability: Capability) -> bool {
        match capability {
            Capability::SessionCatalog => self.session_catalog,
            Capability::NativeSqlite => self.native_sqlite,
            Capability::Filesystem => self.filesystem,
            Capability::ExternalEffects => self.external_effects,
        }
    }
}

/// The one compiler-to-host relationship.
///
/// This trait is crate-private: embeddings can provide database/protocol
/// capabilities through the public API, but cannot assemble a compiler with
/// an unreviewed semantic service set. Optional operations either have a real
/// implementation on the value or refuse through [`CompilerHost::require`].
pub(crate) trait CompilerHost {
    fn capabilities(&self) -> HostCapabilities;

    fn supplies(&self, capability: Capability) -> bool {
        self.capabilities().supplies(capability)
    }

    fn require(&self, capability: Capability, operation: &str) -> Result<()> {
        if self.supplies(capability) {
            Ok(())
        } else {
            Err(DelightQLError::from(Runtime::Unsupported {
                message: format!(
                    "{operation} requires the {} capability, which this host does not supply",
                    capability.name()
                ),
            }))
        }
    }

    /// The dialect the host stated at boot; over it, no connection's own.
    fn stated_dialect(&self) -> Result<Option<SqlDialect>>;
    /// Every dialect a statement compiled here could route to: the primary
    /// connection's and each registered connection's, once each.
    fn connection_dialects(&self) -> Result<Vec<SqlDialect>>;
    fn dialect_pack(&self) -> Result<Arc<DialectPack>>;
    /// The `aggregates` targeting table, read by direct SQL on the host's
    /// own substrate into one immutable image.
    fn aggregate_catalog(&self) -> Result<Arc<AggregateCatalog>>;
    /// The `type_classes` targeting table, read by direct SQL on the host's
    /// own substrate into one immutable image.
    fn type_classes(&self) -> Result<Arc<crate::pipeline::type_classes::TypeClasses>>;
    fn definition_catalog(&self, operation: &str) -> Result<Box<dyn DefinitionCatalog + '_>>;
    fn namespace_kind(&self, namespace: &str) -> Result<Option<crate::namespace::NamespaceKind>>;
    fn query_session_catalog(&self, sql: &str) -> Result<HostQueryResult>;
}

/// The exclusive services used at the compiler entrance. The read-only
/// compiler contract above is what flows through compilation and
/// generation.
pub(crate) trait CompilerExecutionHost: CompilerHost {
    fn register_prompt_blocks(
        &mut self,
        blocks: Vec<crate::pipeline::ast_unresolved::InlineDdlSpec>,
    ) -> Result<()>;
    fn set_entity_docs_atomic(
        &mut self,
        entries: &[(String, String)],
    ) -> Result<Vec<(String, String)>>;
    fn observe_effect_plan(
        &mut self,
        plan: &crate::pipeline::compiled_query::TypedEffectPlan,
    ) -> Result<()>;
    fn reconcile_created_objects(
        &mut self,
        objects: &[crate::pipeline::compiled_query::PlanCreatedObject],
    ) -> Result<CreatedObjectReconciliation>;
}
