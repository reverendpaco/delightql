// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Resolution logic using the entity registry

use super::entity::*;
use super::registry::EntityRegistry;
use crate::enums::EntityType as BootstrapEntityType;
use delightql_types::error::DelightQLError;
use delightql_types::SqlIdentifier;

/// Result of resolving an entity
#[derive(Debug)]
pub enum ResolutionResult {
    /// Known built-in function
    #[allow(dead_code)]
    BuiltInFunction {
        name: SqlIdentifier,
        is_aggregate: bool,
    },
    /// Database entity (table, view, etc.)
    DatabaseEntity(EntityInfo),
    /// Query-local CTE
    CTE(EntityInfo),
    /// Consulted view — needs body expansion at the relation level
    ConsultedView {
        name: SqlIdentifier,
        body_source: String,
        namespace: String,
    },
    /// Consulted fact — needs multi-clause expansion at the relation level
    ConsultedFact {
        name: SqlIdentifier,
        body_source: String,
    },
    /// Unknown entity - will be passed through
    Unknown(String),
}

/// Resolve an entity name using the registry with optional alias tracking.
///
/// `resolution_namespace` overrides the default "main" scope for unqualified
/// entity lookup. Used during DDL view body resolution so that DDL-local
/// engages are visible without polluting main scope.
pub fn resolve_entity_with_alias(
    name: &str,
    alias: Option<&str>,
    registry: &mut EntityRegistry,
    resolution_namespace: Option<&str>,
) -> ResolutionResult {
    // Check if this name is actually an alias
    let actual_name = if let Some(target) = registry.query_local.resolve_alias(name) {
        target.to_string()
    } else {
        name.to_string()
    };

    // Query-local CTEs
    if let Some(cte_schema) = registry.query_local.lookup_cte(&actual_name) {
        // Clone the schema immediately to avoid borrow issues
        let cte_schema_clone = cte_schema.clone();

        // If we're accessing this CTE with an alias, track it
        if let Some(alias_name) = alias {
            if alias_name != actual_name {
                registry
                    .query_local
                    .register_alias(alias_name.to_string(), actual_name.clone());
            }
        }

        return ResolutionResult::CTE(EntityInfo {
            name: actual_name.clone().into(),
            canonical_name: None, // CTEs don't have canonical names from bootstrap
            resolved_namespace: None,
            backend_schema: None,
            entity_type: ResolvedEntityKind::Relation,
            registry_source: RegistrySource::QueryLocal,
            schema_source: SchemaSource::SelectClause,
            definition: EntityDefinition::RelationSchema(cte_schema_clone),
        });
    }

    // Level 3: Built-in functions
    if registry.built_in.is_known_function(&actual_name) {
        return ResolutionResult::BuiltInFunction {
            name: actual_name.clone().into(),
            is_aggregate: registry.built_in.is_aggregate(&actual_name),
        };
    }

    // Level 4: Database entities
    // Use namespace-aware resolution via the system.
    // STRICT definition independence (Phase 7 stage 2, owner-ratified):
    // inside a definition the search is scoped to the owning namespace +
    // its own edges — NEVER the caller's session. At the prompt (None)
    // the scope is `home`. The old retry-against-the-session fallback
    // was the caller-leak: a file's rule could resolve a bare name
    // through whatever the CALLER happened to have enlisted.
    let ns = resolution_namespace.unwrap_or("home");
    if let Some(system) = registry.database.system {
        let result = system.resolve_unqualified_entity(&actual_name, ns, None);
        // AMBIENT DATA (Phase 7 stage 2, owner-ratified): a definition's
        // strict miss may still be a physical DATA table in the session's
        // `home` scope — the database is one shared world, not an import.
        // The retry reuses the FULL home-scope resolver (so the ambiguity
        // refusal and its diagnostics are identical to the prompt's), and
        // the RESULT is gated by namespace kind: only `data`-kind hits
        // pass. Another file's DEFINITIONS (lib-kind) never leak through
        // this door.
        let result = match (&result, resolution_namespace.is_some()) {
            (Ok(None), true) => {
                match system.resolve_unqualified_entity(&actual_name, "home", None) {
                    Ok(Some((path, name))) => {
                        let fq = path
                            .iter()
                            .map(|i| i.name.as_str())
                            .collect::<Vec<_>>()
                            .join("::");
                        if system.namespace_is_data_kind(&fq) {
                            Ok(Some((path, name)))
                        } else {
                            Ok(None)
                        }
                    }
                    other => other,
                }
            }
            _ => result,
        };
        match result {
            Ok(Some((namespace_path, canonical_name))) => {
                // Entity found in accessible namespace - use namespace-qualified lookup
                let core_namespace_path =
                    crate::pipeline::ast_resolved::NamespacePath::from_types_namespace_path(
                        &namespace_path,
                    );
                match registry
                    .database
                    .lookup_table_with_namespace(&core_namespace_path, &actual_name)
                {
                    Ok(Some((
                        table_schema,
                        connection_id,
                        _registry_canonical,
                        backend_schema_opt,
                    ))) => {
                        // Track connection_id for cross-connection join validation
                        registry.track_connection_id(connection_id);
                        return ResolutionResult::DatabaseEntity(EntityInfo {
                            name: actual_name.clone().into(),
                            canonical_name: Some(canonical_name),
                            resolved_namespace: Some(core_namespace_path.clone()),
                            backend_schema: backend_schema_opt,
                            entity_type: ResolvedEntityKind::Relation,
                            registry_source: RegistrySource::Database,
                            schema_source: SchemaSource::DatabaseCatalog,
                            definition: EntityDefinition::RelationSchema(table_schema),
                        });
                    }
                    Ok(None) | Err(_) => {
                        // Not a database table (or namespace has no database backend,
                        // e.g. pure-DQL namespaces like std::prelude) — check consult registry.
                        let fq: String = core_namespace_path
                            .items()
                            .iter()
                            .map(|i| i.name.as_str())
                            .collect::<Vec<_>>()
                            .join("::");
                        if let Some(entity) = registry
                            .consult
                            .lookup_entity(&actual_name, &fq, resolution_namespace) {
                            if entity.entity_type
                                == BootstrapEntityType::DqlTemporaryViewExpression
                            {
                                return ResolutionResult::ConsultedView {
                                    name: entity.name.clone(),
                                    body_source: entity.definition.clone(),
                                    namespace: fq.clone(),
                                };
                            }
                            if entity.entity_type == BootstrapEntityType::DqlFactExpression
                            {
                                return ResolutionResult::ConsultedFact {
                                    name: entity.name.clone(),
                                    body_source: entity.definition.clone(),
                                };
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                // Entity not in any accessible namespace.
                // When inside a view body (resolution_namespace is set), also try
                // direct database lookup for DNS tables — the view body should be
                // able to reference tables in the underlying database.
                // If non-authoritative (WASM, pipe connections), also fall back.
                if resolution_namespace.is_some() || !system.namespace_authoritative {
                    if let Some(table_schema) = registry.database.lookup_table(&actual_name) {
                        return ResolutionResult::DatabaseEntity(EntityInfo {
                            name: actual_name.clone().into(),
                            canonical_name: None, // No canonical name available in fallback path
                            resolved_namespace: None,
                            backend_schema: None,
                            entity_type: ResolvedEntityKind::Relation,
                            registry_source: RegistrySource::Database,
                            schema_source: SchemaSource::DatabaseCatalog,
                            definition: EntityDefinition::RelationSchema(table_schema),
                        });
                    }
                }
            }
            Err(e) => {
                // Propagate ambiguity errors so the user gets a clear message.
                // Other database errors fall through to Unknown.
                if let DelightQLError::ValidationError { ref message, .. } = e {
                    if message.contains("Ambiguous entity") {
                        // Pass the raw message (not Display-formatted) to avoid
                        // double "Validation error:" prefix when re-wrapped.
                        return ResolutionResult::Unknown(message.clone());
                    }
                }
            }
        }
    } else {
        // No system available - fall back to old behavior for backward compatibility
        // This happens in tests or when namespace resolution isn't set up
        if let Some(table_schema) = registry.database.lookup_table(&actual_name) {
            return ResolutionResult::DatabaseEntity(EntityInfo {
                name: actual_name.clone().into(),
                canonical_name: None, // No system, no canonical name
                resolved_namespace: None,
                backend_schema: None,
                entity_type: ResolvedEntityKind::Relation,
                registry_source: RegistrySource::Database,
                schema_source: SchemaSource::DatabaseCatalog,
                definition: EntityDefinition::RelationSchema(table_schema),
            });
        }
    }

    // Level 5: Unknown - passthrough
    ResolutionResult::Unknown(actual_name)
}
