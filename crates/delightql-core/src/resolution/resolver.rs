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
    /// A physical relation created by an earlier statement in the same plan.
    ///
    /// Its heading is query-local, but unlike a CTE it may be a DML target.
    MaterializedRelation(EntityInfo),
    /// Consulted definition with a relational body — needs body expansion at
    /// the relation level. Facts take this road too: a fact elaborated into
    /// ordinary relational clauses at assembly, so its stored source expands
    /// through the same one reconstruction door a view's does.
    ConsultedView {
        name: SqlIdentifier,
        body_source: String,
        namespace: String,
    },
    /// A consulted entity whose name resolved, but whose kind cannot occupy
    /// relation position. Relation resolution retains the kind so it can teach
    /// the valid invocation form instead of claiming the name is absent.
    DefinedNonRelation {
        name: SqlIdentifier,
        entity_type: BootstrapEntityType,
    },
    /// A RELATION THE RUNTIME SERVES. Its category is relational — it names a
    /// relation and publishes a heading — but no schema this resolver can
    /// consult holds its rows: they are produced by executing the entity.
    ///
    /// It is kept apart from `DefinedNonRelation` because the two say opposite
    /// things about the same position. Where this one reaches a road that
    /// cannot execute, what is missing is the road, not the category.
    RuntimeServedRelation {
        name: SqlIdentifier,
        entity_type: BootstrapEntityType,
    },
    /// Unknown entity - will be passed through
    Unknown(String),
}

/// Classify one consulted catalog hit for use in relation position.
///
/// This is the kind boundary shared by qualified and unqualified relation
/// lookup. A name that resolves to a non-relational functor remains a resolved
/// name; the relation resolver decides how to explain its invocation form.
pub fn classify_consulted_relation(entity: super::registry::ConsultedEntity) -> ResolutionResult {
    match entity.entity_type {
        BootstrapEntityType::DqlTemporaryViewExpression => ResolutionResult::ConsultedView {
            name: entity.name,
            body_source: entity.definition,
            namespace: entity.namespace,
        },
        // A fact IS a relational definition after elaboration; its catalog
        // kind stays Fact, and its relation-position road is the view's.
        BootstrapEntityType::DqlFactExpression => ResolutionResult::ConsultedView {
            name: entity.name,
            body_source: entity.definition,
            namespace: entity.namespace,
        },
        entity_type @ (BootstrapEntityType::DqlFunctionExpression
        | BootstrapEntityType::DqlHoFunctionExpression
        | BootstrapEntityType::DqlContextAwareFunctionExpression
        | BootstrapEntityType::DqlHoTemporaryViewExpression
        | BootstrapEntityType::DqlTemporarySigmaRule
        | BootstrapEntityType::BinPseudoPredicate
        | BootstrapEntityType::BinSigmaPredicate
        | BootstrapEntityType::DqlErContextRule
        | BootstrapEntityType::DqlEffectRule
        // A syntax-terminal or liminal-only directive's REFLECTED identity:
        // present in the catalog, never occupying relation position — its
        // realization's own contextual policy teaches the invocation form.
        | BootstrapEntityType::SyntaxDirective) => ResolutionResult::DefinedNonRelation {
            name: entity.name,
            entity_type,
        },
        // A bin relation IS a relation; the runtime serves its rows. Naming
        // that category here is what keeps it out of the TVF fallback, which
        // would strip the namespace and generate SQL against a phantom table.
        entity_type @ BootstrapEntityType::BinRelation => ResolutionResult::RuntimeServedRelation {
            name: entity.name,
            entity_type,
        },
        BootstrapEntityType::DqlPermanentViewExpression
        | BootstrapEntityType::DqlTemporaryTableExpression
        | BootstrapEntityType::DqlPermanentTableExpression
        | BootstrapEntityType::DbPermanentTable
        | BootstrapEntityType::DbPermanentView
        | BootstrapEntityType::DbTemporaryTable
        | BootstrapEntityType::DbTemporaryView => {
            ResolutionResult::Unknown(entity.name.to_string())
        }
    }
}

/// Resolve an entity name using the registry with optional alias tracking.
///
/// `resolution_namespace` overrides the default "main" scope for unqualified
/// entity lookup. Used during DDL view body resolution so that DDL-local
/// enlists are visible without polluting main scope.
pub fn resolve_entity_with_alias(
    name: &delightql_types::SqlIdentifier,
    alias: Option<&delightql_types::SqlIdentifier>,
    registry: &mut EntityRegistry,
    resolution_namespace: Option<&str>,
) -> crate::error::Result<ResolutionResult> {
    // Check if this name is actually an alias. Agreement everywhere below
    // is the identifier law's: an unstropped spelling folds, a stropped one
    // keeps its authored bytes.
    let actual_name = registry
        .query_local
        .resolve_alias(name)
        .cloned()
        .unwrap_or_else(|| name.clone());

    // Query-local CTEs
    if let Some(cte_schema) = registry.query_local.lookup_cte(&actual_name) {
        // Clone the schema immediately to avoid borrow issues
        let cte_schema_clone = cte_schema.clone();

        // If we're accessing this CTE with an alias, track it
        if let Some(alias_name) = alias {
            if *alias_name != actual_name {
                registry
                    .query_local
                    .register_alias(alias_name.clone(), actual_name.clone());
            }
        }

        return Ok(ResolutionResult::CTE(EntityInfo {
            name: actual_name.clone(),
            canonical_name: None, // CTEs don't have canonical names from bootstrap
            resolved_namespace: None,
            backend_schema: None,
            entity_type: ResolvedEntityKind::Relation,
            registry_source: RegistrySource::QueryLocal,
            schema_source: SchemaSource::SelectClause,
            definition: EntityDefinition::RelationSchema(cte_schema_clone),
        }));
    }

    // A plan-created table is physical even though its schema is known only
    // from the creating statement. It deliberately follows CTE lookup so a
    // query-local CTE of the same name retains normal lexical shadowing.
    if let Some(schema) = registry
        .query_local
        .lookup_materialized_relation(&actual_name)
        .cloned()
    {
        if let Some(alias_name) = alias {
            if *alias_name != actual_name {
                registry
                    .query_local
                    .register_alias(alias_name.clone(), actual_name.clone());
            }
        }

        return Ok(ResolutionResult::MaterializedRelation(EntityInfo {
            name: actual_name.clone(),
            canonical_name: Some(actual_name.clone()),
            resolved_namespace: None,
            backend_schema: None,
            entity_type: ResolvedEntityKind::Relation,
            registry_source: RegistrySource::QueryLocal,
            schema_source: SchemaSource::SelectClause,
            definition: EntityDefinition::RelationSchema(schema),
        }));
    }

    // Level 3: Built-in functions
    if registry.built_in.is_known_function(actual_name.as_str()) {
        return Ok(ResolutionResult::BuiltInFunction {
            name: actual_name.clone(),
            is_aggregate: registry.built_in.is_aggregate(actual_name.as_str()),
        });
    }

    // Level 4: Database entities
    // Use namespace-aware resolution via the system.
    // STRICT definition independence:
    // inside a definition the search is scoped to the owning namespace +
    // its own edges — NEVER the caller's session. At the prompt (None)
    // the scope is `home`. The old retry-against-the-session fallback
    // was the caller-leak: a file's rule could resolve a bare name
    // through whatever the CALLER happened to have enlisted.
    let ns = resolution_namespace.unwrap_or("home");
    if let Some(system) = registry.database.system {
        let result = system.resolve_unqualified_entity(&actual_name, ns, None);
        // AMBIENT DATA: a definition's
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
                        return Ok(ResolutionResult::DatabaseEntity(EntityInfo {
                            name: actual_name.clone().into(),
                            canonical_name: Some(canonical_name),
                            resolved_namespace: Some(core_namespace_path.clone()),
                            backend_schema: backend_schema_opt,
                            entity_type: ResolvedEntityKind::Relation,
                            registry_source: RegistrySource::Database,
                            schema_source: SchemaSource::DatabaseCatalog,
                            definition: EntityDefinition::RelationSchema(table_schema),
                        }));
                    }
                    Ok(None) => {
                        // Not a database table (or namespace has no database backend,
                        // e.g. pure-DQL namespaces like std::prelude) — check consult registry.
                        let fq: String = core_namespace_path
                            .items()
                            .iter()
                            .map(|i| i.name.as_str())
                            .collect::<Vec<_>>()
                            .join("::");
                        if let Some(entity) = registry.consult.lookup_entity(
                            &actual_name,
                            false,
                            &fq,
                            resolution_namespace,
                        ) {
                            return Ok(classify_consulted_relation(entity));
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(None) => {
                // Entity not in any accessible namespace.
                // When inside a view body (resolution_namespace is set), also try
                // direct database lookup for DNS tables — the view body should be
                // able to reference tables in the underlying database.
                // If non-authoritative (WASM, pipe connections), also fall back.
                if resolution_namespace.is_some() || !system.namespace_authoritative {
                    if let Some(table_schema) = registry.database.lookup_table(&actual_name)? {
                        return Ok(ResolutionResult::DatabaseEntity(EntityInfo {
                            name: actual_name.clone().into(),
                            canonical_name: None, // No canonical name available in fallback path
                            resolved_namespace: None,
                            backend_schema: None,
                            entity_type: ResolvedEntityKind::Relation,
                            registry_source: RegistrySource::Database,
                            schema_source: SchemaSource::DatabaseCatalog,
                            definition: EntityDefinition::RelationSchema(table_schema),
                        }));
                    }
                }
            }
            Err(e) => {
                // Namespace discovery can legitimately decline to answer for
                // non-database definitions (for example, a pure-DQL scope
                // with no connection). Preserve the existing fallback for
                // those errors; provider failures are propagated by the
                // direct DatabaseSchema lookup below.
                if let DelightQLError::ValidationError { ref message, .. } = e {
                    if message.contains("Ambiguous entity") {
                        // Pass the raw message (not Display-formatted) to avoid
                        // double "Validation error:" prefix when re-wrapped.
                        return Ok(ResolutionResult::Unknown(message.clone()));
                    }
                }
            }
        }
    } else {
        // No system, so no namespace to resolve against: the database
        // catalog is the only registry there is. A registry built without a
        // system — as unit tests build one — takes this road.
        if let Some(table_schema) = registry.database.lookup_table(&actual_name)? {
            return Ok(ResolutionResult::DatabaseEntity(EntityInfo {
                name: actual_name.clone().into(),
                canonical_name: None, // No system, no canonical name
                resolved_namespace: None,
                backend_schema: None,
                entity_type: ResolvedEntityKind::Relation,
                registry_source: RegistrySource::Database,
                schema_source: SchemaSource::DatabaseCatalog,
                definition: EntityDefinition::RelationSchema(table_schema),
            }));
        }
    }

    // Level 5: Unknown - passthrough
    Ok(ResolutionResult::Unknown(actual_name.as_str().to_string()))
}
