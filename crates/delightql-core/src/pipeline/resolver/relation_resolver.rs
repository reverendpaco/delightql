//! Relation resolution logic
//!
//! This module handles the resolution of Relation nodes (Ground, Anonymous, TVF)
//! and pattern application for positional patterns.

use super::resolving::domain_expressions::resolve_domain_expr_with_schema;
use super::tvf::get_tvf_schema;
use super::type_conversion::{
    convert_domain_expression, convert_qualified_name, preserve_domain_spec,
};
use super::{BubbledState, JoinContext, PatternResolver, ResolutionConfig};
use crate::enums::EntityType as BootstrapEntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::NamespacePath;
use crate::pipeline::ast_unresolved;
use delightql_types::SqlIdentifier;

/// Helper to apply PatternResolver for column selection
pub(super) fn apply_pattern_resolver(
    domain_spec: &ast_unresolved::DomainSpec,
    base_cols: &[ast_resolved::ColumnMetadata],
    table_name: &str,
    _registry: &crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    // VALIDATE: Positional pattern length must match table columns
    if let ast_unresolved::DomainSpec::Positional(patterns) = domain_spec {
        if patterns.len() != base_cols.len() {
            return Err(DelightQLError::validation_error(
                format!(
                    "Positional pattern incomplete - table '{}' has {} columns but pattern specifies {} elements",
                    table_name, base_cols.len(), patterns.len()
                ),
                "Positional pattern validation".to_string()
            ));
        }
    }

    // GlobWithUsingAll: no per-column validation needed (shared cols computed at join time)
    // — just proceed to pattern resolver which treats it like Glob

    // VALIDATE: GlobWithUsing columns must exist in the table
    if let ast_unresolved::DomainSpec::GlobWithUsing(using_cols) = domain_spec {
        for col_name in using_cols {
            let exists = base_cols
                .iter()
                .any(|c| super::col_name_eq(c.name(), col_name));
            if !exists {
                return Err(DelightQLError::column_not_found_error(
                    col_name.clone(),
                    format!(
                        "USING column '{}' not found in table '{}'",
                        col_name, table_name
                    ),
                ));
            }
        }
    }

    // Use the actual PatternResolver
    let pattern_resolver = PatternResolver::new();

    // Convert outer_context to JoinContext if present
    let join_context = outer_context.map(JoinContext::from);

    match pattern_resolver.resolve_pattern(
        domain_spec,
        base_cols,
        table_name,
        join_context.as_ref(),
    ) {
        Ok(pattern_result) => {
            let output_columns = pattern_result.output_columns;
            let filtered_schema = ast_resolved::CprSchema::Resolved(output_columns.clone());

            // Create the base relation
            let base_relation = ast_resolved::Relation::Ground {
                identifier: ast_resolved::QualifiedName {
                    name: table_name.into(),
                    namespace_path: NamespacePath::empty(),
                    grounding: None,
                },
                canonical_name: ast_resolved::PhaseBox::new(None),
                domain_spec: preserve_domain_spec(domain_spec)?,
                alias: None, // Will be set by caller
                outer: false,
                mutation_target: false,
                passthrough: false,
                cpr_schema: ast_resolved::PhaseBox::new(filtered_schema),
                hygienic_injections: Vec::new(),
            };

            let mut final_expr = ast_resolved::RelationalExpression::Relation(base_relation);

            // Apply WHERE constraints if any literals were found
            if !pattern_result.where_constraints.is_empty() {
                // Combine multiple constraints with AND
                let combined_condition = if pattern_result.where_constraints.len() == 1 {
                    pattern_result
                        .where_constraints
                        .into_iter()
                        .next()
                        .expect("Checked len==1 above")
                } else {
                    // Create AND chain for multiple constraints
                    pattern_result
                        .where_constraints
                        .into_iter()
                        .reduce(|left, right| ast_resolved::BooleanExpression::And {
                            left: Box::new(left),
                            right: Box::new(right),
                        })
                        .expect("Checked non-empty above, reduce must succeed")
                };

                let sigma_condition = ast_resolved::SigmaCondition::Predicate(combined_condition);

                final_expr = ast_resolved::RelationalExpression::Filter {
                    source: Box::new(final_expr),
                    condition: sigma_condition,
                    origin: ast_resolved::FilterOrigin::PositionalLiteral {
                        source_table: table_name.to_string(),
                    },
                    cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                        output_columns.clone(),
                    )),
                };
            }

            // Store join conditions for later application during JOIN operations
            // For positional patterns like (user_id, o.user_id), these become JOIN conditions
            if !pattern_result.join_conditions.is_empty() {
                // For now, log the join conditions - they'll be applied by the JOIN resolver
                // In a full implementation, these would be stored in the resolved relation
                // and retrieved during JOIN processing
                log::debug!(
                    "Storing {} join conditions from positional pattern for table {}",
                    pattern_result.join_conditions.len(),
                    table_name
                );

                // TODO: Integrate with JOIN resolver to apply these conditions
                // The JOIN resolver should check for stored join conditions when processing JOINs
            }

            Ok((final_expr, BubbledState::resolved(output_columns)))
        }
        Err(e) => {
            // If PatternResolver fails, fall back to original behavior
            // Use debug level since fallback is expected for unsupported patterns
            log::debug!(
                "PatternResolver not applicable for table {} ({}), using fallback",
                table_name,
                e
            );
            let schema = ast_resolved::CprSchema::Resolved(base_cols.to_vec());
            let relation = ast_resolved::Relation::Ground {
                identifier: ast_resolved::QualifiedName {
                    name: table_name.into(),
                    namespace_path: NamespacePath::empty(),
                    grounding: None,
                },
                canonical_name: ast_resolved::PhaseBox::new(None),
                domain_spec: preserve_domain_spec(domain_spec)?,
                alias: None,
                outer: false,
                mutation_target: false,
                passthrough: false,
                cpr_schema: ast_resolved::PhaseBox::new(schema),
                hygienic_injections: Vec::new(),
            };
            Ok((
                ast_resolved::RelationalExpression::Relation(relation),
                BubbledState::resolved(base_cols.to_vec()),
            ))
        }
    }
}

#[stacksafe::stacksafe]
pub(super) fn resolve_relation_with_registry(
    rel: ast_unresolved::Relation,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    match &rel {
        ast_unresolved::Relation::Ground { .. } => {
            resolve_ground(rel, registry, outer_context, config, grounding)
        }
        ast_unresolved::Relation::Anonymous { .. } => {
            resolve_anonymous(rel, registry, outer_context)
        }
        ast_unresolved::Relation::TVF { .. } => resolve_tvf(rel, registry, outer_context, config),
        ast_unresolved::Relation::InnerRelation { .. } => {
            resolve_inner_relation(rel, registry, outer_context, config, grounding)
        }

        ast_unresolved::Relation::ConsultedView { .. } => {
            panic!(
                "INTERNAL ERROR: ConsultedView should not appear as input to relation resolution. \
                 ConsultedView is created by the resolver, not consumed at this point."
            )
        }
        ast_unresolved::Relation::PseudoPredicate { .. } => {
            panic!(
                "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                 Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
            )
        }
    }
}

/// Resolve a Ground relation variant (named table, view, CTE, or consulted entity).
///
/// This handles passthrough tables, grounded entities, namespace-qualified tables,
/// unqualified tables, CTEs, consulted views/facts, and unknown entities.
fn resolve_ground(
    rel: ast_unresolved::Relation,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    use crate::resolution::{resolve_entity_with_alias, EntityDefinition, ResolutionResult};

    let ast_unresolved::Relation::Ground {
        identifier,
        canonical_name: _,
        domain_spec,
        alias,
        outer,
        mutation_target: _,
        passthrough,
        cpr_schema: _,
        hygienic_injections: _,
    } = rel
    else {
        unreachable!("resolve_ground called with non-Ground variant");
    };

    // PASSTHROUGH: skip entity catalog, use schema introspector directly.
    // Best-effort: try to get columns from backend, fall back to opaque glob if not found.
    if passthrough {
        if identifier.namespace_path.is_empty() {
            return Err(DelightQLError::validation_error(
                "Passthrough table access requires a namespace path (e.g., main/table_name(*))"
                    .to_string(),
                "passthrough_requires_namespace".to_string(),
            ));
        }

        // Try schema introspector for column info (best-effort)
        let (table_schema, canonical_name, resolved_namespace) = match registry
            .database
            .lookup_table_with_namespace(&identifier.namespace_path, &identifier.name)
        {
            Ok(Some((schema, connection_id, canon))) => {
                registry.track_connection_id(connection_id);
                (
                    Some(schema),
                    Some(canon),
                    Some(identifier.namespace_path.clone()),
                )
            }
            Ok(None) | Err(_) => {
                // Best-effort: table not found in introspector — fall back to opaque
                (None, None, Some(identifier.namespace_path.clone()))
            }
        };

        if let Some(schema) = table_schema {
            // Got columns from backend — resolve normally with pattern resolver
            if let ast_resolved::CprSchema::Resolved(ref base_cols) = schema {
                let table_name_str = alias.as_deref().unwrap_or(&identifier.name);

                // Relabel columns with alias if present
                let relabeled_cols: Vec<ast_resolved::ColumnMetadata> =
                    if let Some(alias_name) = &alias {
                        base_cols
                            .iter()
                            .map(|col| {
                                let mut new_col = col.clone();
                                new_col.fq_table.name =
                                    ast_resolved::TableName::Named(alias_name.clone().into());
                                new_col
                            })
                            .collect()
                    } else {
                        base_cols.clone()
                    };

                let (mut final_expr, state) = apply_pattern_resolver(
                    &domain_spec,
                    &relabeled_cols,
                    table_name_str,
                    registry,
                    outer_context,
                )?;

                // Patch the relation with correct identifier, canonical name, alias
                if let ast_resolved::RelationalExpression::Relation(ref mut r) = final_expr {
                    if let ast_resolved::Relation::Ground {
                        identifier: ref mut rel_id,
                        canonical_name: ref mut rel_canonical,
                        alias: ref mut rel_alias,
                        outer: ref mut rel_outer,
                        passthrough: ref mut rel_passthrough,
                        ..
                    } = r
                    {
                        *rel_id = convert_qualified_name(identifier);
                        if let Some(ref ns) = resolved_namespace {
                            rel_id.namespace_path = ns.clone();
                        }
                        *rel_canonical = ast_resolved::PhaseBox::new(canonical_name);
                        *rel_alias = alias;
                        *rel_outer = outer;
                        *rel_passthrough = true;
                    }
                } else if let ast_resolved::RelationalExpression::Filter {
                    ref mut source, ..
                } = final_expr
                {
                    if let ast_resolved::RelationalExpression::Relation(ref mut r) = source.as_mut()
                    {
                        if let ast_resolved::Relation::Ground {
                            identifier: ref mut rel_id,
                            canonical_name: ref mut rel_canonical,
                            alias: ref mut rel_alias,
                            outer: ref mut rel_outer,
                            passthrough: ref mut rel_passthrough,
                            ..
                        } = r
                        {
                            *rel_id = convert_qualified_name(identifier);
                            if let Some(ref ns) = resolved_namespace {
                                rel_id.namespace_path = ns.clone();
                            }
                            *rel_canonical = ast_resolved::PhaseBox::new(canonical_name);
                            *rel_alias = alias;
                            *rel_outer = outer;
                            *rel_passthrough = true;
                        }
                    }
                }

                return Ok((final_expr, state));
            }
            // Non-resolved schema — treat as opaque fallback below
        }

        // Opaque fallback: no column info available
        // Only glob domain_spec is allowed in opaque mode
        if !matches!(
            domain_spec,
            ast_unresolved::DomainSpec::Glob | ast_unresolved::DomainSpec::Bare
        ) {
            return Err(DelightQLError::validation_error(
                format!(
                    "Passthrough table '{}/{}' schema not available — only (*) is allowed, not positional binding",
                    identifier.namespace_path, identifier.name
                ),
                "passthrough_opaque_glob_only".to_string(),
            ));
        }

        let resolved = ast_resolved::Relation::Ground {
            identifier: convert_qualified_name(identifier),
            canonical_name: ast_resolved::PhaseBox::new(None),
            domain_spec: ast_resolved::DomainSpec::Glob,
            alias,
            outer,
            mutation_target: false,
            passthrough: true,
            cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Unknown),
            hygienic_injections: Vec::new(),
        };
        return Ok((
            ast_resolved::RelationalExpression::Relation(resolved),
            BubbledState::resolved(vec![]),
        ));
    }

    // Check for namespace-qualified tables FIRST
    // Bypass resolve_entity_with_alias for namespace-qualified tables
    // CTEs can't have namespace paths (they're query-local), so this is safe
    let resolution = if let Some(ref grounding) = identifier.grounding {
        // F^S.e(*) — only entities in S are visible. Never look in F.
        let mut found_entity: Option<(String, i32)> = None;
        for ns in &grounding.grounded_ns {
            let fq = super::grounding::namespace_path_to_fq(ns);
            if let Some(entity) = registry.consult.lookup_entity(&identifier.name, &fq) {
                if entity.entity_type == BootstrapEntityType::DqlTemporaryViewExpression.as_i32()
                    || entity.entity_type == BootstrapEntityType::DqlFactExpression.as_i32()
                {
                    log::debug!(
                        "Expanding consulted entity '{}' from namespace '{}'",
                        identifier.name,
                        fq
                    );
                    found_entity = Some((entity.definition.clone(), entity.entity_type));
                    break;
                }
            }
        }

        if let Some((body_source, entity_type)) = found_entity {
            // Capture view name and namespace for error context
            let view_name = identifier.name.clone();
            let view_ns = grounding
                .grounded_ns
                .first()
                .map(|ns| super::grounding::namespace_path_to_fq(ns))
                .unwrap_or_default();

            if entity_type == BootstrapEntityType::DqlFactExpression.as_i32() {
                // Fact: parse all clauses and merge into one anonymous table
                let expanded = expand_fact_body(&body_source, &view_name).map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Error while expanding fact '{}' (from namespace '{}'): {}",
                            view_name, view_ns, e
                        ),
                        e.to_string(),
                    )
                })?;

                let (resolved_body, body_bubbled) =
                    super::resolve_relational_expression_with_registry(
                        expanded,
                        registry,
                        outer_context,
                        config,
                        Some(grounding),
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!(
                                "Error while resolving fact '{}' (from namespace '{}'): {}",
                                view_name, view_ns, e
                            ),
                            e.to_string(),
                        )
                    })?;

                let body_schema = super::helpers::extraction::extract_cpr_schema(&resolved_body)?;
                let effective_alias: Option<SqlIdentifier> = Some(
                    alias
                        .clone()
                        .unwrap_or_else(|| crate::pipeline::transformer_v3::next_alias().into()),
                );

                let effective_name = effective_alias.as_deref().unwrap_or(&view_name);

                let base_expr = ast_resolved::RelationalExpression::Relation(
                    ast_resolved::Relation::InnerRelation {
                        pattern: ast_resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                            identifier: ast_resolved::QualifiedName {
                                namespace_path: NamespacePath::empty(),
                                name: view_name.clone().into(),
                                grounding: None,
                            },
                            subquery: Box::new(resolved_body),
                            is_consulted_view: false,
                        },
                        alias: effective_alias.clone(),
                        outer,
                        cpr_schema: ast_resolved::PhaseBox::new(body_schema.clone()),
                    },
                );

                if !matches!(domain_spec, ast_unresolved::DomainSpec::Glob) {
                    let (final_expr, final_bubbled) = apply_call_site_pattern(
                        &domain_spec,
                        base_expr,
                        &body_schema,
                        effective_name,
                        outer_context,
                    )?;
                    return Ok((final_expr, final_bubbled));
                } else {
                    let body_bubbled = relabel_bubbled_with_alias(body_bubbled, effective_name);
                    return Ok((base_expr, body_bubbled));
                }
            } else {
                // View: parse and patch the view body as a full Query (preserves CTEs)
                let query = super::grounding::expand_consulted_view(&body_source, grounding)
                    .map_err(|e| {
                        // Preserve validation errors (e.g., head form mismatches)
                        // so error assertions can match on the subcategory URI.
                        if matches!(e, DelightQLError::ValidationError { .. }) {
                            return e;
                        }
                        DelightQLError::database_error(
                            format!(
                                "Error while expanding view '{}' (from namespace '{}'): {}",
                                view_name, view_ns, e
                            ),
                            e.to_string(),
                        )
                    })?;

                let (resolved_query, body_bubbled) = super::resolve_query_inline(
                    query,
                    registry,
                    outer_context,
                    config,
                    Some(grounding),
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Error while resolving view '{}' (from namespace '{}'): {}",
                            view_name, view_ns, e
                        ),
                        e.to_string(),
                    )
                })?;

                let body_schema =
                    super::helpers::extraction::extract_cpr_schema_from_query(&resolved_query)?;
                let effective_alias: SqlIdentifier = alias
                    .clone()
                    .unwrap_or_else(|| crate::pipeline::transformer_v3::next_alias().into());

                let effective_name = effective_alias.to_string();

                let scoped =
                    ast_resolved::ScopedSchema::bind(body_schema.clone(), effective_alias.clone());

                let base_expr = ast_resolved::RelationalExpression::Relation(
                    ast_resolved::Relation::ConsultedView {
                        identifier: ast_resolved::QualifiedName {
                            namespace_path: NamespacePath::empty(),
                            name: view_name.clone().into(),
                            grounding: None,
                        },
                        body: Box::new(resolved_query),
                        scoped: ast_resolved::PhaseBox::new(scoped),
                        outer,
                    },
                );

                if !matches!(domain_spec, ast_unresolved::DomainSpec::Glob) {
                    let (final_expr, final_bubbled) = apply_call_site_pattern(
                        &domain_spec,
                        base_expr,
                        &body_schema,
                        &effective_name,
                        outer_context,
                    )?;
                    return Ok((final_expr, final_bubbled));
                } else {
                    let body_bubbled = relabel_bubbled_with_alias(body_bubbled, &effective_name);
                    return Ok((base_expr, body_bubbled));
                }
            }
        }

        // Entity not in any grounded namespace S — check the data namespace F.
        // F^S.table_name(*) should resolve table_name from F when it's not a view in S.
        let data_ns_path = identifier.namespace_path.clone();
        match registry
            .database
            .lookup_table_with_namespace(&data_ns_path, &identifier.name)
        {
            Ok(Some((table_schema, connection_id, canonical_name))) => {
                // Table found in data namespace F. Grounding is NOT propagated
                // to pipe operators — functions from S require enlist!() (see test 302).
                // Track connection_id for cross-connection join validation
                registry.track_connection_id(connection_id);
                ResolutionResult::DatabaseEntity(crate::resolution::EntityInfo {
                    name: identifier.name.clone(),
                    canonical_name: Some(canonical_name),
                    resolved_namespace: Some(data_ns_path.clone()),
                    entity_type: crate::resolution::EntityType::Relation,
                    registry_source: crate::resolution::RegistrySource::Database,
                    schema_source: crate::resolution::SchemaSource::DatabaseCatalog,
                    definition: EntityDefinition::RelationSchema(table_schema),
                })
            }
            Ok(None) => {
                // Not found in data namespace either
                ResolutionResult::Unknown(identifier.namespace_path.with_table(&identifier.name))
            }
            Err(e) => return Err(e),
        }
    } else if !identifier.namespace_path.is_empty() {
        // Namespace-qualified table (no grounding) - use bootstrap resolution
        match registry
            .database
            .lookup_table_with_namespace(&identifier.namespace_path, &identifier.name)
        {
            Ok(Some((table_schema, connection_id, canonical_name))) => {
                // Found table at namespace location
                // Track connection_id for cross-connection join validation
                registry.track_connection_id(connection_id);
                ResolutionResult::DatabaseEntity(crate::resolution::EntityInfo {
                    name: identifier.name.clone(),
                    canonical_name: Some(canonical_name),
                    resolved_namespace: Some(identifier.namespace_path.clone()),
                    entity_type: crate::resolution::EntityType::Relation,
                    registry_source: crate::resolution::RegistrySource::Database,
                    schema_source: crate::resolution::SchemaSource::DatabaseCatalog,
                    definition: EntityDefinition::RelationSchema(table_schema),
                })
            }
            Ok(None) => {
                // Not a database table — check consult registry for consulted views
                let fq = super::grounding::namespace_path_to_fq(&identifier.namespace_path);
                if let Some(entity) = registry.consult.lookup_entity(&identifier.name, &fq) {
                    if entity.entity_type
                        == BootstrapEntityType::DqlTemporaryViewExpression.as_i32()
                    {
                        ResolutionResult::ConsultedView {
                            name: entity.name.clone(),
                            body_source: entity.definition.clone(),
                            namespace: fq.clone(),
                        }
                    } else if entity.entity_type == BootstrapEntityType::DqlFactExpression.as_i32()
                    {
                        ResolutionResult::ConsultedFact {
                            name: entity.name.clone(),
                            body_source: entity.definition.clone(),
                        }
                    } else {
                        ResolutionResult::Unknown(
                            identifier.namespace_path.with_table(&identifier.name),
                        )
                    }
                } else if let Some(grounding) = grounding {
                    // Fallback: entity not in patched namespace, search grounded namespaces.
                    // Handles inline DDL views referencing sibling entities: DataNsPatcher
                    // rewrites sample(*) → main::sample(*), but fact lives in "main::user".
                    let mut fallback_result = None;
                    for ns in &grounding.grounded_ns {
                        let gfq = super::grounding::namespace_path_to_fq(ns);
                        if let Some(entity) =
                            registry.consult.lookup_entity(&identifier.name, &gfq)
                        {
                            if entity.entity_type
                                == BootstrapEntityType::DqlTemporaryViewExpression.as_i32()
                            {
                                fallback_result = Some(ResolutionResult::ConsultedView {
                                    name: entity.name.clone(),
                                    body_source: entity.definition.clone(),
                                    namespace: gfq,
                                });
                            } else if entity.entity_type
                                == BootstrapEntityType::DqlFactExpression.as_i32()
                            {
                                fallback_result = Some(ResolutionResult::ConsultedFact {
                                    name: entity.name.clone(),
                                    body_source: entity.definition.clone(),
                                });
                            }
                            break;
                        }
                    }
                    fallback_result.unwrap_or_else(|| {
                        ResolutionResult::Unknown(
                            identifier.namespace_path.with_table(&identifier.name),
                        )
                    })
                } else {
                    ResolutionResult::Unknown(
                        identifier.namespace_path.with_table(&identifier.name),
                    )
                }
            }
            Err(e) => {
                // Namespace resolution failed (unknown namespace)
                // Return error early - don't try other resolution paths
                return Err(e);
            }
        }
    } else {
        // Unqualified table - use existing resolution path
        let entity_name = identifier.name.clone();
        resolve_entity_with_alias(
            &entity_name,
            alias.as_deref(),
            registry,
            config.resolution_namespace.as_deref(),
        )
    };

    match resolution {
        ResolutionResult::CTE(entity_info) => {
            // Extract the CTE schema
            let EntityDefinition::RelationSchema(cte_schema) = entity_info.definition;
            match &cte_schema {
                ast_resolved::CprSchema::Resolved(cols) => {
                    // Apply alias if present
                    let base_cols = if let Some(alias_name) = &alias {
                        cols.iter()
                            .map(|col| {
                                let mut new_col = col.clone();
                                new_col.fq_table.name =
                                    ast_resolved::TableName::Named(alias_name.clone().into());
                                // Push SubqueryAlias so the provenance stack
                                // carries the alias for qualifier resolution
                                let prev = col.info.name().unwrap_or("<unnamed>").to_string();
                                new_col.info = new_col.info.clone().with_identity(
                                    ast_resolved::ColumnIdentity {
                                        name: prev.clone().into(),
                                        context: ast_resolved::IdentityContext::SubqueryAlias {
                                            alias: alias_name.to_string(),
                                            previous_context: prev,
                                        },
                                        phase: ast_resolved::TransformationPhase::Resolved,
                                        table_qualifier: ast_resolved::TableName::Named(
                                            alias_name.clone().into(),
                                        ),
                                    },
                                );
                                new_col
                            })
                            .collect()
                    } else {
                        cols.clone()
                    };

                    // Use PatternResolver for column selection
                    let (mut final_expr, state) = apply_pattern_resolver(
                        &domain_spec,
                        &base_cols,
                        alias.as_deref().unwrap_or(&identifier.name),
                        registry,
                        outer_context,
                    )?;

                    // Update the relation with proper identifier and alias (CTEs have no canonical name)
                    if let ast_resolved::RelationalExpression::Relation(ref mut r) = final_expr {
                        if let ast_resolved::Relation::Ground {
                            identifier: ref mut rel_id,
                            canonical_name: ref mut rel_canonical,
                            alias: ref mut rel_alias,
                            outer: ref mut rel_outer,
                            ..
                        } = r
                        {
                            *rel_id = convert_qualified_name(identifier.clone());
                            *rel_canonical = ast_resolved::PhaseBox::new(None);
                            *rel_alias = alias.clone();
                            *rel_outer = outer;
                        }
                    } else if let ast_resolved::RelationalExpression::Filter {
                        ref mut source,
                        ..
                    } = final_expr
                    {
                        // If it's wrapped in a Filter, update the inner relation
                        if let ast_resolved::RelationalExpression::Relation(ref mut r) =
                            source.as_mut()
                        {
                            if let ast_resolved::Relation::Ground {
                                identifier: ref mut rel_id,
                                canonical_name: ref mut rel_canonical,
                                alias: ref mut rel_alias,
                                outer: ref mut rel_outer,
                                ..
                            } = r
                            {
                                *rel_id = convert_qualified_name(identifier.clone());
                                *rel_canonical = ast_resolved::PhaseBox::new(None);
                                *rel_alias = alias;
                                *rel_outer = outer;
                            }
                        }
                    }

                    Ok((final_expr, state))
                }
                _ => {
                    // Fallback for non-resolved schemas
                    let resolved = ast_resolved::Relation::Ground {
                        identifier: convert_qualified_name(identifier),
                        canonical_name: ast_resolved::PhaseBox::new(None),
                        domain_spec: preserve_domain_spec(&domain_spec)?,
                        alias,
                        outer,
                        mutation_target: false,
                        passthrough: false,
                        cpr_schema: ast_resolved::PhaseBox::new(cte_schema.clone()),
                        hygienic_injections: Vec::new(),
                    };
                    Ok((
                        ast_resolved::RelationalExpression::Relation(resolved),
                        BubbledState::resolved(vec![]),
                    ))
                }
            }
        }
        ResolutionResult::DatabaseEntity(entity_info) => {
            // Extract fields before entity_info is consumed
            let canonical_name = entity_info.canonical_name.clone();
            let resolved_namespace = entity_info.resolved_namespace.clone();
            // Extract the table schema
            let EntityDefinition::RelationSchema(table_schema) = entity_info.definition;
            match &table_schema {
                ast_resolved::CprSchema::Resolved(cols) => {
                    // Apply alias if present
                    let base_cols = if let Some(alias_name) = &alias {
                        cols.iter()
                            .map(|col| {
                                let mut new_col = col.clone();
                                new_col.fq_table.name =
                                    ast_resolved::TableName::Named(alias_name.clone().into());
                                new_col
                            })
                            .collect()
                    } else {
                        cols.clone()
                    };

                    // Use PatternResolver for column selection
                    let (mut final_expr, state) = apply_pattern_resolver(
                        &domain_spec,
                        &base_cols,
                        alias.as_deref().unwrap_or(&identifier.name),
                        registry,
                        outer_context,
                    )?;

                    // Build the resolved identifier, using the discovered
                    // namespace path so the transformer can emit schema-qualified SQL.
                    let mut resolved_id = convert_qualified_name(identifier.clone());
                    if let Some(ref ns) = resolved_namespace {
                        resolved_id.namespace_path = ns.clone();
                    }

                    // Update the relation with proper identifier, alias, and canonical name
                    if let ast_resolved::RelationalExpression::Relation(ref mut r) = final_expr {
                        if let ast_resolved::Relation::Ground {
                            identifier: ref mut rel_id,
                            canonical_name: ref mut rel_canonical,
                            alias: ref mut rel_alias,
                            outer: ref mut rel_outer,
                            ..
                        } = r
                        {
                            *rel_id = resolved_id.clone();
                            *rel_canonical = ast_resolved::PhaseBox::new(canonical_name.clone());
                            *rel_alias = alias.clone();
                            *rel_outer = outer;
                        }
                    } else if let ast_resolved::RelationalExpression::Filter {
                        ref mut source,
                        ..
                    } = final_expr
                    {
                        // If it's wrapped in a Filter, update the inner relation
                        if let ast_resolved::RelationalExpression::Relation(ref mut r) =
                            source.as_mut()
                        {
                            if let ast_resolved::Relation::Ground {
                                identifier: ref mut rel_id,
                                canonical_name: ref mut rel_canonical,
                                alias: ref mut rel_alias,
                                outer: ref mut rel_outer,
                                ..
                            } = r
                            {
                                *rel_id = resolved_id;
                                *rel_canonical =
                                    ast_resolved::PhaseBox::new(canonical_name.clone());
                                *rel_alias = alias;
                                *rel_outer = outer;
                            }
                        }
                    }

                    Ok((final_expr, state))
                }
                _ => {
                    // Fallback for non-resolved schemas
                    let mut fallback_id = convert_qualified_name(identifier);
                    if let Some(ref ns) = resolved_namespace {
                        fallback_id.namespace_path = ns.clone();
                    }
                    let resolved = ast_resolved::Relation::Ground {
                        identifier: fallback_id,
                        canonical_name: ast_resolved::PhaseBox::new(canonical_name.clone()),
                        domain_spec: preserve_domain_spec(&domain_spec)?,
                        alias,
                        outer,
                        mutation_target: false,
                        passthrough: false,
                        cpr_schema: ast_resolved::PhaseBox::new(table_schema.clone()),
                        hygienic_injections: Vec::new(),
                    };
                    Ok((
                        ast_resolved::RelationalExpression::Relation(resolved),
                        BubbledState::resolved(vec![]),
                    ))
                }
            }
        }
        ResolutionResult::ConsultedView {
            name: view_name,
            body_source,
            namespace: view_ns,
        } => {
            // Consulted view — expand the body and resolve recursively.
            //
            // Check if this view comes from a pre-grounded namespace
            // (created by ground_into!). If so, apply data namespace patching
            // so unqualified table references resolve to the bound data namespace.
            let auto_grounding = registry
                .consult
                .get_namespace_default_data_ns(&view_ns)
                .and_then(|data_ns_fq| {
                    let parts: Vec<String> =
                        data_ns_fq.split("::").map(|s| s.to_string()).collect();
                    let data_ns = ast_unresolved::NamespacePath::from_parts(parts).ok()?;
                    let ns_parts: Vec<String> =
                        view_ns.split("::").map(|s| s.to_string()).collect();
                    let grounded_ns = ast_unresolved::NamespacePath::from_parts(ns_parts).ok()?;
                    Some(ast_unresolved::GroundedPath {
                        data_ns,
                        grounded_ns: vec![grounded_ns],
                    })
                });

            if let Some(ref grounding) = auto_grounding {
                // Pre-grounded namespace: expand view as full Query (preserves CTEs)
                let query = super::grounding::expand_consulted_view(
                    &body_source, grounding,
                ).map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Error while expanding pre-grounded view '{}' (from namespace '{}'): {}",
                            view_name, view_ns, e
                        ),
                        e.to_string(),
                    )
                })?;

                let (resolved_query, body_bubbled) =
                    super::resolve_query_inline(
                        query, registry, outer_context, config, Some(grounding),
                    ).map_err(|e| {
                        DelightQLError::database_error(
                            format!(
                                "Error while resolving pre-grounded view '{}' (from namespace '{}'): {}",
                                view_name, view_ns, e
                            ),
                            e.to_string(),
                        )
                    })?;

                let body_schema =
                    super::helpers::extraction::extract_cpr_schema_from_query(&resolved_query)?;

                let effective_alias: SqlIdentifier = alias
                    .clone()
                    .unwrap_or_else(|| crate::pipeline::transformer_v3::next_alias().into());

                let effective_name = effective_alias.to_string();

                let scoped =
                    ast_resolved::ScopedSchema::bind(body_schema.clone(), effective_alias.clone());

                let base_expr = ast_resolved::RelationalExpression::Relation(
                    ast_resolved::Relation::ConsultedView {
                        identifier: ast_resolved::QualifiedName {
                            namespace_path: NamespacePath::empty(),
                            name: view_name.clone().into(),
                            grounding: None,
                        },
                        body: Box::new(resolved_query),
                        scoped: ast_resolved::PhaseBox::new(scoped),
                        outer,
                    },
                );

                if !matches!(domain_spec, ast_unresolved::DomainSpec::Glob) {
                    let (final_expr, final_bubbled) = apply_call_site_pattern(
                        &domain_spec,
                        base_expr,
                        &body_schema,
                        &effective_name,
                        outer_context,
                    )?;
                    return Ok((final_expr, final_bubbled));
                } else {
                    let body_bubbled = relabel_bubbled_with_alias(body_bubbled, &effective_name);
                    return Ok((base_expr, body_bubbled));
                }
            }

            // Normal consulted view (not pre-grounded) — parse as full Query
            // to preserve CTEs, then resolve through the full pipeline.
            // Uses build_ddl_file to handle multi-clause (disjunctive) views.
            let defs = crate::ddl::ddl_builder::build_ddl_file(&body_source).map_err(|e| {
                DelightQLError::database_error(
                    format!("Error while parsing borrowed view '{}': {}", view_name, e),
                    e.to_string(),
                )
            })?;
            // Enforce argumentative head contracts (translate to glob heads with projections)
            let has_arg = defs.iter().any(|d| {
                matches!(
                    d.head,
                    crate::pipeline::asts::ddl::DdlHead::ArgumentativeView { .. }
                )
            });
            let defs = if has_arg {
                super::grounding::desugar_argumentative_defs(defs)?
            } else {
                defs
            };
            let query = if defs.len() <= 1 {
                // Single clause: same as before
                let ddl_def = defs.into_iter().next().ok_or_else(|| {
                    DelightQLError::parse_error(format!(
                        "No definition found for view '{}'",
                        view_name
                    ))
                })?;
                ddl_def.into_query().ok_or_else(|| {
                    DelightQLError::parse_error(format!(
                        "Expected relational body for view '{}', got scalar",
                        view_name
                    ))
                })?
            } else {
                // Multi-clause: synthesize disjunctive CTEs (no data_ns patching
                // since this is a non-grounded borrowed view)
                super::grounding::expand_multi_clause_view(defs, None).map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Error while expanding disjunctive view '{}': {}",
                            view_name, e
                        ),
                        e.to_string(),
                    )
                })?
            };

            // Inline sibling functions from the consult registry before resolution.
            // Without this, function calls like `double:(b)` in the view body
            // would pass through to SQL as unresolved function names.
            let (query, view_ccafe_cfes) =
                super::grounding::inline_in_query_borrowed(query, &registry.consult, None)
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!(
                                "Error while inlining functions in view '{}': {}",
                                view_name, e
                            ),
                            e.to_string(),
                        )
                    })?;

            // If any context-aware DDL functions were discovered in the view body,
            // precompile them and wrap the query with WithPrecompiledCfes.
            let query = if !view_ccafe_cfes.is_empty() {
                let precompiled: Vec<_> = view_ccafe_cfes
                    .into_iter()
                    .map(|cfe| {
                        crate::pipeline::cfe_precompiler::definition::precompile_cfe_definition(
                            cfe,
                            registry.database.schema(),
                            registry.database.system,
                        )
                    })
                    .collect::<crate::error::Result<_>>()?;
                ast_unresolved::Query::WithPrecompiledCfes {
                    cfes: precompiled,
                    query: Box::new(query),
                }
            } else {
                query
            };

            // Scope ER-rule lookups to the view's namespace for qualified access
            let body_config = if !view_ns.is_empty() && view_ns != "main" {
                ResolutionConfig {
                    resolution_namespace: Some(view_ns.clone()),
                    ..config.clone()
                }
            } else {
                config.clone()
            };

            // Temporarily activate namespace-local enlists and aliases so the view body
            // can resolve entities from namespaces enlisted inside its DDL.
            let activated_enlists = registry.consult.activate_namespace_local_enlists(&view_ns);
            let activated_aliases = registry.consult.activate_namespace_local_aliases(&view_ns);

            let resolve_result =
                super::resolve_query_inline(query, registry, outer_context, &body_config, None);

            // Deactivate before checking the result (cleanup on both success and error)
            registry
                .consult
                .deactivate_namespace_local_aliases(&activated_aliases);
            registry
                .consult
                .deactivate_namespace_local_enlists(&activated_enlists);

            let (resolved_query, body_bubbled) = resolve_result.map_err(|e| {
                DelightQLError::database_error(
                    format!("Error while resolving borrowed view '{}': {}", view_name, e),
                    e.to_string(),
                )
            })?;

            let body_schema =
                super::helpers::extraction::extract_cpr_schema_from_query(&resolved_query)?;

            // Seal provenance at the ConsultedView boundary: the view's output
            // column names ARE the subquery's output column names. Inner provenance
            // (UserAliases from the view body's internal CTEs/pipes) must not leak
            // through, or downstream operators (RenameCover, etc.) will see stale
            // source_name() values and generate SQL referencing inner column names
            // that don't exist in the subquery output.
            fn seal_column_provenance(col: &mut ast_resolved::ColumnMetadata) {
                let display_name = col.info.name().unwrap_or("?").to_string();
                col.info = ast_resolved::ColumnProvenance::from_column(display_name);
            }

            let body_schema = match body_schema {
                ast_resolved::CprSchema::Resolved(cols) => {
                    let sealed = cols
                        .into_iter()
                        .map(|mut col| {
                            seal_column_provenance(&mut col);
                            col
                        })
                        .collect();
                    ast_resolved::CprSchema::Resolved(sealed)
                }
                other => other,
            };

            // Also seal the bubbled state — pipe operators get input columns
            // from the bubbled state, not the scoped schema.
            let body_bubbled = {
                let mut bubbled = body_bubbled;
                for col in &mut bubbled.i_provide {
                    seal_column_provenance(col);
                }
                bubbled
            };

            let effective_alias: SqlIdentifier =
                alias.unwrap_or_else(|| crate::pipeline::transformer_v3::next_alias().into());

            let effective_name = effective_alias.to_string();

            let scoped =
                ast_resolved::ScopedSchema::bind(body_schema.clone(), effective_alias.clone());

            let base_expr = ast_resolved::RelationalExpression::Relation(
                ast_resolved::Relation::ConsultedView {
                    identifier: ast_resolved::QualifiedName {
                        namespace_path: NamespacePath::empty(),
                        name: view_name.clone().into(),
                        grounding: None,
                    },
                    body: Box::new(resolved_query),
                    scoped: ast_resolved::PhaseBox::new(scoped),
                    outer,
                },
            );

            if !matches!(domain_spec, ast_unresolved::DomainSpec::Glob) {
                let (final_expr, final_bubbled) = apply_call_site_pattern(
                    &domain_spec,
                    base_expr,
                    &body_schema,
                    &effective_name,
                    outer_context,
                )?;
                Ok((final_expr, final_bubbled))
            } else {
                let body_bubbled = relabel_bubbled_with_alias(body_bubbled, &effective_name);
                Ok((base_expr, body_bubbled))
            }
        }
        ResolutionResult::ConsultedFact {
            name: fact_name,
            body_source,
        } => {
            // Consulted fact — parse all clauses, merge rows into one
            // anonymous table, resolve, and wrap as a subquery.
            let body = expand_fact_body(&body_source, &fact_name).map_err(|e| {
                DelightQLError::database_error(
                    format!("Error while expanding borrowed fact '{}': {}", fact_name, e),
                    e.to_string(),
                )
            })?;

            let (resolved_body, body_bubbled) = super::resolve_relational_expression_with_registry(
                body,
                registry,
                outer_context,
                config,
                None,
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Error while resolving borrowed fact '{}': {}", fact_name, e),
                    e.to_string(),
                )
            })?;

            let body_schema = super::helpers::extraction::extract_cpr_schema(&resolved_body)?;

            let effective_alias: Option<SqlIdentifier> =
                Some(alias.unwrap_or_else(|| crate::pipeline::transformer_v3::next_alias().into()));

            let effective_name = effective_alias.as_deref().unwrap_or(&fact_name);

            let base_expr = ast_resolved::RelationalExpression::Relation(
                ast_resolved::Relation::InnerRelation {
                    pattern: ast_resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                        identifier: ast_resolved::QualifiedName {
                            namespace_path: NamespacePath::empty(),
                            name: fact_name.clone().into(),
                            grounding: None,
                        },
                        subquery: Box::new(resolved_body),
                        is_consulted_view: false,
                    },
                    alias: effective_alias.clone(),
                    outer,
                    cpr_schema: ast_resolved::PhaseBox::new(body_schema.clone()),
                },
            );

            if !matches!(domain_spec, ast_unresolved::DomainSpec::Glob) {
                let (final_expr, final_bubbled) = apply_call_site_pattern(
                    &domain_spec,
                    base_expr,
                    &body_schema,
                    effective_name,
                    outer_context,
                )?;
                Ok((final_expr, final_bubbled))
            } else {
                let body_bubbled = relabel_bubbled_with_alias(body_bubbled, effective_name);
                Ok((base_expr, body_bubbled))
            }
        }
        ResolutionResult::Unknown(ref msg) if msg.contains("Ambiguous entity") => {
            // Ambiguity error from resolve_unqualified_entity —
            // entity exists in multiple engaged namespaces.
            // Surface this as a clear error instead of "table not found".
            Err(DelightQLError::validation_error(
                msg.clone(),
                "Ambiguous unqualified entity resolution",
            ))
        }
        _ => {
            // Unknown entity - error out unless in transpile-only mode
            // This represents a table/entity that couldn't be resolved

            // Check if we're in transpile-only mode
            if config.transpile_only {
                // In transpile-only mode, allow unknown tables with Unknown schema
                let resolved_relation = ast_resolved::Relation::Ground {
                    identifier: convert_qualified_name(identifier),
                    canonical_name: ast_resolved::PhaseBox::new(None),
                    domain_spec: preserve_domain_spec(&domain_spec)?,
                    alias,
                    outer,
                    mutation_target: false,
                    passthrough: false,
                    cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Unknown),
                    hygienic_injections: Vec::new(),
                };

                let state = BubbledState::resolved(vec![]);

                Ok((
                    ast_resolved::RelationalExpression::Relation(resolved_relation),
                    state,
                ))
            } else {
                // In normal mode, error out for unknown tables
                let (table_name, context) = if !identifier.namespace_path.is_empty() {
                    // Construct namespace path string using :: separator (DelightQL format)
                    let ns_parts: Vec<_> = identifier
                        .namespace_path
                        .iter_reversed()
                        .map(|i| i.name.as_str())
                        .collect();
                    let ns_str = ns_parts.join("::");
                    (
                        identifier.name.to_string(),
                        format!("Entity '{}' not found in namespace '{}'. Possible causes: namespace not resolved, entity not activated, or missing backend schema configuration.", identifier.name, ns_str)
                    )
                } else {
                    (
                        identifier.name.to_string(),
                        "Table or view does not exist in the database".to_string(),
                    )
                };

                Err(DelightQLError::TableNotFoundError {
                    table_name,
                    context,
                })
            }
        }
    }
}

/// Resolve an Anonymous relation variant (inline table with rows/headers).
///
/// Handles header resolution, row value resolution, and QUA schema conformance.
fn resolve_anonymous(
    rel: ast_unresolved::Relation,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    let ast_unresolved::Relation::Anonymous {
        column_headers,
        rows,
        alias: relation_alias,
        outer,
        exists_mode,
        qua_target,
        cpr_schema: _,
    } = rel
    else {
        unreachable!("resolve_anonymous called with non-Anonymous variant");
    };

    // P10 FIX: Anonymous relations now support DomainExpression headers for unification

    // Convert rows from unresolved to resolved format
    // EPOCH 7: Resolve anonymous table data rows with outer_context for melt/unpivot
    let resolved_rows: Result<Vec<ast_resolved::Row>> = rows
        .iter()
        .map(|row| {
            // Row is a struct with values field, not an enum
            let resolved_values: Result<Vec<ast_resolved::DomainExpression>> = row
                .values
                .clone()
                .into_iter()
                .map(|val| {
                    match val {
                        ast_unresolved::DomainExpression::Literal { value, alias } => {
                            // Literals pass through unchanged
                            Ok(ast_resolved::DomainExpression::Literal { value, alias })
                        }
                        // EPOCH 7: Resolve column references and other expressions
                        // This enables melt/unpivot patterns like:
                        // _(attr, val @ "name", first_name; "id", user_id)
                        //                       ^^^^^^^^^^      ^^^^^^^
                        _ => {
                            // Use outer_context to resolve column references from joined tables
                            let available = outer_context.unwrap_or(&[]);
                            resolve_domain_expr_with_schema(val, available, None)
                        }
                    }
                })
                .collect();

            Ok(ast_resolved::Row {
                values: resolved_values?,
            })
        })
        .collect();

    let resolved_rows = resolved_rows?;

    // P10 FIX: Process DomainExpression headers and resolve them
    // Headers can now contain references that need resolution for unification
    let (resolved_headers, resolved_schema) = if let Some(headers) = &column_headers {
        let mut resolved_headers = Vec::new();
        let mut columns = Vec::new();

        for (idx, header) in headers.iter().enumerate() {
            match header {
                ast_unresolved::DomainExpression::Lvar {
                    name,
                    qualifier,
                    namespace_path,
                    alias,
                    provenance: _,
                } => {
                    // This is a column reference - could be simple or qualified
                    // For unification, we need to track what it references
                    resolved_headers.push(ast_resolved::DomainExpression::Lvar {
                        name: name.clone(),
                        qualifier: qualifier.clone(),
                        namespace_path: namespace_path.clone(),
                        alias: alias.clone(),
                        provenance: ast_resolved::PhaseBox::phantom(),
                    });

                    // Create column metadata
                    // If this is a qualified reference (e.g., o.status), we need to track it
                    let col_name = if qualifier.is_some() {
                        // Qualified - this creates a unification constraint
                        name.clone()
                    } else {
                        // Simple identifier - use as column name
                        name.clone()
                    };

                    let table_name = if let Some(alias_name) = &relation_alias {
                        ast_resolved::TableName::Named(alias_name.clone().into())
                    } else {
                        ast_resolved::TableName::Fresh
                    };
                    let mut prov = ast_resolved::ColumnProvenance::from_table_column(
                        col_name.clone(),
                        table_name.clone(),
                        qualifier.is_some(),
                    );
                    if let Some(alias_str) = &alias {
                        prov = prov.with_alias(alias_str.clone());
                    }
                    // Note: qualifier information is already captured in the was_qualified flag
                    columns.push(ast_resolved::ColumnMetadata::new_with_name_flag(
                        prov,
                        ast_resolved::FqTable {
                            parents_path: NamespacePath::empty(),
                            name: table_name,
                            backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                        },
                        Some(idx + 1),
                        true, // Explicit headers are user-provided names
                    ));
                }
                _ => {
                    // Preserve all other expression types (functions, literals, etc.)
                    // Use simple conversion without schema validation since anonymous table
                    // headers may reference tables that will be joined later
                    let resolved_expr = convert_domain_expression(header)?;
                    resolved_headers.push(resolved_expr.clone());

                    // Generate column metadata based on expression type
                    // Track hygienic column counter for anonymous tables
                    static ANON_HYGIENIC_COUNTER: std::sync::atomic::AtomicUsize =
                        std::sync::atomic::AtomicUsize::new(0);

                    let (col_name, needs_hygienic) = match &resolved_expr {
                        ast_resolved::DomainExpression::Function(_func) => {
                            // Function expressions in anonymous table headers that reference
                            // columns (for unification) should be hidden from output
                            // Use hygienic name directly
                            let hygienic_id = ANON_HYGIENIC_COUNTER
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let name = format!("__dql_anon_{}", hygienic_id);
                            (name, true)
                        }
                        ast_resolved::DomainExpression::Literal { value, .. } => {
                            // For literals, use a generic column name
                            let name = match value {
                                ast_resolved::LiteralValue::String(s)
                                    if s.starts_with("column") =>
                                {
                                    s.clone()
                                }
                                _ => crate::pipeline::naming::anonymous_column_name(idx),
                            };
                            (name, false)
                        }
                        other => panic!("catch-all hit in relation_resolver.rs resolve_inline_relation (DomainExpression column name): {:?}", other),
                    };

                    let table_name = if let Some(alias_name) = &relation_alias {
                        ast_resolved::TableName::Named(alias_name.clone().into())
                    } else {
                        ast_resolved::TableName::Fresh
                    };
                    let mut col_meta = ast_resolved::ColumnMetadata::new_with_name_flag(
                        ast_resolved::ColumnProvenance::from_table_column(
                            col_name,
                            table_name.clone(),
                            false,
                        ),
                        ast_resolved::FqTable {
                            parents_path: NamespacePath::empty(),
                            name: table_name,
                            backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                        },
                        Some(idx + 1),
                        false, // Anonymous table columns don't have user names
                    );
                    col_meta.needs_hygienic_alias = needs_hygienic;
                    columns.push(col_meta);
                }
            }
        }

        (
            Some(resolved_headers),
            ast_resolved::CprSchema::Resolved(columns),
        )
    } else {
        // No headers - generate automatic column names col1, col2, etc.
        let num_cols = if let Some(first_row) = resolved_rows.first() {
            first_row.values.len()
        } else {
            0
        };
        let columns = (0..num_cols)
            .map(|idx| {
                let table_name = if let Some(alias_name) = &relation_alias {
                    ast_resolved::TableName::Named(alias_name.clone().into())
                } else {
                    ast_resolved::TableName::Fresh
                };
                ast_resolved::ColumnMetadata::new_with_name_flag(
                    ast_resolved::ColumnProvenance::from_table_column(
                        crate::pipeline::naming::anonymous_column_name(idx),
                        table_name.clone(),
                        false,
                    ),
                    ast_resolved::FqTable {
                        parents_path: NamespacePath::empty(),
                        name: table_name,
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    },
                    Some(idx + 1),
                    false, // Anonymous table columns don't have user names
                )
            })
            .collect();
        (None, ast_resolved::CprSchema::Resolved(columns))
    };

    // QUA: Schema conformance — reorder + pad to match target table
    let (resolved_headers, resolved_rows, resolved_schema) = if let Some(ref target_name) =
        qua_target
    {
        // 1. Must have headers (positional data can't map to named columns)
        let source_headers = match &resolved_headers {
            Some(h) => h,
            None => {
                return Err(DelightQLError::parse_error(format!(
                    "qua {}: anonymous table must have column headers (positional data cannot conform to a schema)",
                    target_name
                )));
            }
        };

        // 2. Lookup target table in database registry
        let target_schema = match registry.database.lookup_table(target_name.as_str()) {
            Some(schema) => schema,
            None => {
                return Err(DelightQLError::TableNotFoundError {
                    table_name: target_name.to_string(),
                    context: format!("qua target table '{}' not found in database", target_name),
                });
            }
        };

        // 3. Extract target column names
        let target_cols: Vec<String> = match &target_schema {
            ast_resolved::CprSchema::Resolved(cols) => cols
                .iter()
                .filter_map(|c| c.info.name().map(|s| s.to_string()))
                .collect(),
            _ => {
                return Err(DelightQLError::parse_error(format!(
                    "qua {}: target table schema not resolved",
                    target_name
                )));
            }
        };

        // 4. Extract source column names from headers
        let source_names: Vec<String> = source_headers
            .iter()
            .map(|h| {
                match h {
                    ast_resolved::DomainExpression::Lvar { name, .. } => name.to_string(),
                    other => panic!("catch-all hit in relation_resolver.rs resolve_qua_pipe (DomainExpression source_names): {:?}", other),
                }
            })
            .collect();

        // 5. Validate: every source column must exist in target
        for src_name in &source_names {
            if !target_cols.iter().any(|tc| tc == src_name) {
                return Err(DelightQLError::parse_error(format!(
                    "qua {}: column '{}' does not exist in target table (available: {})",
                    target_name,
                    src_name,
                    target_cols.join(", ")
                )));
            }
        }

        // 6. Build mapping: for each target col, find source position or None
        let mapping: Vec<Option<usize>> = target_cols
            .iter()
            .map(|tc| source_names.iter().position(|sn| sn == tc))
            .collect();

        // 7. Rebuild headers in target column order
        let table_name_for_schema = if let Some(alias_name) = &relation_alias {
            ast_resolved::TableName::Named(alias_name.clone().into())
        } else {
            ast_resolved::TableName::Fresh
        };

        let new_headers: Vec<ast_resolved::DomainExpression> = target_cols
            .iter()
            .map(|tc| ast_resolved::DomainExpression::Lvar {
                name: tc.clone().into(),
                qualifier: None,
                namespace_path: NamespacePath::empty(),
                alias: None,
                provenance: ast_resolved::PhaseBox::phantom(),
            })
            .collect();

        // 8. Rebuild rows: reorder values per mapping, insert NULL for missing
        let new_rows: Vec<ast_resolved::Row> = resolved_rows
            .into_iter()
            .map(|row| {
                let new_values: Vec<ast_resolved::DomainExpression> = mapping
                    .iter()
                    .map(|src_idx| match src_idx {
                        Some(idx) => row.values[*idx].clone(),
                        None => ast_resolved::DomainExpression::Literal {
                            value: ast_resolved::LiteralValue::Null,
                            alias: None,
                        },
                    })
                    .collect();
                ast_resolved::Row { values: new_values }
            })
            .collect();

        // 9. Rebuild schema from target columns
        let new_schema_cols: Vec<ast_resolved::ColumnMetadata> = target_cols
            .iter()
            .enumerate()
            .map(|(idx, col_name)| {
                ast_resolved::ColumnMetadata::new_with_name_flag(
                    ast_resolved::ColumnProvenance::from_table_column(
                        col_name.clone(),
                        table_name_for_schema.clone(),
                        false,
                    ),
                    ast_resolved::FqTable {
                        parents_path: NamespacePath::empty(),
                        name: table_name_for_schema.clone(),
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    },
                    Some(idx + 1),
                    true,
                )
            })
            .collect();

        (
            Some(new_headers),
            new_rows,
            ast_resolved::CprSchema::Resolved(new_schema_cols),
        )
    } else {
        (resolved_headers, resolved_rows, resolved_schema)
    };

    let resolved_relation = ast_resolved::Relation::Anonymous {
        column_headers: resolved_headers,
        rows: resolved_rows,
        alias: relation_alias,
        outer,
        exists_mode,
        qua_target: None,
        cpr_schema: ast_resolved::PhaseBox::new(resolved_schema.clone()),
    };

    // Create bubbled state with the schema columns
    let state = match &resolved_schema {
        ast_resolved::CprSchema::Resolved(cols) => BubbledState::resolved(cols.clone()),
        other => panic!(
            "catch-all hit in relation_resolver.rs resolve_tvf (CprSchema): {:?}",
            other
        ),
    };

    Ok((
        ast_resolved::RelationalExpression::Relation(resolved_relation),
        state,
    ))
}

/// Resolve a TVF (Table-Valued Function) variant.
///
/// Handles HO view expansion (grounded, namespace-qualified, and unqualified via engage!),
/// as well as normal TVF resolution with schema lookup.
fn resolve_tvf(
    rel: ast_unresolved::Relation,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    let ast_unresolved::Relation::TVF {
        function,
        arguments,
        argument_groups,
        first_parens_spec,
        domain_spec,
        alias,
        namespace,
        grounding,
        ..
    } = rel
    else {
        unreachable!("resolve_tvf called with non-TVF variant");
    };

    // Build first_parens_spec from existing arguments if not already set.
    // This handles TVFs built outside the parser (e.g., by the effect executor).
    let first_parens_spec = first_parens_spec.unwrap_or_else(|| {
        if arguments.is_empty() {
            ast_unresolved::DomainSpec::Glob
        } else {
            // Fallback: convert flat string args to Lvars
            ast_unresolved::DomainSpec::Positional(
                arguments
                    .iter()
                    .map(|a| {
                        // Quoted strings → Literal, bare identifiers → Lvar
                        if (a.starts_with('"') && a.ends_with('"'))
                            || (a.starts_with('\'') && a.ends_with('\''))
                        {
                            let val = a[1..a.len() - 1].to_string();
                            ast_unresolved::DomainExpression::literal_builder(
                                ast_unresolved::LiteralValue::String(val),
                            )
                            .build()
                        } else if a.parse::<f64>().is_ok() {
                            ast_unresolved::DomainExpression::literal_builder(
                                ast_unresolved::LiteralValue::Number(a.clone()),
                            )
                            .build()
                        } else if a == "*" {
                            ast_unresolved::DomainExpression::glob_builder().build()
                        } else if a == "@" {
                            ast_unresolved::DomainExpression::ValuePlaceholder { alias: None }
                        } else {
                            ast_unresolved::DomainExpression::lvar_builder(a.clone()).build()
                        }
                    })
                    .collect(),
            )
        }
    });

    let groups_ref = argument_groups.as_deref();

    // Check if this TVF is actually a higher-order view invocation
    if let Some(ref grounding) = grounding {
        for ns in &grounding.grounded_ns {
            let fq = super::grounding::namespace_path_to_fq(ns);
            if let Some(entity) = registry.consult.lookup_entity(&function, &fq) {
                if entity.entity_type == BootstrapEntityType::DqlHoTemporaryViewExpression.as_i32()
                {
                    let (table_bindings, scalar_spec, _pipe_idx) =
                        super::grounding::split_ho_first_parens(
                            &first_parens_spec,
                            &entity,
                            None,
                            groups_ref,
                        )?;
                    return expand_ho_view(
                        &function,
                        &entity,
                        &scalar_spec,
                        table_bindings,
                        None,
                        Some(&grounding.data_ns),
                        grounding,
                        registry,
                        outer_context,
                        config,
                    );
                }
            }
        }
    }

    // Namespace-qualified HO view (ns.ho_view(args)(*))
    if grounding.is_none() {
        if let Some(ref ns) = namespace {
            let fq = super::grounding::namespace_path_to_fq(ns);
            if let Some(entity) = registry.consult.lookup_entity(&function, &fq) {
                if entity.entity_type == BootstrapEntityType::DqlHoTemporaryViewExpression.as_i32()
                {
                    let ho_grounding = ast_unresolved::GroundedPath {
                        data_ns: ast_unresolved::NamespacePath::empty(),
                        grounded_ns: vec![ns.clone()],
                    };

                    // Scope ER-rule lookups to the HO-view's namespace
                    let ho_config = if !fq.is_empty() && fq != "main" {
                        ResolutionConfig {
                            resolution_namespace: Some(fq),
                            ..config.clone()
                        }
                    } else {
                        config.clone()
                    };

                    let (table_bindings, scalar_spec, _pipe_idx) =
                        super::grounding::split_ho_first_parens(
                            &first_parens_spec,
                            &entity,
                            None,
                            groups_ref,
                        )?;
                    return expand_ho_view(
                        &function,
                        &entity,
                        &scalar_spec,
                        table_bindings,
                        None,
                        None,
                        &ho_grounding,
                        registry,
                        outer_context,
                        &ho_config,
                    );
                }
            }
        }
    }

    // Fallback: unqualified HO view via engage!
    if grounding.is_none() {
        if let Some(entity) = registry.consult.lookup_enlisted_ho_view(&function)? {
            let ns_parts: Vec<String> = entity.namespace.split("::").map(String::from).collect();
            let entity_ns = ast_unresolved::NamespacePath::from_parts(ns_parts).map_err(|e| {
                DelightQLError::database_error(
                    format!("Invalid namespace for HO view '{}': {:?}", function, e),
                    format!("{:?}", e),
                )
            })?;
            let ho_grounding = ast_unresolved::GroundedPath {
                data_ns: ast_unresolved::NamespacePath::empty(),
                grounded_ns: vec![entity_ns],
            };

            let (table_bindings, scalar_spec, _pipe_idx) =
                super::grounding::split_ho_first_parens(
                    &first_parens_spec,
                    &entity,
                    None,
                    groups_ref,
                )?;
            return expand_ho_view(
                &function,
                &entity,
                &scalar_spec,
                table_bindings,
                None,
                None,
                &ho_grounding,
                registry,
                outer_context,
                config,
            );
        }
    }

    // Resolve column ordinals in TVF arguments against outer context.
    // Ordinals like |1| in `json_each(|1|)` must be resolved to actual column
    // names before SQL generation, since SQL doesn't understand ordinal syntax.
    let mut arguments = arguments;
    if let ast_unresolved::DomainSpec::Positional(ref exprs) = first_parens_spec {
        if let Some(context) = outer_context {
            for (i, expr) in exprs.iter().enumerate() {
                if let ast_unresolved::DomainExpression::ColumnOrdinal(ref ordinal_box) = expr {
                    let ordinal = ordinal_box.get();
                    let candidates: Vec<_> = if let Some(ref qual) = ordinal.qualifier {
                        context
                            .iter()
                            .filter(|col| {
                                matches!(&col.fq_table.name, ast_resolved::TableName::Named(t) if t == qual)
                            })
                            .collect()
                    } else {
                        context.iter().collect()
                    };

                    if candidates.is_empty() {
                        return Err(DelightQLError::ColumnNotFoundError {
                            column: format!("|{}|", ordinal.position),
                            context: "No columns available for ordinal resolution in TVF argument"
                                .to_string(),
                        });
                    }

                    let idx = if ordinal.reverse {
                        if ordinal.position as usize > candidates.len() {
                            return Err(DelightQLError::ColumnNotFoundError {
                                column: format!("|-{}|", ordinal.position),
                                context: format!(
                                    "Position {} from end exceeds {} available columns",
                                    ordinal.position,
                                    candidates.len()
                                ),
                            });
                        }
                        candidates.len() - ordinal.position as usize
                    } else {
                        if ordinal.position == 0 {
                            return Err(DelightQLError::ColumnNotFoundError {
                                column: "|0|".to_string(),
                                context: "Column positions start at 1".to_string(),
                            });
                        }
                        let pos = (ordinal.position - 1) as usize;
                        if pos >= candidates.len() {
                            return Err(DelightQLError::ColumnNotFoundError {
                                column: format!("|{}|", ordinal.position),
                                context: format!(
                                    "Position {} exceeds {} available columns",
                                    ordinal.position,
                                    candidates.len()
                                ),
                            });
                        }
                        pos
                    };

                    let column = candidates[idx];
                    let col_name = column.name().to_string();
                    let resolved_arg = if let Some(ref qual) = ordinal.qualifier {
                        format!("{}.{}", qual, col_name)
                    } else if let ast_resolved::TableName::Named(ref t) = column.fq_table.name {
                        format!("{}.{}", t, col_name)
                    } else {
                        col_name
                    };

                    if i < arguments.len() {
                        arguments[i] = resolved_arg;
                    }
                }
            }
        }
    }

    // Normal TVF resolution.
    // Known TVFs (json_each, pragma_table_info) get hardcoded schemas.
    // Unknown TVFs get CprSchema::Unknown — columns discovered at runtime.
    let schema = get_tvf_schema(&function, alias.as_deref());

    // Handle unknown TVFs based on config
    if matches!(schema, ast_resolved::CprSchema::Unknown) {
        if config.permissive || config.transpile_only {
            // Generate warning (unless transpile-only)
            if config.permissive && !config.transpile_only {
                eprintln!(
                    "WARNING: Unknown TVF '{}' - treating as generic table function",
                    function
                );
            }
            // Keep Unknown schema
        } else {
            return Err(DelightQLError::parse_error(format!(
                "Unknown TVF: {}",
                function
            )));
        }
    }

    let state = match &schema {
        ast_resolved::CprSchema::Resolved(cols) => BubbledState::resolved(cols.clone()),
        ast_resolved::CprSchema::Unknown => BubbledState::resolved(vec![]), // Unknown schema means no columns for validation
        other => panic!(
            "catch-all hit in relation_resolver.rs resolve_ground_relation (CprSchema): {:?}",
            other
        ),
    };

    // Resolve namespace to physical backend schema + connection routing.
    // Same logic as Ground passthrough: resolve namespace, track connection_id,
    // replace DQL namespace path with physical schema name for SQL generation.
    let resolved_namespace = if let Some(ref ns) = namespace {
        if !ns.is_empty() {
            match registry.database.resolve_namespace(ns) {
                Ok(Some((physical_schema, conn_id))) => {
                    registry.track_connection_id(conn_id);
                    // physical_schema=None means tables are in `main` of that connection
                    physical_schema.map(|s| NamespacePath::single(&*s))
                }
                _ => namespace,
            }
        } else {
            namespace
        }
    } else {
        None
    };

    let resolved = ast_resolved::Relation::TVF {
        function,
        arguments,
        domain_spec: preserve_domain_spec(&domain_spec)?,
        alias,
        namespace: resolved_namespace,
        grounding: None,
        cpr_schema: ast_resolved::PhaseBox::new(schema),
        argument_groups: None,
        first_parens_spec: None,
    };

    Ok((
        ast_resolved::RelationalExpression::Relation(resolved),
        state,
    ))
}

/// Resolve an InnerRelation variant (subquery inside parentheses).
///
/// INNER-RELATION: table(|> pipeline) or table(, correlation |> pipeline)
/// Resolves the subquery and keeps pattern as Indeterminate.
/// The refiner will classify it into UDT/CDT-SJ/CDT-GJ/CDT-WJ.
fn resolve_inner_relation(
    rel: ast_unresolved::Relation,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    let ast_unresolved::Relation::InnerRelation {
        pattern,
        alias,
        outer,
        ..
    } = rel
    else {
        unreachable!("resolve_inner_relation called with non-InnerRelation variant");
    };

    // Extract identifier and subquery from the pattern
    let (identifier, subquery) = match pattern {
        ast_unresolved::InnerRelationPattern::Indeterminate {
            identifier,
            subquery,
            ..
        } => (identifier, subquery),
        _ => {
            return Err(crate::error::DelightQLError::ParseError {
                message: "Expected Indeterminate pattern from builder".to_string(),
                source: None,
                subcategory: None,
            });
        }
    };

    // Resolve the inner subquery.
    // Using pipes inside the subquery are converted to correlation Filters
    // automatically by the Pipe handler in resolve_relational_expression_with_registry
    // when outer_context is available — no per-site extraction needed.
    let (resolved_subquery, bubbled) = super::resolve_relational_expression_with_registry(
        (*subquery).clone(),
        registry,
        outer_context,
        config,
        grounding,
    )?;

    // Extract schema from resolved subquery
    let schema = super::helpers::extraction::extract_cpr_schema(&resolved_subquery)?;

    // Relabel columns with the inner relation's effective name (alias if present, otherwise identifier)
    // This ensures qualified globs like `users.*` or `u.*` can match these columns
    let effective_name = alias.as_deref().unwrap_or(&identifier.name);
    let schema = super::helpers::extraction::transform_schema_table_names(schema, effective_name);

    // Also relabel the bubbled state's i_provide columns so the join sees the correct table names
    let relabeled_i_provide: Vec<ast_resolved::ColumnMetadata> = bubbled
        .i_provide
        .into_iter()
        .map(|mut col| {
            col.fq_table.name = ast_resolved::TableName::Named(effective_name.to_string().into());
            col
        })
        .collect();
    let bubbled = super::BubbledState::resolved(relabeled_i_provide);

    // Create resolved InnerRelation with Indeterminate pattern
    // Refiner will classify this later
    let resolved = ast_resolved::Relation::InnerRelation {
        pattern: ast_resolved::InnerRelationPattern::Indeterminate {
            identifier: convert_qualified_name(identifier),
            subquery: Box::new(resolved_subquery),
        },
        alias,
        outer,
        cpr_schema: ast_resolved::PhaseBox::new(schema),
    };

    Ok((
        ast_resolved::RelationalExpression::Relation(resolved),
        bubbled,
    ))
}

/// Relabel the `i_provide` columns of a BubbledState with a new table name.
///
/// Consulted entities (facts and views) resolve their bodies internally, producing
/// columns with the entity's original table name. When the entity is aliased
/// (e.g., `country_tier(*) as ct`), downstream pipes need `i_provide` columns
/// to carry the alias so qualified refs like `ct.Country` can match.
/// Relabel a CprSchema's column table names with an alias.
///
/// Convert a resolved Query back to a RelationalExpression for HO view expansion.
///
/// When the HO view body has no CTEs, the Query is unwrapped transparently.
/// When it has CTEs, it's wrapped in a ConsultedView to provide a subquery boundary.
pub(super) fn ho_view_query_to_relational(
    resolved_query: ast_resolved::Query,
    bubbled: super::BubbledState,
    view_name: &str,
) -> crate::error::Result<(ast_resolved::RelationalExpression, super::BubbledState)> {
    match resolved_query {
        ast_resolved::Query::Relational(expr) => Ok((expr, bubbled)),
        query_with_ctes => {
            let body_schema =
                super::helpers::extraction::extract_cpr_schema_from_query(&query_with_ctes)?;
            let alias: SqlIdentifier = crate::pipeline::transformer_v3::next_alias().into();
            let bubbled = relabel_bubbled_with_alias(bubbled, &alias);
            let scoped = ast_resolved::ScopedSchema::bind(body_schema, alias.clone());
            Ok((
                ast_resolved::RelationalExpression::Relation(
                    ast_resolved::Relation::ConsultedView {
                        identifier: ast_resolved::QualifiedName {
                            namespace_path: NamespacePath::empty(),
                            name: view_name.to_string().into(),
                            grounding: None,
                        },
                        body: Box::new(query_with_ctes),
                        scoped: ast_resolved::PhaseBox::new(scoped),
                        outer: false,
                    },
                ),
                bubbled,
            ))
        }
    }
}

pub(super) fn relabel_bubbled_with_alias(
    bubbled: super::BubbledState,
    effective_name: &str,
) -> super::BubbledState {
    let relabeled: Vec<ast_resolved::ColumnMetadata> = bubbled
        .i_provide
        .into_iter()
        .map(|mut col| {
            col.fq_table.name = ast_resolved::TableName::Named(effective_name.to_string().into());
            col
        })
        .collect();
    super::BubbledState::resolved(relabeled)
}

/// Unified HO view expansion: handles both direct and piped invocations.
///
/// Uses PatternResolver for first-parens (scalar params) instead of per-clause
/// pre-filtering. The squished relation includes ALL clauses; PatternResolver
/// applies WHERE constraints from call-site literals.
///
/// Validate that scalar expressions at MixedGround positions are ground values,
/// not unbound identifiers. MixedGround positions have free variables in some
/// clauses — the caller must provide a literal or expression, not a bare lvar.
fn validate_scalar_spec_mixed_ground(
    scalar_spec: &ast_unresolved::DomainSpec,
    positions: &[crate::pipeline::asts::ddl::HoPositionInfo],
    function: &str,
    has_outer_context: bool,
) -> Result<()> {
    use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode};

    let exprs = match scalar_spec {
        ast_unresolved::DomainSpec::Positional(exprs) => exprs,
        ast_unresolved::DomainSpec::Glob => return Ok(()),
        _ => return Ok(()),
    };

    // Get scalar positions from position analysis
    let scalar_positions: Vec<_> = positions
        .iter()
        .enumerate()
        .filter(|(_, p)| matches!(p.column_kind, HoColumnKind::Scalar))
        .collect();

    for (idx, expr) in exprs.iter().enumerate() {
        let Some((abs_pos, pos_info)) = scalar_positions.get(idx) else {
            continue;
        };
        if pos_info.ground_mode != HoGroundMode::MixedGround {
            continue;
        }
        // Check if the expression is a bare identifier (lvar) — not a literal
        let is_ground = match expr {
            ast_unresolved::DomainExpression::Literal { .. } => true,
            ast_unresolved::DomainExpression::Lvar { .. } => false,
            ast_unresolved::DomainExpression::Projection(_) => false,
            // Function calls, expressions, etc. are considered ground
            _ => true,
        };

        if !is_ground && !has_outer_context {
            let expr_text = format!("{:?}", expr);
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "ho/unbound-mixed-param",
                format!(
                    "Unbound scalar at MixedGroundParam position {} of HO view '{}'. \
                     This position has free variables in some clauses — the caller must \
                     provide a ground value (literal or expression), not a bare identifier. \
                     Got: {}",
                    abs_pos, function, expr_text
                ),
                "HO parameter validation",
            ));
        }
    }
    Ok(())
}

/// Logic:
/// 1. Build pipe source CTE if pipe_source is Some
/// 2. Call build_squished_relation() → unresolved Query with all clauses
/// 3. Activate namespace-local enlists
/// 4. resolve_query_inline(squished_query, ...) → resolved ConsultedView
/// 5. Deactivate namespace-local enlists
/// 6. ho_view_query_to_relational() → ConsultedView + BubbledState
/// 7. apply_call_site_pattern(scalar_spec, resolved_expr, schema, ...) for scalar filtering
pub(super) fn expand_ho_view(
    function: &str,
    entity: &crate::resolution::registry::ConsultedEntity,
    scalar_spec: &ast_unresolved::DomainSpec,
    table_bindings: crate::pipeline::query_features::HoParamBindings,
    pipe_source: Option<ast_unresolved::RelationalExpression>,
    data_ns: Option<&ast_unresolved::NamespacePath>,
    grounding: &ast_unresolved::GroundedPath,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
) -> Result<(ast_resolved::RelationalExpression, super::BubbledState)> {
    log::debug!(
        "Expanding HO view '{}' (unified) from namespace '{}'",
        function,
        entity.namespace,
    );

    // Validate arity for argumentative params that received table references.
    super::grounding::validate_argumentative_arity(&table_bindings, registry)?;

    // Validate mixed ground params from position analysis.
    let defs = crate::ddl::ddl_builder::build_ddl_file(&entity.definition).unwrap_or_default();
    let positions = if !entity.positions.is_empty() {
        entity.positions.clone()
    } else {
        super::grounding::build_ho_position_analysis(&defs)
    };
    let positions = super::grounding::ensure_position_column_names(positions, &defs);

    // Validate scalar_spec against positions: reject unbound identifiers at MixedGround positions.
    validate_scalar_spec_mixed_ground(
        scalar_spec,
        &positions,
        function,
        pipe_source.is_some() || outer_context.is_some(),
    )?;

    // Build pipe source CTE if piped
    let pipe_source_cte = pipe_source.map(|source| ("_ho_pipe_src".to_string(), source));

    // Build the squished relation (ALL clauses, no pre-filtering)
    let squished_query = super::grounding::build_squished_relation(
        function,
        entity,
        table_bindings,
        pipe_source_cte,
        data_ns,
    )?;

    // Activate namespace-local enlists and aliases
    let activated_enlists = registry
        .consult
        .activate_namespace_local_enlists(&entity.namespace);
    let activated_aliases = registry
        .consult
        .activate_namespace_local_aliases(&entity.namespace);

    let resolve_result = super::resolve_query_inline(
        squished_query,
        registry,
        outer_context,
        config,
        Some(grounding),
    );

    registry
        .consult
        .deactivate_namespace_local_aliases(&activated_aliases);
    registry
        .consult
        .deactivate_namespace_local_enlists(&activated_enlists);

    let (resolved_query, bubbled) = resolve_result?;

    // Convert to ConsultedView relation
    let (resolved_expr, bubbled) =
        ho_view_query_to_relational(resolved_query, bubbled, function)?;

    // Apply PatternResolver to first-parens (scalar positions) via combined DomainSpec.
    //
    // The squished relation has schema [output_cols..., scalar_cols...] (glob-head)
    // or [scalar_cols..., output_cols...] (argumentative-head).
    // We build a combined DomainSpec covering ALL columns:
    //   - Scalar positions: expressions from scalar_spec (Literal → WHERE, Lvar → rename)
    //   - Output positions: pass-through Lvars (keep original name)
    // One PatternResolver call handles everything.
    if matches!(scalar_spec, ast_unresolved::DomainSpec::Glob) {
        return Ok((resolved_expr, bubbled));
    }

    let body_schema = super::helpers::extraction::extract_cpr_schema(&resolved_expr)?;
    let scalar_exprs = match scalar_spec {
        ast_unresolved::DomainSpec::Positional(exprs) => exprs,
        _ => return Ok((resolved_expr, bubbled)),
    };

    // Identify scalar column names from position analysis
    let scalar_col_names: Vec<Option<&str>> = positions
        .iter()
        .filter(|p| matches!(p.column_kind, crate::pipeline::asts::ddl::HoColumnKind::Scalar))
        .map(|p| p.column_name.as_deref())
        .collect();


    // Build WHERE constraints and column filtering for scalar positions.
    // We construct the filter directly rather than going through apply_call_site_pattern,
    // because HO ConsultedViews get CTE-wrapped and the qualifier would be wrong.
    let schema_cols = match &body_schema {
        ast_resolved::CprSchema::Resolved(cols) => cols,
        _ => return Ok((resolved_expr, bubbled)),
    };

    let mut where_constraints = Vec::new();
    let mut output_columns = Vec::new();
    let mut scalar_idx = 0;

    for col in schema_cols {
        let col_name = col.name();
        let is_scalar = scalar_col_names
            .iter()
            .any(|n| n.map_or(false, |n| n == col_name));
        if is_scalar && scalar_idx < scalar_exprs.len() {
            let scalar_expr = &scalar_exprs[scalar_idx];
            scalar_idx += 1;
            match scalar_expr {
                ast_unresolved::DomainExpression::Literal { value, .. } => {
                    // Literal → WHERE constraint + hide column (hygienic)
                    // Use unqualified column ref to avoid qualifier mismatch with CTE wrapping
                    let col_ref = ast_resolved::DomainExpression::Lvar {
                        name: col_name.into(),
                        qualifier: None,
                        namespace_path: ast_resolved::NamespacePath::empty(),
                        alias: None,
                        provenance: ast_resolved::PhaseBox::phantom(),
                    };
                    let lit_val = ast_resolved::DomainExpression::Literal {
                        value: value.clone(),
                        alias: None,
                    };
                    where_constraints.push(ast_resolved::BooleanExpression::Comparison {
                        operator: "traditional_eq".to_string(),
                        left: Box::new(col_ref),
                        right: Box::new(lit_val),
                    });
                    // Hide scalar column (don't add to output)
                    let mut hidden = col.clone();
                    hidden.needs_hygienic_alias = true;
                    output_columns.push(hidden);
                }
                ast_unresolved::DomainExpression::Lvar { name, .. } => {
                    // Lvar → rename column (project with new name)
                    let mut renamed = col.clone();
                    renamed.info = renamed.info.with_alias(name.to_string());
                    renamed.needs_sql_rename = true;
                    output_columns.push(renamed);
                }
                ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
                    // Underscore → skip (hide) column
                    let mut hidden = col.clone();
                    hidden.needs_hygienic_alias = true;
                    output_columns.push(hidden);
                }
                _ => {
                    // Pass through
                    output_columns.push(col.clone());
                }
            }
        } else {
            output_columns.push(col.clone());
        }
    }

    // Update schema on the inner relation
    let mut expr = resolved_expr;
    update_relation_cpr_schema(&mut expr, &output_columns);

    // Wrap in Filter if there are WHERE constraints
    if !where_constraints.is_empty() {
        let combined = combine_where_constraints(where_constraints);
        expr = ast_resolved::RelationalExpression::Filter {
            source: Box::new(expr),
            condition: ast_resolved::SigmaCondition::Predicate(combined),
            origin: ast_resolved::FilterOrigin::HoGroundScalar {
                source_table: function.to_string(),
            },
            cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                output_columns.clone(),
            )),
        };
    }

    Ok((expr, BubbledState::resolved(output_columns)))
}


/// Apply call-site positional patterns to an already-resolved consulted entity expression.
///
/// When a consulted view/fact is invoked with positional args (e.g., `active_users(1, fn, ln, ...)`),
/// the call-site domain_spec specifies column selection, renaming, and literal filtering.
/// This function applies those patterns on top of the resolved body expression — the same
/// work that `apply_pattern_resolver` does for Ground tables, but for ConsultedView/InnerRelation.
fn apply_call_site_pattern(
    domain_spec: &ast_unresolved::DomainSpec,
    expr: ast_resolved::RelationalExpression,
    body_schema: &ast_resolved::CprSchema,
    entity_name: &str,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    // Get base columns and relabel with entity_name so WHERE constraints
    // reference the correct alias (e.g., "t0"."id" = 1)
    let base_cols = match body_schema {
        ast_resolved::CprSchema::Resolved(cols) => cols
            .iter()
            .map(|col| {
                let mut relabeled = col.clone();
                relabeled.fq_table.name =
                    ast_resolved::TableName::Named(entity_name.to_string().into());
                relabeled
            })
            .collect::<Vec<_>>(),
        _ => {
            return Err(DelightQLError::validation_error(
                format!(
                    "Cannot apply positional pattern to '{}': schema not resolved",
                    entity_name
                ),
                "Pattern application".to_string(),
            ));
        }
    };

    let pattern_resolver = PatternResolver::new();
    let join_context = outer_context.map(JoinContext::from);

    let pattern_result = pattern_resolver.resolve_pattern(
        domain_spec,
        &base_cols,
        entity_name,
        join_context.as_ref(),
    )?;

    let mut output_columns = pattern_result.output_columns;

    // Mark columns that were renamed by this call-site pattern.
    // This distinguishes call-site renames (need SELECT wrapper in transformer)
    // from body-internal renames (already baked into body SQL).
    // Compare against base_cols to detect only NEW renames from the call-site.
    for col in &mut output_columns {
        if col.needs_hygienic_alias {
            continue;
        }
        if let (Some(orig), Some(alias)) = (col.info.original_name(), col.info.alias_name()) {
            if orig != alias {
                // Only flag if this rename is NEW — not already present in body schema
                let was_already_renamed = base_cols.iter().any(|bc| {
                    bc.info.original_name() == Some(orig) && bc.info.alias_name() == Some(alias)
                });
                if !was_already_renamed {
                    col.needs_sql_rename = true;
                }
            }
        }
    }

    // Update cpr_schema on the inner relation to reflect column selection/renaming
    let mut expr = expr;
    update_relation_cpr_schema(&mut expr, &output_columns);

    // Wrap in Filter if there are WHERE constraints from literal patterns
    if !pattern_result.where_constraints.is_empty() {
        let combined = combine_where_constraints(pattern_result.where_constraints);
        expr = ast_resolved::RelationalExpression::Filter {
            source: Box::new(expr),
            condition: ast_resolved::SigmaCondition::Predicate(combined),
            origin: ast_resolved::FilterOrigin::PositionalLiteral {
                source_table: entity_name.to_string(),
            },
            cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                output_columns.clone(),
            )),
        };
    }

    if !pattern_result.join_conditions.is_empty() {
        log::debug!(
            "Storing {} join conditions from positional pattern for consulted entity {}",
            pattern_result.join_conditions.len(),
            entity_name
        );
    }

    Ok((expr, BubbledState::resolved(output_columns)))
}

/// Combine multiple WHERE constraints into a single AND chain.
fn combine_where_constraints(
    constraints: Vec<ast_resolved::BooleanExpression>,
) -> ast_resolved::BooleanExpression {
    debug_assert!(!constraints.is_empty());
    if constraints.len() == 1 {
        constraints.into_iter().next().unwrap()
    } else {
        constraints
            .into_iter()
            .reduce(|left, right| ast_resolved::BooleanExpression::And {
                left: Box::new(left),
                right: Box::new(right),
            })
            .unwrap()
    }
}

/// Update the cpr_schema on a relation expression (ConsultedView or InnerRelation).
fn update_relation_cpr_schema(
    expr: &mut ast_resolved::RelationalExpression,
    new_columns: &[ast_resolved::ColumnMetadata],
) {
    if let ast_resolved::RelationalExpression::Relation(rel) = expr {
        match rel {
            ast_resolved::Relation::ConsultedView { scoped, .. } => {
                let new_schema = ast_resolved::CprSchema::Resolved(new_columns.to_vec());
                let alias = scoped.get().alias().clone();
                *scoped = ast_resolved::PhaseBox::new(ast_resolved::ScopedSchema::bind(
                    new_schema, alias,
                ));
            }
            ast_resolved::Relation::InnerRelation { cpr_schema, .. } => {
                *cpr_schema = ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                    new_columns.to_vec(),
                ));
            }
            other => panic!(
                "catch-all hit in relation_resolver.rs update_scoped_schema: {:?}",
                other
            ),
        }
    }
}

/// Parse a fact's body_source (which may contain multiple fact clauses joined
/// by newlines) and merge all rows into a single anonymous table expression.
fn expand_fact_body(
    body_source: &str,
    fact_name: &str,
) -> Result<ast_unresolved::RelationalExpression> {
    use crate::pipeline::asts::core::expressions::relational::Relation;
    use crate::pipeline::asts::core::phase_box::PhaseBox;

    let defs = crate::ddl::ddl_builder::build_ddl_file(body_source)?;
    if defs.is_empty() {
        return Err(DelightQLError::parse_error(format!(
            "No definitions found for fact '{}'",
            fact_name
        )));
    }

    let mut all_rows = Vec::new();
    let mut headers = None;

    for def in defs {
        let rel_expr = def.into_flat_relational_expr().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Expected relational body for fact '{}', got scalar",
                fact_name
            ))
        })?;

        // Extract the Anonymous relation from the relational expression
        if let ast_unresolved::RelationalExpression::Relation(Relation::Anonymous {
            column_headers,
            rows,
            ..
        }) = rel_expr
        {
            if headers.is_none() {
                headers = column_headers;
            }
            all_rows.extend(rows);
        } else {
            return Err(DelightQLError::parse_error(format!(
                "Expected anonymous table body for fact '{}', got other expression",
                fact_name
            )));
        }
    }

    Ok(ast_unresolved::RelationalExpression::Relation(
        Relation::Anonymous {
            column_headers: headers,
            rows: all_rows,
            alias: Some(fact_name.into()),
            outer: false,
            exists_mode: false,
            qua_target: None,
            cpr_schema: PhaseBox::phantom(),
        },
    ))
}
