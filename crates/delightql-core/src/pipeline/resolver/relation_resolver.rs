// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Relation resolution logic
//!
//! This module handles the resolution of base relations and relational calls.
//! and pattern application for positional patterns.
use crate::pipeline::asts::core::literals::column_ordinal_text;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};

use super::tvf::get_tvf_schema;
use super::type_conversion::{convert_domain_expression, convert_qualified_name};
use super::{BubbledState, JoinContext, PatternResolver, ResolutionConfig};
use crate::enums::EntityType as BootstrapEntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::NamespacePath;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{NamedReference, Reference};
use delightql_types::SqlIdentifier;

pub(super) fn bind_physical_relation(
    scope: crate::names::ScopeId,
    canonical: Option<&SqlIdentifier>,
    backend_schema: Option<&str>,
    identities: &crate::names::Registry,
) -> Result<()> {
    let Some(entity) = identities.entity_of_scope(scope) else {
        return Err(DelightQLError::parse_error(
            "A physical relation heading has no catalog entity identity",
        ));
    };
    let canonical = canonical.map(|name| identities.intern(name.as_str(), name.is_stropped()));
    let backend_schema = backend_schema.map(|name| identities.intern(name, false));
    identities.bind_entity_physical(entity, canonical, backend_schema);
    Ok(())
}

/// Resolve a relation-access shape that has no heading available for binding.
///
/// Glob shapes carry no slot expressions. A positional shape needs a source
/// heading so each authored slot can become the occurrence it binds.
pub(super) fn resolve_schema_free_access(
    spec: &ast_unresolved::Access,
) -> Result<ast_resolved::Access> {
    match spec {
        ast_unresolved::Access::All => Ok(ast_resolved::Access::All),
        ast_unresolved::Access::Unasked => Ok(ast_resolved::Access::Unasked),
        ast_unresolved::Access::Dequalify(columns) => {
            Ok(ast_resolved::Access::Dequalify(columns.clone()))
        }
        ast_unresolved::Access::DequalifyAll => Ok(ast_resolved::Access::DequalifyAll),
        ast_unresolved::Access::Slots(_) => Err(DelightQLError::validation_error(
            "A positional relation access requires a resolved heading",
            "Positional pattern resolution",
        )),
    }
}

/// Mint a distinct resolver-phase occurrence for an unaliased access.
fn resolver_scope(identities: &crate::names::Registry, entity_name: &str) -> crate::names::ScopeId {
    let spelling = identities.intern(entity_name, false);
    let entity = identities.mint_entity(spelling);
    identities.mint_derived_scope(
        crate::names::ScopeOrigin::Resolution { of: entity },
        crate::names::Hint::Prefix("resolver"),
    )
}

/// Compute an effective alias: use the user-supplied alias if present,
/// otherwise retain the authored access spelling while a fresh structural
/// occurrence provides SQL alias hygiene at baptism.
fn compute_effective_alias(
    alias: &Option<SqlIdentifier>,
    identities: &crate::names::Registry,
    entity_name: &str,
) -> (SqlIdentifier, Option<crate::names::ScopeId>) {
    if let Some(a) = alias.clone() {
        (a, None)
    } else {
        let scope = resolver_scope(identities, entity_name);
        (entity_name.into(), Some(scope))
    }
}

/// The access-boundary export for a consulted view/fact.
///
/// A view's lvars are a function of how it is CALLED, not of how its body
/// spelled them, and the access name — the user's alias, or the bare view name
/// — is what qualifies them. But the name a column answers to and the name it
/// is published under are two facts, not one: exporting a column that answers
/// ONLY to the access name makes `v(*), name = "x"` unaddressable, because
/// nothing published `name` any more. So the export is `BareAnswering`: the
/// column keeps its published name and is ALSO reachable under the access
/// name, which is the pairing full-name unification needs.
///
/// What still does not cross is the caller's own argumentative binding —
/// `declared_bare` belongs to the call site and never leaks through the entity
/// boundary. The SQL occurrence stays distinct (hygiene: self-joins need
/// distinct aliases), while the access name rides the metadata rather than
/// naming that occurrence.
/// Say where a consulted body failed WITHOUT taking its badge away.
///
/// A body is compiled by the ordinary machinery, so its refusals are the
/// ordinary ones and they already carry the identifier that names them. Only
/// an UNBADGED failure gains the view's context here: re-wrapping a
/// categorized refusal as a runtime database error renames a semantic
/// refusal after the fact, and an annotation matching on its badge stops
/// matching (badge hygiene).
fn wrap_view_body_error(
    error: DelightQLError,
    doing: &str,
    view_name: &str,
    view_ns: &str,
) -> DelightQLError {
    if error.error_uri() != "delightql-error://runtime" {
        return error;
    }
    DelightQLError::database_error(
        format!("Error while {doing} pre-grounded view '{view_name}' (from namespace '{view_ns}'): {error}"),
        error.to_string(),
    )
}

fn access_boundary_export(
    alias: &Option<SqlIdentifier>,
    entity_name: &str,
    input: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> (SqlIdentifier, crate::names::ScopeId) {
    let (effective, resolver_id) = compute_effective_alias(alias, identities, entity_name);
    let access_name: SqlIdentifier = match alias.clone() {
        Some(a) => a,
        None => entity_name.into(),
    };
    let scope = resolver_id.unwrap_or_else(|| {
        let hint = identities.intern(access_name.as_str(), access_name.is_stropped());
        identities.mint_derived_scope(
            crate::names::ScopeOrigin::UserAlias { of: input },
            crate::names::Hint::User(hint),
        )
    });
    let access =
        identities.canonical(identities.intern(access_name.as_str(), access_name.is_stropped()));
    for column in identities.heading(input).columns_seen() {
        crate::probe::probe!(
            reconcile,
            "boundary export {:?} {:?} -> {:?}",
            column,
            identities.addressing(column),
            scope
        );
        // A hygienic carrier stops at the boundary. The slot it stands
        // for introduced no name — a ground term, a repeat, `_` — so
        // it publishes nothing, and it exists only to be read by a
        // WHERE inside the body it came from. Exporting it hands it
        // the access name, which publishes an internal column AND
        // puts a target in the heading that no inner column can
        // answer. A head that names its columns already drops it
        // before this point; a glob head must reach the same heading.
        if identities.addressing(column) == crate::names::Addressing::Hygienic {
            continue;
        }
        identities.republish_column(
            column,
            scope,
            crate::names::Republish::BoundaryExport,
            identities.published(column),
            crate::names::Addressing::BareAnswering(access),
            |_| {},
        );
    }
    (effective, scope)
}

/// Helper to apply PatternResolver for column selection
pub(super) fn apply_pattern_resolver(
    access: &ast_unresolved::Access,
    base_cols: &[crate::names::ColId],
    table_name: &str,
    registry: &crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    formal_frame: Option<&super::FormalFrame>,
    resolution_scope: Option<&str>,
    instantiation_depth: &super::InstantiationDepth,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    // VALIDATE: slot pattern length must match table columns
    if let ast_unresolved::Access::Slots(patterns) = access {
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

    // DequalifyAll: no per-column validation needed (shared cols computed at join time)
    // — just proceed to pattern resolver which treats it like Glob

    // VALIDATE: Dequalify columns must exist in the table
    if let ast_unresolved::Access::Dequalify(using_cols) = access {
        for col_name in using_cols {
            // As written: the dequalifying step names the column the author
            // spelled, and this is the read that decides whether it is there.
            let spelling = registry
                .identities
                .intern(col_name.as_str(), col_name.is_stropped());
            let name = registry.identities.canonical(spelling);
            let exists = base_cols
                .iter()
                .any(|column| registry.identities.published_sym(*column) == Some(name));
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
    let pattern_resolver = PatternResolver::with_formals(
        formal_frame,
        Some(super::SlotInstantiation {
            scoped_cfes: &registry.query_local.scoped_cfes,
            consult: &registry.consult,
            lookup_scope: resolution_scope,
            depth: instantiation_depth,
        }),
    );

    // Convert outer_context to JoinContext if present
    let join_context = outer_context.map(JoinContext::from);

    match pattern_resolver.resolve_pattern(
        access,
        base_cols,
        table_name,
        join_context.as_ref(),
        &registry.identities,
    ) {
        Ok(pattern_result) => {
            let output_scope = pattern_result.output_scope;
            let resolved_spec = pattern_result.resolved_spec(access)?;
            let output_columns = pattern_result.output_columns.into_vec();

            // Create the base read. Outerness is the caller's to set:
            // this road resolves the pattern, not the call site.
            let final_expr = super::pattern_resolver::apply_local_constraints(
                ast_resolved::Relation::ground_read(resolved_spec, false, output_scope),
                pattern_result.where_constraints,
                output_scope,
            );

            let mut state = BubbledState::resolved(output_columns.to_vec(), &registry.identities);
            // A relation pattern controls what the relation contributes to
            // the result, but its source columns remain addressable while the
            // surrounding relational expression is being formed. This is
            // what permits `materials(*.(id))` to use `materials.id` in an
            // attached join predicate without leaking `id` into the output.
            // A following pipe remains the scope barrier and discards these
            // source qualifiers.
            state.qualifier_scope.clear();
            for column in base_cols {
                let scope = registry.identities.scope_of(*column);
                if !state.qualifier_scope.contains(&scope) {
                    state.qualifier_scope.push(scope);
                }
            }
            Ok((final_expr, state))
        }
        // A pattern that cannot be resolved REFUSES. It must never widen to
        // the full base schema: the pattern is the user's statement of what
        // the relation contributes, so publishing every column instead
        // returns wrong data for a query that looks correct — the one
        // failure mode this family is named for.
        Err(e) => Err(e),
    }
}

// resolve_relation_with_registry — DELETED (Step 0f). Dispatch absorbed into
// ResolverFold::resolve_relation_impl (resolver_fold.rs).

/// Resolve a compiler-owned relation read by identity. Its producer has
/// already published the heading, so no query-local spelling lookup
/// participates.
pub(super) fn resolve_plan_scope(
    rel: ast_unresolved::Relation,
    access: ast_unresolved::Access,
    registry: &crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    formal_frame: Option<&super::FormalFrame>,
    config: &ResolutionConfig,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    let ast_unresolved::Relation::Ground {
        mention:
            ast_unresolved::GroundMention::Plan {
                scope,
                authored_name,
                alias,
            },
        outer,
        cpr_schema: _,
    } = rel
    else {
        unreachable!("resolve_plan_scope called with a different relation")
    };
    if !matches!(
        registry.identities.origin_of(scope),
        crate::names::ScopeOrigin::Scratch { .. } | crate::names::ScopeOrigin::HoCarrier { .. }
    ) {
        return Err(DelightQLError::validation_error(
            "A plan-scope relation must refer to compiler-owned storage",
            "compiler relation identity",
        ));
    }
    let source_columns = registry.identities.known_heading(scope)?;
    if source_columns.is_empty() {
        return Err(DelightQLError::validation_error(
            "A plan-scope relation was read before its heading was published",
            "compiler relation identity",
        ));
    }
    let source_expr = ast_resolved::Relation::ground_read(ast_resolved::Access::All, false, scope);

    let Some(authored_name) = authored_name else {
        if !matches!(access, ast_unresolved::Access::All) || alias.is_some() || outer {
            return Err(DelightQLError::validation_error(
                "A direct plan-scope read cannot carry user access metadata",
                "effect plan identity",
            ));
        }
        return Ok((
            source_expr,
            BubbledState::resolved(source_columns.to_vec(), &registry.identities),
        ));
    };

    // An argumentative declaration states exact width, and a positional
    // pattern on a carrier is that declaration applied — so a width mismatch
    // refuses under the same param-arity category the by-name binding road
    // uses, before any occurrence is minted.
    if let ast_unresolved::Access::Slots(patterns) = &access {
        if matches!(
            registry.identities.origin_of(scope),
            crate::names::ScopeOrigin::HoCarrier { .. }
        ) {
            let visible = registry
                .identities
                .known_heading(scope)?
                .iter()
                .filter(|column| {
                    registry.identities.addressing(**column) != crate::names::Addressing::Hygienic
                })
                .count();
            if patterns.len() != visible {
                return Err(DelightQLError::validation_error_categorized(
                    "constraint/ho_param/argumentative_functor/arity",
                    format!(
                        "Positional pattern incomplete - rule '{}' has {} columns but pattern specifies {} elements",
                        authored_name,
                        visible,
                        patterns.len(),
                    ),
                    "HO parameter arity mismatch",
                ));
            }
        }
    }

    // A redirected authored access has two identities: the scratch object
    // in the inner FROM and the caller-facing occurrence outside it. Keeping
    // them as nested relations prevents pattern resolution from replacing
    // the physical scratch scope with an authored alias scope.
    let access_name = alias.clone().unwrap_or_else(|| authored_name.clone());
    let access_spelling = registry
        .identities
        .intern(access_name.as_str(), access_name.is_stropped());
    let access_sym = registry.identities.canonical(access_spelling);
    let access_scope = registry.identities.mint_derived_scope(
        crate::names::ScopeOrigin::UserAlias { of: scope },
        crate::names::Hint::User(access_spelling),
    );
    for column in source_columns {
        registry.identities.republish_column(
            column,
            access_scope,
            crate::names::Republish::BoundaryExport,
            registry.identities.published(column),
            crate::names::Addressing::BareAnswering(access_sym),
            |_| {},
        );
    }
    let access_expr = ast_resolved::Chain::relation(ast_resolved::Relation::InnerRelation {
        pattern: ast_resolved::InnerRelationPattern::Indeterminate {
            identifier: ast_resolved::QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: authored_name.clone(),
            },
            subquery: Box::new(source_expr),
        },
        preminted_scope: Some(access_scope),
        alias: Some(access_name.clone()),
        outer,
        cpr_schema: access_scope,
    });

    if matches!(access, ast_unresolved::Access::All) {
        let columns = registry.identities.known_heading(access_scope)?;
        return Ok((
            access_expr,
            BubbledState::resolved(columns.to_vec(), &registry.identities),
        ));
    }

    apply_call_site_pattern(
        &access,
        access_expr,
        access_scope,
        access_name.as_str(),
        authored_name.as_str(),
        outer_context,
        &registry.identities,
        formal_frame,
        Some(super::SlotInstantiation {
            scoped_cfes: &registry.query_local.scoped_cfes,
            consult: &registry.consult,
            lookup_scope: config.resolution_namespace.as_deref(),
            depth: &config.instantiation_depth,
        }),
    )
}

/// Resolve a Ground relation variant (named table, view, CTE, or consulted entity).
///
/// This handles passthrough tables, grounded entities, namespace-qualified tables,
/// unqualified tables, CTEs, consulted views/facts, and unknown entities.
pub(super) fn resolve_ground(
    rel: ast_unresolved::Relation,
    access: ast_unresolved::Access,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    use crate::resolution::{resolve_entity_with_alias, EntityDefinition, ResolutionResult};

    let ast_unresolved::Relation::Ground {
        mention:
            ast_unresolved::GroundMention::Named {
                identifier,
                alias,
                mutation_target,
                passthrough,
            },
        outer,
        cpr_schema: _,
    } = rel
    else {
        unreachable!("resolve_ground called with non-Ground variant");
    };

    // `!!` is evidence about the relation this access reads, and it belongs
    // to the occurrence the access publishes — recorded below, once the
    // relation kind has been settled and before anything is built on it.
    // Every relation built from that occurrence afterwards carries the
    // evidence, so a name, an alias, a CTE binding or a join arm hands it on
    // instead of leaving a later reader to walk the syntax back to a ground
    // name it may no longer have.
    let marked_relation = mutation_target.then(|| {
        registry
            .identities
            .intern(identifier.name.as_str(), identifier.name.is_stropped())
    });

    // PASSTHROUGH: skip entity catalog, use schema introspector directly.
    if passthrough {
        let resolved = r_resolve_passthrough(
            identifier,
            access,
            alias,
            outer,
            registry,
            outer_context,
            config,
        )?;
        note_mutation_mark(marked_relation, &resolved.0, &registry.identities);
        return Ok(resolved);
    }

    // Check for namespace-qualified tables FIRST
    // Bypass resolve_entity_with_alias for namespace-qualified tables
    // CTEs can't have namespace paths (they're query-local), so this is safe
    let mut serve_bootstrap: Option<ServedBootstrapRead> = None;
    let resolution = if !identifier.namespace_path.is_empty() {
        // Namespace-qualified table (no grounding) - use bootstrap resolution
        match registry
            .database
            .lookup_table_with_namespace_qualified(&identifier.namespace_path, &identifier.name)
        {
            Ok(Some((table_schema, connection_id, canonical_name, bs_opt))) => {
                // Found table at namespace location
                // THE BOOTSTRAP IS A SOURCE, NEVER A TARGET
                // (materialization-law §2): while a materialization source
                // resolves, a bootstrap read is answered as a literal
                // snapshot, and connection 1 never enters the attribution
                // set — exemption is ABSENCE, not a tie-break.
                if connection_id == 1 && config.serve_bootstrap_reads {
                    serve_bootstrap = Some(ServedBootstrapRead {
                        canonical: canonical_name.clone(),
                        backend_schema: bs_opt.clone(),
                        namespace_fq: identifier.namespace_path.fq_string(),
                    });
                } else {
                    // Track connection_id for cross-connection join validation
                    registry.track_connection_id(connection_id);
                }
                ResolutionResult::DatabaseEntity(crate::resolution::EntityInfo {
                    name: identifier.name.clone(),
                    canonical_name: Some(canonical_name),
                    resolved_namespace: Some(identifier.namespace_path.clone()),
                    backend_schema: bs_opt,
                    entity_type: crate::resolution::ResolvedEntityKind::Relation,
                    registry_source: crate::resolution::RegistrySource::Database,
                    schema_source: crate::resolution::SchemaSource::DatabaseCatalog,
                    definition: EntityDefinition::RelationSchema(table_schema),
                })
            }
            Ok(None) => {
                // Not a database table — check consult registry for consulted views
                let fq = identifier.namespace_path.fq_string();
                if let Some(entity) = registry.consult.lookup_entity(
                    &identifier.name,
                    identifier.name.is_stropped(),
                    &fq,
                    config.resolution_namespace.as_deref(),
                ) {
                    crate::resolution::classify_consulted_relation(entity)
                } else if let Some(grounding) = grounding {
                    // Fallback: entity not in patched namespace, search grounded namespaces.
                    // Handles inline DDL views referencing sibling entities: DataNsPatcher
                    // rewrites sample(*) → main::sample(*), but fact lives in scratch ("home").
                    let mut fallback_result = None;
                    for ns in &grounding.grounded_ns {
                        let gfq = ns.fq_string();
                        if let Some(entity) = registry.consult.lookup_entity(
                            &identifier.name,
                            identifier.name.is_stropped(),
                            &gfq,
                            config.resolution_namespace.as_deref(),
                        ) {
                            if matches!(
                                entity.entity_type,
                                BootstrapEntityType::DqlTemporaryViewExpression
                                    | BootstrapEntityType::DqlFactExpression
                            ) {
                                fallback_result = Some(ResolutionResult::ConsultedView {
                                    name: entity.name.clone(),
                                    body_source: entity.definition.clone(),
                                    namespace: gfq,
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
            alias.as_ref(),
            registry,
            config.resolution_namespace.as_deref(),
        )?
    };

    let resolved = match resolution {
        ResolutionResult::CTE(entity_info) => r_resolve_cte(
            entity_info,
            identifier,
            access,
            alias,
            outer,
            registry,
            outer_context,
            config,
        ),
        ResolutionResult::MaterializedRelation(entity_info) => r_resolve_cte(
            entity_info,
            identifier,
            access,
            alias,
            outer,
            registry,
            outer_context,
            config,
        ),
        ResolutionResult::DatabaseEntity(entity_info) => r_resolve_database_entity(
            entity_info,
            identifier,
            access,
            alias,
            outer,
            registry,
            outer_context,
            config,
        ),
        ResolutionResult::ConsultedView {
            name: view_name,
            body_source,
            namespace: view_ns,
        } => r_resolve_consulted_view(
            view_name,
            body_source,
            view_ns,
            access,
            alias,
            outer,
            registry,
            outer_context,
            config,
        ),
        ResolutionResult::DefinedNonRelation { name, entity_type } => {
            Err(defined_non_relation_error(&name, entity_type))
        }
        // THE CATEGORY IS RIGHT AND THE ROAD IS MISSING. Reaching this arm
        // means the executable boundary — which runs before resolution, over
        // the submission's own chains — did not see this occurrence, so the
        // rows were never produced. Refusing here is what keeps a known
        // relation out of the generic-TVF fallback, where its namespace would
        // be stripped and SQL generated against a table that does not exist.
        ResolutionResult::RuntimeServedRelation { name, entity_type } => {
            Err(runtime_served_unreached_error(&name, entity_type))
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
        _ => r_resolve_unknown(identifier),
    }?;
    note_mutation_mark(marked_relation, &resolved.0, &registry.identities);
    let resolved = match serve_bootstrap {
        Some(served) => serve_bootstrap_relation(resolved, served, registry)?,
        None => resolved,
    };
    Ok(resolved)
}

/// A bootstrap read a materialization source resolves: served as rows.
struct ServedBootstrapRead {
    canonical: delightql_types::SqlIdentifier,
    backend_schema: Option<String>,
    namespace_fq: String,
}

/// SERVE THE SNAPSHOT the directive already promises: the catalog rows are
/// read HERE, at plan build, on the bootstrap connection — no engine
/// connection reads another's tables — and the resolved read's head becomes
/// a literal table PUBLISHING THE SAME SCOPE, so every downstream binding,
/// pattern restriction and continuation stands unchanged. The compiled
/// source then executes whole on whatever connection attribution selects,
/// in that connection's own dialect.
#[cfg(not(target_arch = "wasm32"))]
fn serve_bootstrap_relation(
    resolved: (ast_resolved::Chain, BubbledState),
    served: ServedBootstrapRead,
    registry: &mut crate::resolution::EntityRegistry,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    use crate::pipeline::asts::core::{
        AnonRelation, AnonTable, Datum, Grelex, LiteralValue, TabularBody, TabularRow,
    };
    let (mut chain, bubbled) = resolved;
    let Grelex::Reference(ast_resolved::Relation::Ground {
        outer, cpr_schema, ..
    }) = &chain.head
    else {
        return Err(internal_serving_error(
            "a served bootstrap read stands on a ground relation",
        ));
    };
    let outer = *outer;
    let scope = *cpr_schema;

    // The registered column set, in the order the catalog heading was
    // minted from — the same source, so the literal rows align with the
    // scope's own heading. The Arena keeps characters out of reach; the
    // physical names come from the catalog, where they are data.
    let columns: Vec<String> = registry
        .database
        .schema()
        .get_table_columns(Some(&served.namespace_fq), served.canonical.as_str())?
        .ok_or_else(|| {
            internal_serving_error("a served bootstrap table answers its registered columns")
        })?
        .into_iter()
        .map(|column| column.name.to_string())
        .collect();
    let heading = registry.identities.known_heading(scope)?;
    if heading.len() != columns.len() {
        return Err(internal_serving_error(
            "a served bootstrap read publishes the registered heading whole",
        ));
    }

    let Some(system) = registry.database.system else {
        return Err(internal_serving_error(
            "a served bootstrap read resolves with the system present",
        ));
    };
    let quoted = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let from = match &served.backend_schema {
        Some(schema) => format!("{}.{}", quoted(schema), quoted(served.canonical.as_str())),
        None => quoted(served.canonical.as_str()),
    };
    let select = format!(
        "SELECT {} FROM {}",
        columns
            .iter()
            .map(|name| quoted(name))
            .collect::<Vec<_>>()
            .join(", "),
        from
    );

    let connection = system.bootstrap_connection();
    let guard = connection.lock().map_err(|e| {
        DelightQLError::connection_poison_error(
            "Failed to acquire bootstrap lock for a served materialization source",
            format!("Connection was poisoned: {}", e),
        )
    })?;
    let mut statement = guard
        .prepare(&select)
        .map_err(|e| internal_serving_error(&format!("bootstrap-source prepare failed: {e}")))?;
    let width = columns.len();
    let mut literal_rows: Vec<TabularRow<Datum<crate::pipeline::asts::core::Resolved>>> =
        Vec::new();
    let mut rows = statement
        .query([])
        .map_err(|e| internal_serving_error(&format!("bootstrap-source execution failed: {e}")))?;
    while let Some(row) = rows
        .next()
        .map_err(|e| internal_serving_error(&format!("bootstrap-source read failed: {e}")))?
    {
        let mut cells = Vec::with_capacity(width);
        for index in 0..width {
            let value = row.get_ref(index).map_err(|e| {
                internal_serving_error(&format!("bootstrap-source cell read failed: {e}"))
            })?;
            cells.push(Datum::Value(ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Ground(served_literal(value)?),
            )));
        }
        literal_rows.push(TabularRow(Box::new(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(cells)
                .expect("a catalog table has at least one column"),
        )));
    }
    drop(rows);
    drop(statement);
    drop(guard);

    // ZERO ROWS: the literal geometry is nonempty by type, so an empty
    // snapshot is one all-NULL row behind a false restriction — the same
    // zero-row relation, with its heading intact.
    let empty = literal_rows.is_empty();
    if empty {
        let cells: Vec<_> = (0..width)
            .map(|_| {
                Datum::Value(ast_resolved::DomainExpression::Application(
                    ast_resolved::FunctionApplication::Ground(LiteralValue::Null),
                ))
            })
            .collect();
        literal_rows.push(TabularRow(Box::new(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(cells)
                .expect("a catalog table has at least one column"),
        )));
    }

    chain.head = Grelex::Literal(AnonRelation {
        table: AnonTable {
            body: TabularBody {
                header: None,
                rows: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(literal_rows)
                    .expect("the empty snapshot was given its NULL row above"),
            },
            cpr_schema: scope,
        },
        alias: None,
        outer,
    });
    if empty {
        let falsehood = ast_resolved::TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Ground(LiteralValue::Number("0".to_string())),
            )),
            right: Box::new(ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Ground(LiteralValue::Number("1".to_string())),
            )),
        });
        // The head's own read (a leading access) stays the head's; the
        // false restriction stands right behind it.
        let position = usize::from(matches!(
            chain.continuations.first(),
            Some(ast_resolved::Continuation::Access { .. })
        ));
        chain.continuations.insert(
            position,
            ast_resolved::Continuation::Restrict {
                condition: falsehood,
                origin: crate::pipeline::asts::core::FilterOrigin::Generated,
                cpr_schema: scope,
            },
        );
    }
    Ok((chain, bubbled))
}

#[cfg(target_arch = "wasm32")]
fn serve_bootstrap_relation(
    resolved: (ast_resolved::Chain, BubbledState),
    _served: ServedBootstrapRead,
    _registry: &mut crate::resolution::EntityRegistry,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    Ok(resolved)
}

fn internal_serving_error(message: &str) -> DelightQLError {
    DelightQLError::transformation_error(message, "bootstrap_serving")
}

/// One engine value as the literal it spells. The catalog's declared
/// schemas carry no BLOB columns; meeting one is a teaching, not a panic.
#[cfg(not(target_arch = "wasm32"))]
fn served_literal(
    value: rusqlite::types::ValueRef<'_>,
) -> Result<crate::pipeline::asts::core::LiteralValue> {
    use crate::pipeline::asts::core::LiteralValue;
    use rusqlite::types::ValueRef;
    Ok(match value {
        ValueRef::Null => LiteralValue::Null,
        ValueRef::Integer(value) => LiteralValue::Number(value.to_string()),
        // `{:?}` round-trips an f64: it always writes a decimal point or
        // an exponent, so the literal keeps REAL affinity.
        ValueRef::Real(value) => LiteralValue::Number(format!("{value:?}")),
        ValueRef::Text(bytes) => LiteralValue::String(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(_) => {
            return Err(DelightQLError::validation_error_categorized(
                "materialization/bootstrap_blob",
                "a bootstrap BLOB column has no literal spelling to serve",
                "project the column out of the materialization source",
            ))
        }
    })
}

/// Record `!!` on the occurrence a resolved access publishes.
///
/// One place, whatever kind of relation the name turned out to name: a
/// catalog table, a temporary one an earlier step created, a CTE. The mark
/// belongs to this occurrence and not to the definition behind it, so a
/// second, unmarked reference to the same name carries nothing.
fn note_mutation_mark(
    relation: Option<crate::names::Spelling>,
    resolved: &ast_resolved::Chain,
    identities: &crate::names::Registry,
) {
    if let Some(relation) = relation {
        identities.mark_mutation_target(
            super::helpers::extraction::extract_cpr_schema(resolved),
            relation,
        );
    }
}

/// Explain a resolved consulted functor that cannot occupy relation position.
///
/// Kind lookup is centralized before this point, so a defined name never falls
/// through to the absence diagnostic merely because its invocation form is
/// non-relational.
fn defined_non_relation_error(
    name: &SqlIdentifier,
    entity_type: BootstrapEntityType,
) -> DelightQLError {
    let message = match entity_type {
        BootstrapEntityType::DqlFunctionExpression
        | BootstrapEntityType::DqlHoFunctionExpression
        | BootstrapEntityType::DqlContextAwareFunctionExpression => format!(
            "'{name}' is a function, not a relation — call it as \
             `{name}:(args)`. (A case/scalar function has no relation face \
             `{name}(*)`.)"
        ),
        BootstrapEntityType::DqlHoTemporaryViewExpression => format!(
            "'{name}' is a higher-order view, not a relation — supply its \
             relation argument, for example `{name}(source(*))(*)`"
        ),
        BootstrapEntityType::DqlTemporarySigmaRule | BootstrapEntityType::BinSigmaPredicate => {
            format!(
                "'{name}' is a sigma predicate, not a relation — use it in a \
                 condition rather than accessing `{name}(*)`"
            )
        }
        BootstrapEntityType::BinPseudoPredicate | BootstrapEntityType::DqlEffectRule => format!(
            "'{name}' is a directive, not a relation — invoke the directive \
             rather than accessing `{name}(*)`"
        ),
        BootstrapEntityType::DqlErContextRule => format!(
            "'{name}' is an ER-context rule, not a relation — select it through \
             its declared ER context"
        ),
        other => format!(
            "'{name}' is defined as {}, not a relation",
            other.variant_name()
        ),
    };
    DelightQLError::validation_error(
        message,
        format!("'{name}' resolved to {}", entity_type.variant_name()),
    )
}

/// A runtime-served relation that resolution reached before execution did.
///
/// This is not a statement about the entity's category: it names a relation
/// and publishes a heading, and every position that reaches it through the
/// executable boundary works. What it reports is that this OCCURRENCE was
/// not on a chain the boundary walks — today, a consulted rule's body, whose
/// expansion happens during resolution, after that boundary has run.
fn runtime_served_unreached_error(
    name: &SqlIdentifier,
    entity_type: BootstrapEntityType,
) -> DelightQLError {
    DelightQLError::validation_error(
        format!(
            "'{name}' is a bin relation served by the runtime, and this \
             occurrence escaped the executable boundary that produces its \
             rows — a compiler fence, not a semantic outcome; the direct, \
             bound and consulted spellings all execute"
        ),
        format!("'{name}' resolved to {}", entity_type.variant_name()),
    )
}

/// Handle PASSTHROUGH resolution: skip entity catalog, use schema introspector directly.
/// Best-effort: try to get columns from backend, fall back to opaque glob if not found.
pub(super) fn r_resolve_passthrough(
    identifier: ast_unresolved::QualifiedName,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    if identifier.namespace_path.is_empty() {
        return Err(DelightQLError::validation_error(
            "Passthrough table access requires a namespace path (e.g., main/table_name(*))"
                .to_string(),
            "passthrough_requires_namespace".to_string(),
        ));
    }

    // Prefer the mounted catalog, then ask the target introspector for a
    // backend-owned relation that the catalog does not enumerate.
    let (table_schema, canonical_name, passthrough_backend_schema) = match registry
        .database
        .lookup_passthrough_table_with_namespace(&identifier.namespace_path, &identifier.name)
    {
        Ok(Some((schema, connection_id, canon, passthrough_backend_schema))) => {
            registry.track_connection_id(connection_id);
            (Some(schema), Some(canon), passthrough_backend_schema)
        }
        Ok(None) | Err(_) => {
            // Best-effort: table not found in introspector — fall back to opaque
            (None, None, None)
        }
    };

    if let Some(schema) = table_schema {
        bind_physical_relation(
            schema,
            canonical_name.as_ref(),
            passthrough_backend_schema.as_deref(),
            &registry.identities,
        )?;
        // Got columns from backend — resolve normally with pattern resolver
        let table_name_str = alias.as_deref().unwrap_or(&identifier.name);

        // Relabel columns with alias if present
        let relabeled_cols = relabel_columns_with_alias(schema, &alias, &registry.identities);

        let (mut final_expr, state) = apply_pattern_resolver(
            &access,
            &relabeled_cols,
            table_name_str,
            registry,
            outer_context,
            config.cfe_formal_frame.as_deref(),
            config.resolution_namespace.as_deref(),
            &config.instantiation_depth,
        )?;

        // Outerness is the only thing the call site still contributes: the
        // backend lookup that got here IS the passthrough decision, and the
        // spelling it was made from is spent on the scope the pattern
        // resolver published.
        if let ast_resolved::Grelex::Reference(ast_resolved::Relation::Ground {
            outer: ref mut rel_outer,
            ..
        }) = final_expr.head
        {
            *rel_outer = outer;
        }

        return Ok((final_expr, state));
    }

    // Opaque fallback: no column info available. Only an access that names
    // no dimensions can be answered without a heading, and which accesses
    // those are is the access type's own answer.
    if !access.is_whole() {
        return Err(DelightQLError::validation_error(
            format!(
                "Passthrough table '{}/{}' schema not available — only (*) is allowed, not positional binding",
                identifier.namespace_path, identifier.name
            ),
            "passthrough_opaque_glob_only".to_string(),
        ));
    }

    // A passthrough reads a backend table the entity catalog does not
    // describe. It is a relation — it has an identity — and its heading is
    // the target's to publish. The scope travels upward so a reference
    // standing over it learns that nothing was enumerated, rather than being
    // told the name is absent.
    let scope = registry.identities.mint_opaque_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
    );
    Ok((
        ast_resolved::Relation::ground_read(ast_resolved::Access::All, outer, scope),
        BubbledState::opaque(scope),
    ))
}

/// Mark a resolved ground relation as an outer-join operand.
///
/// The head is where a ground relation lives; the continuations that may sit
/// above it (a generated restriction) do not carry outerness.
fn patch_ground_outer(expr: &mut ast_resolved::Chain, outer: bool) {
    if let ast_resolved::Grelex::Reference(ast_resolved::Relation::Ground {
        outer: ref mut rel_outer,
        ..
    }) = expr.head
    {
        *rel_outer = outer;
    }
}

/// Handle CTE resolution result.
pub(super) fn r_resolve_cte(
    entity_info: crate::resolution::EntityInfo,
    identifier: ast_unresolved::QualifiedName,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    use crate::resolution::EntityDefinition;

    let canonical_name = entity_info.canonical_name.clone();
    let backend_schema = entity_info.backend_schema.clone();
    // Extract the CTE schema
    let EntityDefinition::RelationSchema(cte_schema) = entity_info.definition;
    if canonical_name.is_some() {
        bind_physical_relation(
            cte_schema,
            canonical_name.as_ref(),
            backend_schema.as_deref(),
            &registry.identities,
        )?;
    }
    if matches!(
        registry.identities.origin_of(cte_schema),
        crate::names::ScopeOrigin::Scratch { .. }
    ) {
        return Err(DelightQLError::validation_error(
            "Plan scratch must be referenced by scope identity",
            "effect plan identity",
        ));
    }
    // The consult of a USER-DEFINED CTE is an access boundary,
    // same regime as a consulted view: the caller reaches the
    // EXPORTED heading, which answers to the access name (the
    // user's alias, or the CTE name) — bare declarations never
    // leak through, and the export re-roots so a body-internal
    // column spelling cannot reach the SQL (it breaks when the
    // CTE head renames). Compiler-generated CTEs (HO expansion,
    // pipe materialization) are the caller-pattern seam's
    // channel: the seam names their positional columns through
    // the identity stack, so they keep their identity untouched
    // — the boundary law for seam shapes lands with the seam
    // rework, not by breaking it. Argumentative access still
    // declares its own bare lvars either way: the pattern
    // resolver re-declares on selection.
    let source_scope = cte_schema;
    let base_cols: Vec<crate::names::ColId> = {
        let access: SqlIdentifier = alias.clone().unwrap_or_else(|| identifier.name.clone());
        let access_spelling = registry
            .identities
            .intern(access.as_str(), access.is_stropped());
        let scope = registry.identities.mint_scope(
            crate::names::ScopeOrigin::UserAlias { of: source_scope },
            crate::names::Hint::User(access_spelling),
            None,
        );
        let access_name = registry.identities.canonical(access_spelling);
        // A hygienic carrier stops at this boundary for the same
        // reason it stops at a view access: it stands for a slot
        // that introduced no name, and the constraint reading it
        // was applied inside the body the CTE materialized.
        // Exporting it as BareAnswering hands an internal column
        // the access name and puts a target in the heading that
        // the CTE's own output no longer offers.
        registry
            .identities
            .known_heading(source_scope)?
            .into_iter()
            .filter(|column| {
                registry.identities.addressing(*column) != crate::names::Addressing::Hygienic
            })
            .map(|column| {
                registry.identities.republish_column(
                    column,
                    scope,
                    crate::names::Republish::BoundaryExport,
                    registry.identities.published(column),
                    crate::names::Addressing::BareAnswering(access_name),
                    |_| {},
                )
            })
            .collect()
    };

    // Use PatternResolver for column selection
    let (mut final_expr, state) = apply_pattern_resolver(
        &access,
        &base_cols,
        alias.as_deref().unwrap_or(&identifier.name),
        registry,
        outer_context,
        config.cfe_formal_frame.as_deref(),
        config.resolution_namespace.as_deref(),
        &config.instantiation_depth,
    )?;

    patch_ground_outer(&mut final_expr, outer);

    Ok((final_expr, state))
}

/// Handle DatabaseEntity resolution result.
pub(super) fn r_resolve_database_entity(
    entity_info: crate::resolution::EntityInfo,
    identifier: ast_unresolved::QualifiedName,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    use crate::resolution::EntityDefinition;

    // Extract fields before entity_info is consumed
    let canonical_name = entity_info.canonical_name.clone();
    let entity_backend_schema = entity_info.backend_schema.clone();
    // Extract the table schema
    let EntityDefinition::RelationSchema(table_schema) = entity_info.definition;
    bind_physical_relation(
        table_schema,
        canonical_name.as_ref(),
        entity_backend_schema.as_deref(),
        &registry.identities,
    )?;
    // Apply alias if present
    let base_cols = relabel_columns_with_alias(table_schema, &alias, &registry.identities);

    // Use PatternResolver for column selection
    let (mut final_expr, state) = apply_pattern_resolver(
        &access,
        &base_cols,
        alias.as_deref().unwrap_or(&identifier.name),
        registry,
        outer_context,
        config.cfe_formal_frame.as_deref(),
        config.resolution_namespace.as_deref(),
        &config.instantiation_depth,
    )?;

    patch_ground_outer(&mut final_expr, outer);

    Ok((final_expr, state))
}

/// Handle ConsultedView resolution result.
pub(super) fn r_resolve_consulted_view(
    view_name: SqlIdentifier,
    body_source: String,
    view_ns: String,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    // Consulted view — normalize the stored bytes EXACTLY ONCE into a
    // typed query, then delegate to the typed entrance below.
    //
    // Check if this view comes from a pre-grounded namespace
    // (created by ground!). If so, apply data namespace patching
    // so unqualified table references resolve to the bound data namespace.
    let auto_grounding = registry
        .consult
        .get_namespace_default_data_ns(&view_ns)
        .and_then(|data_ns_fq| {
            let data_ns = ast_unresolved::NamespacePath::from_fq_string(&data_ns_fq).ok()?;
            let grounded_ns = ast_unresolved::NamespacePath::from_fq_string(&view_ns).ok()?;
            Some(ast_unresolved::GroundedPath {
                data_ns,
                grounded_ns: vec![grounded_ns],
            })
        });

    if let Some(grounding) = auto_grounding {
        // Pre-grounded namespace: expand view as full Query (preserves CTEs)
        let query = super::grounding::expand_consulted_view(&body_source, &grounding)
            .map_err(|e| wrap_view_body_error(e, "expanding", &view_name, &view_ns))?;
        return r_resolve_view_query(
            view_name,
            query,
            view_ns,
            access,
            alias,
            outer,
            registry,
            outer_context,
            config,
            Some(&grounding),
        );
    }

    // Normal consulted view (not pre-grounded) — parse as full Query
    // to preserve CTEs. The one door assembles the group's clause heads;
    // a declared heading is then enforced by the shared desugar law.
    let group = crate::ddl::reconstruct::group(&body_source).map_err(|e| {
        DelightQLError::database_error(
            format!("Error while parsing borrowed view '{}': {}", view_name, e),
            e.to_string(),
        )
    })?;
    let mut clauses = group.spend_heads()?;
    let query = if clauses.len() <= 1 {
        // Single clause: same as before
        let clause = clauses.pop().ok_or_else(|| {
            DelightQLError::parse_error(format!("No definition found for view '{}'", view_name))
        })?;
        clause.into_query().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Expected relational body for view '{}', got scalar",
                view_name
            ))
        })?
    } else {
        // Multi-clause: synthesize disjunctive CTEs (no data_ns patching
        // since this is a non-grounded borrowed view)
        super::grounding::expand_multi_clause_view(&view_name, clauses, None).map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Error while expanding disjunctive view '{}': {}",
                    view_name, e
                ),
                e.to_string(),
            )
        })?
    };

    r_resolve_view_query(
        view_name,
        query,
        view_ns,
        access,
        alias,
        outer,
        registry,
        outer_context,
        config,
        None,
    )
}

/// The TYPED consulted-view entrance: resolve an ALREADY-BUILT body query
/// as `view_name`'s access boundary, inside the causing compilation's
/// registry and config. The text roads above normalize their stored bytes
/// exactly once and land here; a compiler-built wrapper (the liminal
/// ledger) constructs its query directly and enters the same door, so no
/// resolver road mints DQL text to reach a relation it can build.
pub(super) fn r_resolve_view_query(
    view_name: SqlIdentifier,
    query: ast_unresolved::Query,
    view_ns: String,
    access: ast_unresolved::Access,
    alias: Option<SqlIdentifier>,
    outer: bool,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    // Cycle guard (CYCLES THROUGH OTHER DEFINITIONS REFUSE): re-expanding a view already
    // in flight means its self-reference did NOT resolve as the in-progress
    // CTE (recursive clause before base, or an indirect cycle through
    // another view) — refuse with the teaching error instead of spinning.
    // The frame pops on every return path.
    let _expansion_frame = config.expansion_guard.enter(
        format!("{}::{}", view_ns, view_name),
        "resolver::consulted_view_expansion",
    )?;

    // Scope ER-rule lookups to the view's namespace for qualified access.
    // A pre-grounded body keeps the caller's namespace: its
    // data-namespace patching was already applied when the query was
    // built. Sealed against the instantiation too: a caller-side formal
    // must not substitute inside foreign body text.
    let mut body_config = if grounding.is_none() && !view_ns.is_empty() && view_ns != "main" {
        ResolutionConfig {
            resolution_namespace: Some(view_ns.clone()),
            ..config.clone()
        }
    } else {
        config.clone()
    };
    body_config.cfe_formal_frame = None;

    // lookup_entity's alias clause is SCOPE-AWARE (a definition's
    // qualifier alias resolves through its OWN namespace_local_alias
    // rows, never the caller's session set).

    // A rule body is SEALED: its meaning cannot depend on the call
    // site's scope. With the caller's columns in reach, a body's own
    // positional rebind was captured as an outer correlation whenever
    // the caller had same-named columns — the rebind silently
    // vanished and the body resolved as a glob. Correlation into a
    // subquery is the call-site condition's business, never the body's.
    // Body-introduced BINDINGS end with the body — the inline entrance's
    // own extent: a recursive rule's self-registration must not answer a
    // sibling clause's later reference, and a view-local CFE definition
    // must not replace the caller's same-named binding.
    let resolve_result = super::resolve_query_inline(query, registry, None, &body_config, grounding);

    let (resolved_query, _body_bubbled) = resolve_result.map_err(|e| {
        if grounding.is_some() {
            return wrap_view_body_error(e, "resolving", &view_name, &view_ns);
        }
        // Preserve validation errors (e.g., the B5 expansion-cycle refusal)
        // so their subcategory URI survives to the user and to error
        // assertions.
        if matches!(e, DelightQLError::ValidationError { .. }) {
            return e;
        }
        DelightQLError::database_error(
            format!("Error while resolving borrowed view '{}': {}", view_name, e),
            e.to_string(),
        )
    })?;

    let body_schema = super::helpers::extraction::extract_cpr_schema_from_query(&resolved_query)?;

    let (effective_alias, access_scope) =
        access_boundary_export(&alias, &view_name, body_schema, &registry.identities);

    let effective_name = effective_alias.to_string();

    let base_expr = ast_resolved::Chain::relation(ast_resolved::Relation::ConsultedView {
        body: Box::new(resolved_query),
        scoped: access_scope,
        outer,
    });

    if !matches!(access, ast_unresolved::Access::All) {
        let (final_expr, final_bubbled) = apply_call_site_pattern(
            &access,
            base_expr,
            access_scope,
            &effective_name,
            &view_name,
            outer_context,
            &registry.identities,
            config.cfe_formal_frame.as_deref(),
            Some(super::SlotInstantiation {
                scoped_cfes: &registry.query_local.scoped_cfes,
                consult: &registry.consult,
                lookup_scope: config.resolution_namespace.as_deref(),
                depth: &config.instantiation_depth,
            }),
        )?;
        Ok((final_expr, final_bubbled))
    } else {
        // The lvar law: the columns answer to the ACCESS name — the
        // user's alias, or the bare entity name of an unaliased access.
        let body_bubbled = BubbledState::resolved(
            registry.identities.known_heading(access_scope)?.to_vec(),
            &registry.identities,
        );
        Ok((base_expr, body_bubbled))
    }
}

/// Handle Unknown (or unmatched) resolution result.
pub(super) fn r_resolve_unknown(
    identifier: ast_unresolved::QualifiedName,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    let (table_name, context) = if !identifier.namespace_path.is_empty() {
        // Construct namespace path string using :: separator (DelightQL
        // format). NOTE: storage order, not iter_reversed — the old
        // context string used iter_reversed and rendered multi-segment
        // paths BACKWARDS (sys::meta as "meta::sys"), invisibly, because
        // the context field is never displayed.
        let ns_str = identifier.namespace_path.fq_string();
        // Report the FULL path the user wrote, never the bare leaf: the
        // leaf-only "Table not found: orders" for `sales.orders(*)`
        // hid the actual mistake (an under-qualified namespace), sent
        // readers hunting for a missing TABLE, and manufactured a
        // false "mount! is broken" diagnosis.
        (
            format!("{}.{}", ns_str, identifier.name),
            format!("Entity '{}' not found in namespace '{}'. The namespace prefix as written did not resolve — check it against your mounts (sys::ns.namespace(*) lists them; a mount under 'data::{}' is reached as 'data::{}.{}'). Other causes: entity not activated, or missing backend schema configuration.", identifier.name, ns_str, ns_str, ns_str, identifier.name)
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

/// Infer a `declared_type` for each anonymous-table column from its literal
/// grid — the `@`-rows ARE the column's declaration. Conservative: a column
/// types only if every cell is a literal of one uniform type (NULLs ignored;
/// INTEGER unifies with REAL as REAL). Any non-literal cell (melt patterns
/// reference outer columns), boolean, or text/numeric mix yields None —
/// that's sqlite-dynamic data with no honest single type. First consumer:
/// corresponding-union NULL pads, whose type comes from the Registry value
/// facts. An untyped pad inside a subquery collapses to text at the pg
/// subquery boundary before the union can resolve it against the typed branch.
fn infer_anon_column_types(
    rows: &crate::pipeline::asts::vocabulary::Vec1<ast_resolved::TabularRow<ast_resolved::Datum>>,
) -> Vec<Option<String>> {
    let num_cols = rows.first().len();
    (0..num_cols)
        .map(|idx| {
            let mut unified: Option<&str> = None;
            for row in rows {
                let Some(ast_resolved::DomainExpression::Application(
                    ast_resolved::FunctionApplication::Ground(value),
                )) = row.0.get(idx).map(ast_resolved::Datum::value)
                else {
                    return None;
                };
                let cell = match &value {
                    ast_resolved::LiteralValue::Null => continue,
                    ast_resolved::LiteralValue::String(_)
                    | ast_resolved::LiteralValue::Symbol(_)
                    | ast_resolved::LiteralValue::Mention(_) => "TEXT",
                    ast_resolved::LiteralValue::Number(n) => {
                        if n.contains(['.', 'e', 'E']) {
                            "REAL"
                        } else {
                            "INTEGER"
                        }
                    }
                    ast_resolved::LiteralValue::Boolean(_) => return None,
                };
                unified = match (unified, cell) {
                    (None, c) => Some(c),
                    (Some(t), c) if t == c => Some(t),
                    (Some("INTEGER"), "REAL") | (Some("REAL"), "INTEGER") => Some("REAL"),
                    _ => return None,
                };
            }
            unified.map(str::to_string)
        })
        .collect()
}

fn infer_anon_column_shapes(
    rows: &crate::pipeline::asts::vocabulary::Vec1<ast_resolved::TabularRow<ast_resolved::Datum>>,
) -> Vec<crate::names::ValueShape> {
    use crate::pipeline::asts::core::Enclyph;
    (0..rows.first().len())
        .map(|idx| {
            let mut shape = None;
            for row in rows {
                let Some(datum) = row.0.get(idx) else {
                    return crate::names::ValueShape::Unknown;
                };
                let current = match datum.value() {
                    ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Enclyph(Enclyph::Record(_)),
                    ) => crate::names::ValueShape::Record,
                    ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Enclyph(Enclyph::EmptyRecord(_)),
                    ) => crate::names::ValueShape::Record,
                    ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Enclyph(Enclyph::Tuple(_)),
                    ) => crate::names::ValueShape::Tuple,
                    _ => return crate::names::ValueShape::Unknown,
                };
                match shape {
                    None => shape = Some(current),
                    Some(existing) if existing == current => {}
                    Some(_) => return crate::names::ValueShape::Unknown,
                }
            }
            shape.unwrap_or_default()
        })
        .collect()
}

/// Loud where knowable: narrowing (`|> .col{...}`) iterates an ARRAY,
/// and when the narrowed column is an anonymous-table column whose
/// every row is a literal OBJECT constructor, the mistake is provable
/// at resolve time — the expansion would walk the object's MEMBERS and
/// return silent all-NULL rows. Refuse naming both remedies. Data-borne
/// values (real columns, mixed or non-literal rows) pass through; their
/// non-array behavior is an open ruling.
pub(super) fn refuse_knowable_object_narrowing(
    column: &str,
    source: &ast_resolved::Chain,
    identities: &crate::names::Registry,
) -> Result<()> {
    let scope = super::helpers::extraction::extract_cpr_schema(source);
    let sought = identities.canonical(identities.intern(column, false));
    let Some(idx) = identities
        .known_heading(scope)?
        .iter()
        .position(|candidate| identities.published_sym(*candidate) == Some(sought))
    else {
        return Ok(());
    };
    let occurrence = identities
        .known_heading(scope)?
        .in_order()
        .nth(idx)
        .copied()
        .expect("the named position came from this exhaustive heading");
    if identities.facts(occurrence).shape == crate::names::ValueShape::Record {
        return Err(DelightQLError::validation_error_categorized(
            "narrowing/object_literal",
            format!(
                "narrowing iterates an array — every row of '{column}' is a single \
                 object. Path into the object instead: ({column}:{{.field}}), or \
                 spell the one-element sequence: [{{...}}]."
            ),
            "brace narrowing",
        ));
    }
    Ok(())
}

/// Resolve an Anonymous relation variant (inline table with rows/headers).
///
/// Handles header resolution, row value resolution, and QUA schema conformance.
/// Resolve an expression written inside an anonymous table against the row
/// that ENCLOSES it.
///
/// A header and a data cell both reach out of the anonymous relation for the
/// names they use — the anonymous relation has no heading of its own until
/// these are resolved. The context swap is one act, so a header and a cell
/// cannot come to disagree about which columns were in scope.
fn resolve_against_outer_context(
    fold: &mut super::resolver_fold::ResolverFold<'_, '_>,
    outer_context: Option<&[crate::names::ColId]>,
    expression: ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    let saved_available = std::mem::take(&mut fold.available);
    let saved_local_available = std::mem::take(&mut fold.local_available);
    let saved_qualifier_scope = std::mem::take(&mut fold.qualifier_scope);
    let saved_in_correlation = fold.in_correlation;
    fold.available = outer_context.unwrap_or(&[]).to_vec();
    fold.local_available = fold.available.clone();
    fold.qualifier_scope = outer_context
        .unwrap_or(&[])
        .iter()
        .map(|column| fold.registry.identities.scope_of(*column))
        .fold(Vec::new(), |mut scopes, scope| {
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
            scopes
        });
    fold.in_correlation = false;
    let result = fold.transform_domain(expression);
    fold.available = saved_available;
    fold.local_available = saved_local_available;
    fold.qualifier_scope = saved_qualifier_scope;
    fold.in_correlation = saved_in_correlation;
    result
}

pub(super) fn resolve_anonymous(
    anon: ast_unresolved::AnonRelation,
    fold: &mut super::resolver_fold::ResolverFold,
    outer_context: Option<&[crate::names::ColId]>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    let ast_unresolved::AnonRelation {
        table:
            ast_unresolved::AnonTable {
                body: ast_unresolved::TabularBody { header, rows },
                cpr_schema: _,
            },
        alias: relation_alias,
        outer,
    } = anon;
    let column_headers = header
        .as_ref()
        .map(|row| {
            row.iter()
                .map(|item| {
                    item.term().ok_or_else(|| {
                        DelightQLError::parse_error("a tabular header slot has a domain term")
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;

    let scope_hint = relation_alias
        .as_ref()
        .map(|alias| {
            crate::names::Hint::User(
                fold.registry
                    .identities
                    .intern(alias.as_str(), alias.is_stropped()),
            )
        })
        .unwrap_or(crate::names::Hint::None);
    let anonymous_scope = fold.registry.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        scope_hint,
        None,
    );

    // Convert rows from unresolved to resolved format
    // Resolve anonymous table data rows with outer_context for melt/unpivot
    let resolved_rows = rows.clone().try_map(|row| {
        let resolved_values = (*row.0).try_map(|datum| {
            let sparse_column = match &datum {
                ast_unresolved::Datum::SparseFill { column, .. } => Some(column.clone()),
                ast_unresolved::Datum::Value(_) => None,
            };
            let val = datum.into_value();
            match val {
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(value),
                ) => {
                    let value = ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(value),
                    );
                    Ok(match sparse_column {
                        Some(column) => ast_resolved::Datum::SparseFill {
                            column,
                            fallback: match value {
                                ast_resolved::DomainExpression::Application(
                                    ast_resolved::FunctionApplication::Ground(ref value),
                                ) => value.clone(),
                                _ => unreachable!(),
                            },
                        },
                        None => ast_resolved::Datum::Value(value),
                    })
                }
                // Resolve column references and other expressions.
                // This enables melt/unpivot patterns like:
                // _(attr, val @ "name", first_name; "id", user_id)
                //                       ^^^^^^^^^^      ^^^^^^^
                _ => resolve_against_outer_context(fold, outer_context, val)
                    .map(ast_resolved::Datum::Value),
            }
        })?;
        Ok::<_, DelightQLError>(crate::pipeline::asts::core::TabularRow(Box::new(
            resolved_values,
        )))
    })?;

    // An lvar cannot appear both in a header and in the data rows of
    // the same anonymous table: the header is the probe,
    // a row lvar is a candidate — the same name in both makes the
    // membership vacuously true, and in the relational forms it
    // collides the declaration with the reference.
    if let Some(headers) = &column_headers {
        for header in headers {
            let ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn { name, .. },
            ))) = header
            else {
                continue;
            };
            let repeated = rows.iter().any(|row| {
                row.iter().any(|datum| {
                    let cell = datum.value();
                    matches!(cell,
                        ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn { name: cell_name, .. })))
                            if delightql_types::SqlIdentifier::str_eq(cell_name.as_str(), name))
                })
            });
            if repeated {
                return Err(crate::error::DelightQLError::validation_error_categorized(
                    "resolution/anon/header_row_lvar",
                    format!(
                        "lvar '{}' appears both as a header and in the data rows of the same anonymous table",
                        name
                    ),
                    "the header is the probe and a row lvar is a candidate — probing a column against itself is vacuously true; drop the self-candidate or rename the header",
                ));
            }
        }
    }

    // Literal-grid type inference: the rows are the columns' declaration.
    let inferred_types = infer_anon_column_types(&resolved_rows);
    let inferred_shapes = infer_anon_column_shapes(&resolved_rows);

    // Headers are declarations into the anonymous relation's one scope.
    let (resolved_headers, resolved_schema) = if let Some(headers) = &column_headers {
        let mut resolved_headers = Vec::new();

        for (idx, header) in headers.iter().enumerate() {
            match header {
                ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                    AuthoredColumn { name, .. },
                ))) => {
                    let published = fold
                        .registry
                        .identities
                        .intern(name.as_str(), name.is_stropped());
                    let addressing = match &relation_alias {
                        Some(relation_alias) => {
                            let alias_spelling = fold
                                .registry
                                .identities
                                .intern(relation_alias.as_str(), relation_alias.is_stropped());
                            crate::names::Addressing::BareAnswering(
                                fold.registry.identities.canonical(alias_spelling),
                            )
                        }
                        None => crate::names::Addressing::Bare,
                    };
                    let column = fold.registry.identities.mint_column(
                        anonymous_scope,
                        crate::names::ColumnOrigin::Bound {
                            position: idx as u32,
                        },
                        Some(published),
                        addressing,
                        crate::names::ValueFacts {
                            declared_type: inferred_types.get(idx).cloned().flatten(),
                            shape: inferred_shapes.get(idx).copied().unwrap_or_default(),
                            ..Default::default()
                        },
                    );
                    resolved_headers.push(ast_resolved::DomainExpression::Reference(
                        Reference::Named(NamedReference(ColumnOccurrence {
                            column,
                            explicit_qualifier: false,
                        })),
                    ));
                }
                _ => {
                    // A computed header names a column of the ENCLOSING row —
                    // `_(upper:(description) @ …)` probes the outer relation's
                    // `description`. It resolves against the same context the
                    // data rows do, because it is a reference out of the same
                    // place.
                    let resolved_expr =
                        resolve_against_outer_context(fold, outer_context, header.clone())?;
                    resolved_headers.push(resolved_expr.clone());

                    let (origin, addressing) = match &resolved_expr {
                        ast_resolved::DomainExpression::Application(
                            ast_resolved::FunctionApplication::Ground(_),
                        ) => (
                            crate::names::ColumnOrigin::Computed {
                                via: crate::names::Computation::Literal,
                            },
                            crate::names::Addressing::Published,
                        ),
                        ast_resolved::DomainExpression::Application(_) => (
                            crate::names::ColumnOrigin::Computed {
                                via: crate::names::Computation::Function,
                            },
                            crate::names::Addressing::Hygienic,
                        ),
                        other => panic!("catch-all hit in relation_resolver.rs resolve_inline_relation (DomainExpression column name): {:?}", other),
                    };
                    fold.registry.identities.mint_column(
                        anonymous_scope,
                        origin,
                        None,
                        addressing,
                        crate::names::ValueFacts {
                            declared_type: inferred_types.get(idx).cloned().flatten(),
                            shape: inferred_shapes.get(idx).copied().unwrap_or_default(),
                            ..Default::default()
                        },
                    );
                }
            }
        }

        (Some(resolved_headers), anonymous_scope)
    } else {
        let num_cols = resolved_rows.first().len();
        for idx in 0..num_cols {
            fold.registry.identities.mint_column(
                anonymous_scope,
                crate::names::ColumnOrigin::Minted {
                    by: crate::names::MintReason::AnonHeader,
                },
                None,
                crate::names::Addressing::Published,
                crate::names::ValueFacts {
                    declared_type: inferred_types.get(idx).cloned().flatten(),
                    shape: inferred_shapes.get(idx).copied().unwrap_or_default(),
                    ..Default::default()
                },
            );
        }
        (None, anonymous_scope)
    };

    let resolved_header = resolved_headers.map(|headers| {
        let sparse = header
            .as_ref()
            .expect("resolved headers preserve an authored header");
        crate::pipeline::asts::core::TabularRow(Box::new(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                headers
                    .into_iter()
                    .zip(sparse.iter())
                    .map(|(term, authored)| ast_resolved::HeaderItem {
                        slot: ast_resolved::Slot::classify(term),
                        sparse: authored.sparse,
                    })
                    .collect(),
            )
            .expect("a tabular header is nonempty"),
        ))
    });
    let resolved_relation = ast_resolved::AnonRelation {
        table: ast_resolved::AnonTable {
            body: ast_resolved::TabularBody {
                header: resolved_header,
                rows: resolved_rows,
            },
            cpr_schema: resolved_schema,
        },
        alias: relation_alias,
        outer,
    };

    // Create bubbled state with the schema columns
    let state = BubbledState::resolved(
        fold.registry
            .identities
            .known_heading(resolved_schema)?
            .to_vec(),
        &fold.registry.identities,
    );

    Ok((
        ast_resolved::Chain::ground(ast_resolved::Grelex::Literal(resolved_relation)),
        state,
    ))
}

/// The data world a pre-grounded namespace is bound to, as a path.
/// Namespaces created by ground! carry default_data_ns; anything else
/// answers None and grounds against the empty path.
fn pre_grounded_data_ns(
    registry: &crate::resolution::EntityRegistry,
    namespace_fq: &str,
) -> Option<ast_unresolved::NamespacePath> {
    registry
        .consult
        .get_namespace_default_data_ns(namespace_fq)
        .and_then(|data_ns_fq| ast_unresolved::NamespacePath::from_fq_string(&data_ns_fq).ok())
}

/// Handles higher-order view expansion and ordinary relational calls through
/// the shared call carrier.
pub(super) fn resolve_functor_call(
    call: ast_unresolved::FunctorCall,
    // The name the READ answers to, from the relation occurrence the call
    // stands in. Call identity carries none.
    alias: Option<delightql_types::SqlIdentifier>,
    access: ast_unresolved::Access,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    mut join_input: Option<ast_unresolved::Chain>,
    join_input_columns: Option<&[crate::names::ColId]>,
    mut pipe_source: Option<ast_unresolved::Chain>,
) -> Result<(ast_resolved::Chain, BubbledState, bool)> {
    let reference = call.callee;
    let ast_unresolved::FunctorCall {
        marks, arguments, ..
    } = call;
    let function = reference.name_text().to_string();
    let function_stropped = reference.name_identifier().is_stropped();
    let namespace = (!reference.namespace_texts().is_empty()).then(|| reference.namespace_texts());
    let grounding: Option<ast_unresolved::GroundedPath> = None;

    // THE LANDING IS THE GROUP'S ONE SLOT, still unjudged: the member at
    // that index is the piped relation, standing at the formal R8 names.
    // Judging it — the first formal or the one authored `@`, a relation
    // formal and nothing else, never a search, never a displacement — is
    // the higher-order road's, below; the slot is spent there.
    let landing_index = arguments.ho().and_then(|part| part.landing);
    if let Some(index) = landing_index {
        if let Some(source) = arguments
            .ho_members()
            .nth(index)
            .and_then(|member| member.relation())
        {
            pipe_source.get_or_insert_with(|| source.clone());
        }
    }
    // The authored arguments are the members the author wrote: the landed
    // relation is the pipe's, not one of them.
    let authored_arguments = match landing_index {
        Some(index) => ast_unresolved::CallArguments::higher_order(
            arguments
                .ho_members()
                .enumerate()
                .filter(|(position, _)| *position != index)
                .map(|(_, member)| member.clone())
                .collect(),
        ),
        None => arguments.clone(),
    };

    // Check if this TVF is actually a higher-order view invocation
    if let Some(ref grounding) = grounding {
        for ns in &grounding.grounded_ns {
            let fq = ns.fq_string();
            if let Some(entity) = registry.consult.lookup_entity(
                &function,
                function_stropped,
                &fq,
                config.resolution_namespace.as_deref(),
            ) {
                if entity.entity_type == BootstrapEntityType::DqlHoTemporaryViewExpression {
                    let (table_bindings, scalar_spec, _pipe_idx) =
                        super::grounding::split_ho_first_parens(
                            &entity,
                            pipe_source.as_ref(),
                            &authored_arguments,
                            landing_index,
                            registry,
                            config.resolution_namespace.as_deref(),
                        )?;
                    return expand_ho_view(
                        &function,
                        &entity,
                        &scalar_spec,
                        &access,
                        table_bindings,
                        pipe_source.take(),
                        join_input.take(),
                        join_input_columns,
                        Some(&grounding.data_ns),
                        grounding,
                        registry,
                        outer_context,
                        config,
                        alias.clone(),
                    );
                }
            }
        }
    }

    // Namespace-qualified HO view (ns.ho_view(args)(*))
    if grounding.is_none() {
        if let Some(ref ns) = namespace {
            let fq = ns.join("::");
            if let Some(entity) = registry.consult.lookup_entity(
                &function,
                function_stropped,
                &fq,
                config.resolution_namespace.as_deref(),
            ) {
                if entity.entity_type == BootstrapEntityType::DqlHoTemporaryViewExpression {
                    // A pre-grounded namespace (created by ground!)
                    // carries its bound data world in default_data_ns —
                    // the same lookup the plain consulted-view path
                    // makes. Without it, the view body's unqualified
                    // table references resolve against nothing.
                    let data_ns = pre_grounded_data_ns(registry, &fq);
                    let ho_grounding = ast_unresolved::GroundedPath {
                        data_ns: data_ns
                            .clone()
                            .unwrap_or_else(ast_unresolved::NamespacePath::empty),
                        grounded_ns: vec![NamespacePath::from_parts(ns.clone())
                            .expect("canonical reference namespace is nonempty")],
                    };

                    let (table_bindings, scalar_spec, _pipe_idx) =
                        super::grounding::split_ho_first_parens(
                            &entity,
                            pipe_source.as_ref(),
                            &authored_arguments,
                            landing_index,
                            registry,
                            config.resolution_namespace.as_deref(),
                        )?;
                    return expand_ho_view(
                        &function,
                        &entity,
                        &scalar_spec,
                        &access,
                        table_bindings,
                        pipe_source.take(),
                        join_input.take(),
                        join_input_columns,
                        data_ns.is_some().then_some(&ho_grounding.data_ns),
                        &ho_grounding,
                        registry,
                        outer_context,
                        config,
                        alias.clone(),
                    );
                }
            }
        }
    }

    // Fallback: unqualified HO view via enlist!
    if grounding.is_none() {
        if let Some(entity) = registry.consult.lookup_enlisted_ho_view(
            &function,
            function_stropped,
            config.resolution_namespace.as_deref(),
        )? {
            let entity_ns = ast_unresolved::NamespacePath::from_fq_string(&entity.namespace)
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Invalid namespace for HO view '{}': {:?}", function, e),
                        format!("{:?}", e),
                    )
                })?;
            // An enlisted namespace can be pre-grounded too — same
            // default_data_ns lookup as the qualified path above.
            let data_ns = pre_grounded_data_ns(registry, &entity.namespace);
            let ho_grounding = ast_unresolved::GroundedPath {
                data_ns: data_ns
                    .clone()
                    .unwrap_or_else(ast_unresolved::NamespacePath::empty),
                grounded_ns: vec![entity_ns],
            };

            let (table_bindings, scalar_spec, _pipe_idx) = super::grounding::split_ho_first_parens(
                &entity,
                pipe_source.as_ref(),
                &authored_arguments,
                landing_index,
                registry,
                config.resolution_namespace.as_deref(),
            )?;
            return expand_ho_view(
                &function,
                &entity,
                &scalar_spec,
                &access,
                table_bindings,
                pipe_source.take(),
                join_input.take(),
                join_input_columns,
                data_ns.is_some().then_some(&ho_grounding.data_ns),
                &ho_grounding,
                registry,
                outer_context,
                config,
                alias.clone(),
            );
        }
    }

    // A glob-only relation access is the table spelling, not a zero-argument
    // TVF.  Keep this decision after the higher-order lookup roads so names
    // such as `exists(*)` still expand as views, while CTE labels and ordinary
    // tables retain the established `name(*)` default.
    let table_default = arguments.is_empty()
        || matches!(
            arguments.scalar_members(),
            [
                crate::pipeline::asts::core::operators::ScalarArgument::Spread(
                    crate::pipeline::asts::core::Spread::Glob(_)
                )
            ]
        );
    if table_default && !function.ends_with('!') {
        let identifier = ast_unresolved::QualifiedName {
            namespace_path: ast_unresolved::NamespacePath::from_parts(
                namespace.unwrap_or_default(),
            )
            .map_err(|error| {
                DelightQLError::parse_error(format!(
                    "invalid namespace on relation '{}': {:?}",
                    function, error
                ))
            })?,
            name: function.clone().into(),
        };
        return resolve_ground(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier,
                    alias,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
                cpr_schema: (),
            },
            access,
            registry,
            outer_context,
            config,
            grounding.as_ref(),
        )
        .map(|(relation, state)| (relation, state, false));
    }

    // A TVF argument that names a column of the enclosing row is resolved
    // HERE, where that row's columns are in hand: `json_each(|1|)` and
    // `json_each(data)` both have to reach an occurrence before generation,
    // and SQL has no ordinal syntax to fall back on.
    //
    // What comes back is a RESOLVED expression, kept beside the authored
    // list. Writing an occurrence back into the authored argument would put
    // a resolved state in a tree nobody has resolved.
    let member_domains: Vec<Option<&ast_unresolved::DomainExpression>> = match &arguments {
        crate::pipeline::asts::core::operators::CallArguments::None => Vec::new(),
        crate::pipeline::asts::core::operators::CallArguments::HigherOrder(part) => part
            .members
            .iter()
            .map(|argument| argument.scalar_domain())
            .collect(),
        crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => members
            .iter()
            .map(|member| member.scalar_domain())
            .collect(),
    };
    let mut bound_arguments: Vec<Option<ast_resolved::DomainExpression>> =
        vec![None; member_domains.len()];
    if let Some(context) = outer_context {
        for (index, domain) in member_domains.iter().enumerate() {
            if let Some(ast_unresolved::DomainExpression::Reference(Reference::Ordinal(
                ref ordinal,
            ))) = domain
            {
                let candidates = if let Some(ref qualifier) = ordinal.qualifier {
                    let spelling = registry
                        .identities
                        .intern(qualifier.as_str(), qualifier.is_stropped());
                    registry
                        .identities
                        .qualified_glob(registry.identities.canonical(spelling), context)
                } else {
                    crate::names::Candidates::from_vec(context.to_vec())
                };

                if candidates.is_empty() {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: column_ordinal_text(ordinal.position, false),
                        context: "No columns available for ordinal resolution in TVF argument"
                            .to_string(),
                    });
                }

                let idx = if ordinal.reverse {
                    if ordinal.position as usize > candidates.len() {
                        return Err(DelightQLError::ColumnNotFoundError {
                            column: column_ordinal_text(ordinal.position, true),
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
                            column: column_ordinal_text(0, false),
                            context: "Column positions start at 1".to_string(),
                        });
                    }
                    let pos = (ordinal.position - 1) as usize;
                    if pos >= candidates.len() {
                        return Err(DelightQLError::ColumnNotFoundError {
                            column: column_ordinal_text(ordinal.position, false),
                            context: format!(
                                "Position {} exceeds {} available columns",
                                ordinal.position,
                                candidates.len()
                            ),
                        });
                    }
                    pos
                };

                bound_arguments[index] = Some(ast_resolved::DomainExpression::Reference(
                    Reference::Named(NamedReference(ColumnOccurrence {
                        column: *candidates
                            .in_order()
                            .nth(idx)
                            .expect("validated ordinal index is in candidate order"),
                        explicit_qualifier: false,
                    })),
                ));
            } else if let Some(ast_unresolved::DomainExpression::Reference(Reference::Named(
                NamedReference(AuthoredColumn {
                    name, qualifier, ..
                }),
            ))) = domain
            {
                // A named argument is a reference and resolves as one. An
                // ordinal beside it already does, and leaving the name alone
                // carries an authored lvar past the phase that ends them —
                // the lowering then has a spelling where it needs a column.
                use super::unification::{unify_columns, ColumnReference, UnificationResult};
                let reference = ColumnReference::Named {
                    name: name.clone(),
                    qualifier: qualifier.clone(),
                };
                let explicit_qualifier = qualifier.is_some();
                let visible = context.iter().fold(Vec::new(), |mut scopes, column| {
                    let scope = registry.identities.scope_of(*column);
                    if !scopes.contains(&scope) {
                        scopes.push(scope);
                    }
                    scopes
                });
                let resolved =
                    unify_columns(vec![reference], context, &visible, &registry.identities)
                        .into_iter()
                        .next()
                        .expect("one reference produces one unification result");
                match resolved {
                    UnificationResult::Resolved(column) => {
                        bound_arguments[index] = Some(ast_resolved::DomainExpression::Reference(
                            Reference::Named(NamedReference(ColumnOccurrence {
                                column,
                                explicit_qualifier,
                            })),
                        ));
                    }
                    UnificationResult::Opaque => {
                        return Err(crate::pipeline::resolver::opaque_reference_refusal())
                    }
                    UnificationResult::Unresolved(column) => {
                        return Err(DelightQLError::column_not_found_error(
                            column,
                            "in TVF argument",
                        ))
                    }
                    UnificationResult::Refused(refusal) => return Err(refusal.into_error()),
                    UnificationResult::Ambiguous { column, tables } => {
                        return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            format!(
                                "Ambiguous column '{}' exists in scopes: {}",
                                column,
                                tables.join(", ")
                            ),
                            "in TVF argument",
                        ))
                    }
                }
            }
        }
    }

    // A bin relation the catalog knows must not fall through to the TVF
    // road: the fallback strips its namespace and compiles a phantom table
    // reference under the bare name. It is refused HERE with its identity —
    // execution for this entity lives at the effect boundary, which only
    // reaches the submission's own chain and its bindings.
    if let Some(ref ns) = namespace {
        let fq = ns.join("::");
        if let Some(entity) = registry.consult.lookup_entity(
            &function,
            function_stropped,
            &fq,
            config.resolution_namespace.as_deref(),
        ) {
            if entity.entity_type == BootstrapEntityType::BinRelation {
                return Err(runtime_served_unreached_error(
                    &entity.name,
                    entity.entity_type,
                ));
            }
        }
    }

    // A TVF the catalog describes publishes a known heading; one it does
    // not is the default-transpilation case, and its heading is the
    // target's until a caller pattern declares one.
    let described = get_tvf_schema(&function, alias.as_deref(), &registry.identities);

    if described.is_none() {
        if config.permissive {
            eprintln!(
                "WARNING: Unknown TVF '{}' - treating as generic table function",
                function
            );
            // Keep Unknown schema
        } else {
            return Err(DelightQLError::parse_error(format!(
                "Unknown TVF: {}",
                function
            )));
        }
    }

    // A TVF heading — the ampersand form's tail or a second parens — is
    // heading-shaped: each slot NAMES a published column of the function's
    // schema, an ordered projection, never a slot-by-slot binding (the
    // function's arity lives in its argument list, not its heading).
    // Binding happens here because resolution is where authored characters
    // stop: the refiner reads occurrences and refuses an authored lvar.
    let (resolved_spec, schema) = match (&access, described) {
        (ast_unresolved::Access::Slots(slots), Some(scope)) => {
            let source_heading = registry.identities.known_heading(scope)?;
            let (table_name, stropped) = match &alias {
                Some(alias) => (alias.as_str(), alias.is_stropped()),
                None => (function.as_str(), false),
            };
            let hint = registry.identities.intern(table_name, stropped);
            let output_scope = registry.identities.mint_derived_scope(
                crate::names::ScopeOrigin::UserAlias { of: scope },
                crate::names::Hint::User(hint),
            );
            let mut occurrences = Vec::with_capacity(slots.len());
            let mut bound: Vec<crate::names::Sym> = Vec::new();
            for slot in slots {
                let ast_unresolved::Slot::Bind(crate::pipeline::asts::core::WrittenBinder {
                    name,
                    ..
                }) = slot
                else {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "the heading of TVF '{}' is an ordered projection of \
                             the function's columns — each slot must be a bare \
                             column name",
                            table_name
                        ),
                        "in TVF heading",
                    ));
                };
                let sym = registry
                    .identities
                    .canonical(registry.identities.intern(name, name.is_stropped()));
                // A heading's names are programmer-authored, so they obey the
                // uniqueness a projection's do. Without this the second slot
                // is published as `name_2` and then READ as though the
                // function offered a column by that name.
                if bound.contains(&sym) {
                    return Err(DelightQLError::validation_error_categorized(
                        "constraint",
                        format!(
                            "Duplicate column '{name}' in the heading of TVF \
                             '{table_name}': programmer-authored names must be \
                             unique. Rename one with 'as' to disambiguate"
                        ),
                        "in TVF heading",
                    ));
                }
                bound.push(sym);
                // Every carrier is enumerated, never the first: the hard-coded
                // schemas happen to publish unique names, but the contract
                // here is a published schema and runtime introspection does
                // not establish that.
                let mut carriers = source_heading
                    .iter()
                    .copied()
                    .filter(|column| registry.identities.published_sym(*column) == Some(sym));
                let source = match (carriers.next(), carriers.next()) {
                    (Some(source), None) => source,
                    (None, _) => {
                        return Err(DelightQLError::column_not_found_error(
                            name.to_string(),
                            format!("in the heading of TVF '{}'", table_name),
                        ))
                    }
                    (Some(_), Some(_)) => {
                        return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            format!(
                                "TVF '{table_name}' publishes '{name}' more than \
                                 once, so a heading slot naming it reaches no \
                                 single column"
                            ),
                            "in TVF heading",
                        ))
                    }
                };
                let column = registry.identities.republish_column(
                    source,
                    output_scope,
                    crate::names::Republish::Passthrough,
                    registry.identities.published(source),
                    crate::names::Addressing::Published,
                    |_| {},
                );
                // A heading slot NAMES a dimension, which is what a binding
                // slot is. It stays one across the phase edge: only the
                // payload changes, from the written name to the column.
                occurrences.push(ast_resolved::Slot::Bind(column));
            }
            let occurrences = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(occurrences)
                .expect("a TVF heading with slots binds at least one");
            (ast_resolved::Access::Slots(occurrences), output_scope)
        }
        // The target published nothing to project FROM, so the caller
        // pattern is not a projection of a heading — it IS the heading. One
        // slot per dimension of the full width, declared at the mention.
        // Nothing checks it against the target, for the same reason nothing
        // checks `upper:(x)`: a name this compiler cannot verify is the
        // target's to disagree with.
        (ast_unresolved::Access::Slots(slots), None) => {
            let (table_name, stropped) = match &alias {
                Some(alias) => (alias.as_str(), alias.is_stropped()),
                None => (function.as_str(), false),
            };
            let hint = registry.identities.intern(table_name, stropped);
            let declared_scope = registry.identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::User(hint),
                None,
            );
            let mut occurrences = Vec::with_capacity(slots.len());
            let mut bound: Vec<crate::names::Sym> = Vec::new();
            for (position, slot) in slots.iter().enumerate() {
                // A named slot publishes; a slot that names nothing is a
                // dimension all the same, and holds its place latently.
                let declared = match slot {
                    ast_unresolved::Slot::Bind(crate::pipeline::asts::core::WrittenBinder {
                        name,
                        ..
                    }) => Some((
                        name.as_str(),
                        registry.identities.intern(name, name.is_stropped()),
                    )),
                    _ => None,
                };
                let published = declared.map(|(_, spelling)| spelling);
                if let Some((name, spelling)) = declared {
                    let sym = registry.identities.canonical(spelling);
                    if bound.contains(&sym) {
                        return Err(DelightQLError::validation_error_categorized(
                            "constraint",
                            format!(
                                "Duplicate column '{}' in the declared heading of \
                                 '{table_name}': programmer-authored names must be \
                                 unique. Rename one with 'as' to disambiguate",
                                name
                            ),
                            "in TVF heading",
                        ));
                    }
                    bound.push(sym);
                }
                let column = registry.identities.mint_column(
                    declared_scope,
                    crate::names::ColumnOrigin::Bound {
                        position: position as u32,
                    },
                    published,
                    if published.is_some() {
                        crate::names::Addressing::Published
                    } else {
                        crate::names::Addressing::Latent
                    },
                    crate::names::ValueFacts::default(),
                );
                // A heading slot NAMES a dimension, which is what a binding
                // slot is. It stays one across the phase edge: only the
                // payload changes, from the written name to the column.
                occurrences.push(ast_resolved::Slot::Bind(column));
            }
            let occurrences = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(occurrences)
                .expect("a declared heading has at least one dimension");
            (ast_resolved::Access::Slots(occurrences), declared_scope)
        }
        // Nothing was declared and nothing is published: the relation still
        // has an identity, and only its heading is unknown.
        (_, None) => {
            let opaque_scope = registry.identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
                None,
            );
            registry.identities.mark_heading_opaque(opaque_scope);
            (resolve_schema_free_access(&access)?, opaque_scope)
        }
        (_, Some(scope)) => (resolve_schema_free_access(&access)?, scope),
    };

    // What this access offers upward for validation. An opaque relation
    // offers nothing to enumerate — and that is not the claim that it has
    // no dimensions. A name used against it refuses where the name is
    // resolved, not here.
    let state = match registry.identities.heading(schema) {
        crate::names::HeadingKnowledge::Known(heading) => {
            BubbledState::resolved(heading.to_vec(), &registry.identities)
        }
        crate::names::HeadingKnowledge::Opaque => BubbledState::opaque(schema),
    };

    // Resolve namespace to physical backend schema + connection routing.
    // Same logic as Ground passthrough: resolve namespace, track connection_id,
    // replace DQL namespace path with physical schema name for SQL generation.
    let namespace_path = namespace.as_ref().map(|parts| {
        NamespacePath::from_parts(parts.clone()).expect("canonical reference namespace is nonempty")
    });
    let resolved_namespace = if let Some(ref ns) = namespace_path {
        if !ns.is_empty() {
            match registry.database.resolve_namespace(ns) {
                Ok(Some((physical_schema, conn_id))) => {
                    registry.track_connection_id(conn_id);
                    // physical_schema=None means tables are in `main` of that connection
                    physical_schema.map(|s| NamespacePath::single(&*s))
                }
                _ => namespace_path.clone(),
            }
        } else {
            namespace_path.clone()
        }
    } else {
        None
    };

    // Convert ho_arguments from Unresolved to Resolved phase for non-HO TVFs.
    // An argument the binder above already resolved travels as itself; the
    // rest are names nothing here can look up, and drop as they always have.
    let resolved_ho_arguments: Vec<
        crate::pipeline::asts::core::operators::HoArgument<crate::pipeline::asts::core::Resolved>,
    > = member_domains
        .iter()
        .zip(bound_arguments)
        .filter_map(|(domain, bound)| {
            bound
                .or_else(|| convert_domain_expression((*domain)?, &registry.identities).ok())
                .map(|value| {
                    crate::pipeline::asts::core::operators::HoArgument::Value(
                        crate::pipeline::asts::core::ArgumentValue::plain(value),
                    )
                })
        })
        .collect();

    let function_spelling = registry.identities.intern(function.as_str(), false);
    let function_namespace = resolved_namespace
        .as_ref()
        .map(|path| {
            path.iter()
                .map(|item| {
                    registry
                        .identities
                        .intern(item.name.as_str(), item.name.is_stropped())
                })
                .collect()
        })
        .unwrap_or_default();
    let function = registry
        .identities
        .mint_function(function_spelling, function_namespace);
    let resolved = ast_resolved::Relation::FunctorCall {
        alias: (),
        call: ast_resolved::SealedCall::from_inner(
            ast_resolved::FunctorCall {
                callee: function,
                arguments: crate::pipeline::asts::core::operators::CallArguments::higher_order(
                    resolved_ho_arguments,
                ),
                marks,
            },
            false,
        ),
        cpr_schema: schema,
    };

    // The access travels beside the call, in the position it was written:
    // after it, on what the call publishes.
    Ok((
        ast_resolved::Chain::read(resolved, resolved_spec, schema),
        state,
        false,
    ))
}

/// Resolve an InnerRelation variant (subquery inside parentheses).
///
/// INNER-RELATION: table(|> pipeline) or table(, correlation |> pipeline)
/// Resolves the subquery and keeps pattern as Indeterminate.
/// The refiner will classify it into UDT/CDT-SJ/CDT-GJ/CDT-WJ.
pub(super) fn resolve_inner_relation(
    rel: ast_unresolved::Relation,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    let ast_unresolved::Relation::InnerRelation {
        pattern,
        preminted_scope,
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

    // Resolve the inner subquery, also collecting any pipe-level CFEs
    // from the sub-fold so the caller can propagate them to the outer fold.
    // The interior resolves under the access's self-name — the alias when
    // authored, the access name otherwise — so its spine stages keep that
    // name answering for the CURRENT heading.
    let interior_self = {
        let (text, stropped) = match &alias {
            Some(alias) => (alias.as_str(), alias.is_stropped()),
            None => (identifier.name.as_str(), identifier.name.is_stropped()),
        };
        registry
            .identities
            .canonical(registry.identities.intern(text, stropped))
    };
    let (resolved_subquery, _bubbled) = super::resolve_interior_expression(
        (*subquery).clone(),
        registry,
        outer_context,
        config,
        grounding,
        Some(interior_self),
    )?;

    // Extract schema from resolved subquery
    let schema = super::helpers::extraction::extract_cpr_schema(&resolved_subquery);

    // Relabel columns with the inner relation's effective name (alias if present, otherwise identifier)
    // This ensures qualified globs like `users.*` or `u.*` can match these columns
    let schema_columns = schema;
    let input = schema_columns;
    let inner_scope = match preminted_scope {
        Some(scope) => {
            crate::probe::probe!(
                preminted,
                "consume {scope:?} as {} origin={:?} heading={} input={input:?}",
                identifier.name,
                registry.identities.origin_of(scope),
                registry.identities.known_heading(scope)?.len()
            );
            let compatible = match registry.identities.origin_of(scope) {
                crate::names::ScopeOrigin::AnonRelation => true,
                crate::names::ScopeOrigin::UserAlias { of }
                | crate::names::ScopeOrigin::PipeStage { input: of }
                | crate::names::ScopeOrigin::Wrap { input: of, .. }
                | crate::names::ScopeOrigin::Cte { input: of, .. }
                | crate::names::ScopeOrigin::SetArm { of, .. } => of == input,
                crate::names::ScopeOrigin::ErHop { chain, .. } => chain == input,
                crate::names::ScopeOrigin::Interior { of } => {
                    registry.identities.scope_of(of) == input
                }
                crate::names::ScopeOrigin::BaseTable { .. }
                | crate::names::ScopeOrigin::Join { .. }
                | crate::names::ScopeOrigin::Resolution { .. }
                | crate::names::ScopeOrigin::HoCarrier { .. }
                | crate::names::ScopeOrigin::Scratch { .. } => false,
            };
            if !compatible {
                return Err(DelightQLError::validation_error(
                    format!(
                        "pre-minted inner-relation scope {scope:?} is incompatible with \
                         resolved input {input:?}"
                    ),
                    "internal inner-relation identity invariant",
                ));
            }
            if !registry.identities.known_heading(scope)?.is_empty() {
                return Err(DelightQLError::validation_error(
                    format!("pre-minted inner-relation scope {scope:?} is already populated"),
                    "internal inner-relation identity invariant",
                ));
            }
            scope
        }
        None => {
            let (scope_origin, scope_hint) = if let Some(authored_alias) = &alias {
                let spelling = registry
                    .identities
                    .intern(authored_alias.as_str(), authored_alias.is_stropped());
                (
                    crate::names::ScopeOrigin::UserAlias { of: input },
                    crate::names::Hint::User(spelling),
                )
            } else {
                // The wrap is the compiler's, but the access is authored and
                // named: `purchases(…interior…)` answers to `purchases` from
                // outside exactly as `purchases(*)` does. Only an alias
                // replaces that name; the wrap must not consume it.
                let spelling = registry
                    .identities
                    .intern(identifier.name.as_str(), identifier.name.is_stropped());
                (
                    crate::names::ScopeOrigin::Wrap {
                        input,
                        why: crate::names::WrapReason::Projection,
                    },
                    crate::names::Hint::User(spelling),
                )
            };
            registry
                .identities
                .mint_derived_scope(scope_origin, scope_hint)
        }
    };
    let scope_columns = |columns: &[crate::names::ColId]| {
        columns
            .iter()
            .map(|column| {
                registry.identities.republish_column(
                    *column,
                    inner_scope,
                    crate::names::Republish::BoundaryExport,
                    registry.identities.published(*column),
                    crate::names::Addressing::Published,
                    |_| {},
                )
            })
            .collect::<Vec<_>>()
    };
    let input_columns = registry.identities.known_heading(input)?;
    let relabeled_i_provide = scope_columns(&input_columns.to_vec());
    let schema = inner_scope;

    // Also relabel the bubbled state's i_provide columns so the join sees the correct table names
    let bubbled = super::BubbledState::resolved(relabeled_i_provide, &registry.identities);

    // Create resolved InnerRelation with Indeterminate pattern
    // Refiner will classify this later
    let resolved = ast_resolved::Relation::InnerRelation {
        pattern: ast_resolved::InnerRelationPattern::Indeterminate {
            identifier: convert_qualified_name(identifier),
            subquery: Box::new(resolved_subquery),
        },
        preminted_scope,
        alias,
        outer,
        cpr_schema: schema,
    };

    Ok((ast_resolved::Chain::relation(resolved), bubbled))
}

/// Relabel the `i_provide` columns of a BubbledState with a new table name.
///
/// Consulted entities (facts and views) resolve their bodies internally, producing
/// columns with the entity's original table name. When the entity is aliased
/// (e.g., `country_tier(*) as ct`), downstream pipes need `i_provide` columns
/// to carry the alias so qualified refs like `ct.Country` can match.
/// Relabel a scope's column table names with an alias.
///
/// Convert a resolved Query back to a Chain for HO view expansion.
///
/// When the HO view body has no CTEs, the Query is unwrapped transparently.
/// When it has CTEs, it's wrapped in a ConsultedView to provide a subquery boundary.
pub(super) fn ho_view_query_to_relational(
    resolved_query: ast_resolved::Query,
    bubbled: super::BubbledState,
    view_name: &str,
    user_alias: Option<SqlIdentifier>,
    identities: &crate::names::Registry,
) -> crate::error::Result<(ast_resolved::Chain, super::BubbledState)> {
    match resolved_query.into_bare_body() {
        Ok(expr) => {
            if let Some(ref alias) = user_alias {
                let bubbled = relabel_bubbled_with_alias(
                    bubbled,
                    alias,
                    BoundaryAnswering::Silent,
                    identities,
                );
                Ok((expr, bubbled))
            } else {
                Ok((expr, bubbled))
            }
        }
        Err(query_with_ctes) => {
            let query_with_ctes = *query_with_ctes;
            let body_schema =
                super::helpers::extraction::extract_cpr_schema_from_query(&query_with_ctes)?;
            let (_alias, access_scope) =
                access_boundary_export(&user_alias, view_name, body_schema, identities);
            let bubbled = BubbledState::resolved(
                identities.known_heading(access_scope)?.to_vec(),
                identities,
            );
            Ok((
                ast_resolved::Chain::relation(ast_resolved::Relation::ConsultedView {
                    body: Box::new(query_with_ctes),
                    scoped: access_scope,
                    outer: false,
                }),
                bubbled,
            ))
        }
    }
}

/// How columns crossing a relabel boundary are ADDRESSED afterwards.
/// Every crossing must say — there is no default, so a new road cannot
/// forget the question the way the existing silent roads did.
pub(super) enum BoundaryAnswering {
    /// The columns answer to this surface name (the user's alias or
    /// the bare entity name) in addition to the pushed SQL qualifier.
    #[allow(dead_code)]
    AnswersTo(SqlIdentifier),
    /// No answering channel: only the pushed qualifier (often a
    /// compiler-owned occurrence) reaches presence, so surface-name-qualified
    /// references refuse. This is the known gap on the consulted-view
    /// roads — unaliased `pv(*), pv.a == 1` refuses while
    /// `pv(*) as p, p.a == 1` works. Kept until the presence-tier fix
    /// lands; do not choose it for a NEW road without a ruling.
    Silent,
}

pub(super) fn relabel_bubbled_with_alias(
    bubbled: super::BubbledState,
    effective_name: &str,
    answering: BoundaryAnswering,
    identities: &crate::names::Registry,
) -> super::BubbledState {
    let input = identities
        .common_scope(&bubbled.i_provide)
        .unwrap_or_else(|| {
            identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
                None,
            )
        });
    let spelling = identities.intern(effective_name, false);
    let scope = identities.mint_derived_scope(
        crate::names::ScopeOrigin::UserAlias { of: input },
        crate::names::Hint::User(spelling),
    );
    let access = match answering {
        BoundaryAnswering::AnswersTo(access) => {
            let spelling = identities.intern(access.as_str(), access.is_stropped());
            Some(identities.canonical(spelling))
        }
        BoundaryAnswering::Silent => None,
    };
    let relabeled: Vec<crate::names::ColId> = bubbled
        .i_provide
        .into_iter()
        .map(|column| {
            identities.republish_column(
                column,
                scope,
                crate::names::Republish::BoundaryExport,
                identities.published(column),
                access
                    .map(crate::names::Addressing::AnsweringTo)
                    .unwrap_or(crate::names::Addressing::Published),
                |_| {},
            )
        })
        .collect();
    super::BubbledState::resolved(relabeled, identities)
}

/// Relabel column metadata with an alias: if an alias is present, update the
/// table_name on each column to reflect the alias. Otherwise return a clone.
fn relabel_columns_with_alias(
    input: crate::names::ScopeId,
    alias: &Option<SqlIdentifier>,
    identities: &crate::names::Registry,
) -> Vec<crate::names::ColId> {
    if let Some(alias_name) = alias {
        let spelling = identities.intern(alias_name.as_str(), alias_name.is_stropped());
        let scope = identities.mint_derived_scope(
            crate::names::ScopeOrigin::UserAlias { of: input },
            crate::names::Hint::User(spelling),
        );
        identities
            .heading(input)
            .columns_seen()
            .into_iter()
            .map(|column| {
                identities.republish_column(
                    column,
                    scope,
                    crate::names::Republish::BoundaryExport,
                    identities.published(column),
                    crate::names::Addressing::Published,
                    |_| {},
                )
            })
            .collect()
    } else {
        identities.heading(input).columns_seen()
    }
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
    scalar_spec: &ast_unresolved::Access,
    positions: &[crate::pipeline::asts::ddl::HoPositionInfo],
    function: &str,
    has_outer_context: bool,
) -> Result<()> {
    use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode};

    let exprs = match scalar_spec {
        ast_unresolved::Access::Slots(exprs) => exprs,
        ast_unresolved::Access::All => return Ok(()),
        _ => return Ok(()),
    };

    // Get scalar positions from position analysis
    let scalar_positions: Vec<_> = positions
        .iter()
        .enumerate()
        .filter(|(_, p)| matches!(p.column_kind, HoColumnKind::Scalar))
        .collect();

    for (idx, slot) in exprs.iter().enumerate() {
        let Some((abs_pos, pos_info)) = scalar_positions.get(idx) else {
            continue;
        };
        if pos_info.ground_mode != HoGroundMode::MixedGround {
            continue;
        }
        // Check if the expression is a bare identifier (lvar) — not a literal
        // A crossed slot constrains with a truth read as a value; it is not
        // a bare lvar, so it counts as ground for this judgment.
        let Some(expr) = slot.term() else {
            continue;
        };
        let is_ground = match expr {
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            ) => true,
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn { .. },
            ))) => false,
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
/// 8. apply_ho_access_pattern(access_spec, ...) binds the trailing access group
pub(super) fn expand_ho_view(
    function: &str,
    entity: &crate::resolution::registry::ConsultedEntity,
    scalar_spec: &ast_unresolved::Access,
    access_spec: &ast_unresolved::Access,
    table_bindings: crate::pipeline::query_features::HoParamBindings,
    pipe_source: Option<ast_unresolved::Chain>,
    join_input: Option<ast_unresolved::Chain>,
    join_input_columns: Option<&[crate::names::ColId]>,
    data_ns: Option<&ast_unresolved::NamespacePath>,
    grounding: &ast_unresolved::GroundedPath,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    user_alias: Option<SqlIdentifier>,
) -> Result<(ast_resolved::Chain, super::BubbledState, bool)> {
    let (expr, bubbled, absorbed_join_input) = expand_ho_view_body(
        function,
        entity,
        scalar_spec,
        table_bindings,
        pipe_source,
        join_input,
        join_input_columns,
        data_ns,
        grounding,
        registry,
        outer_context,
        config,
        user_alias.clone(),
    )?;
    let (expr, bubbled) = apply_ho_access_pattern(
        access_spec,
        expr,
        bubbled,
        function,
        &user_alias,
        &registry.identities,
    )?;
    Ok((expr, bubbled, absorbed_join_input))
}

/// The trailing access group on a parameterized-rule call is ordinary
/// argumentative access over the declared heading — uniform with plain
/// rules and receipt access (ruling R-3). The declared heading is the
/// expansion's visible columns; hygienic carriers (injected scalar
/// discriminators, param labels) are not part of it and pass through
/// hidden. WHERE constraints use unqualified column references because
/// HO ConsultedViews get CTE-wrapped and a qualifier would be wrong.
fn apply_ho_access_pattern(
    access_spec: &ast_unresolved::Access,
    expr: ast_resolved::Chain,
    bubbled: super::BubbledState,
    function: &str,
    user_alias: &Option<SqlIdentifier>,
    identities: &crate::names::Registry,
) -> Result<(ast_resolved::Chain, super::BubbledState)> {
    let ast_unresolved::Access::Slots(pattern_exprs) = access_spec else {
        // Glob access stays payload-transparent.
        return Ok((expr, bubbled));
    };

    let body_schema = super::helpers::extraction::extract_cpr_schema(&expr);
    let schema_cols = identities.known_heading(body_schema)?;

    let visible_count = schema_cols
        .iter()
        .filter(|column| identities.addressing(**column) != crate::names::Addressing::Hygienic)
        .count();
    if pattern_exprs.len() != visible_count {
        return Err(DelightQLError::validation_error(
            format!(
                "Positional pattern incomplete - rule '{}' has {} columns but pattern specifies {} elements",
                function, visible_count, pattern_exprs.len()
            ),
            "Positional pattern validation".to_string(),
        ));
    }

    let mut where_constraints = Vec::new();
    let output_scope = identities.mint_derived_scope(
        crate::names::ScopeOrigin::Wrap {
            input: body_schema,
            why: crate::names::WrapReason::Projection,
        },
        crate::names::Hint::None,
    );
    let resolved_ref = |column| {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence {
                column,
                // The access constraint is evaluated against the expanded relation's
                // published row. Preserve qualification so a surrounding EXISTS
                // cannot reinterpret the same column as an outer or unqualified
                // reference after the consulted body is wrapped.
                explicit_qualifier: true,
            },
        )))
    };

    // The access law is classified in ONE place — PatternResolver's
    // positional_to_selections — and lowered here. This road differs from
    // the bare-table road only in lowering: an HO body is CTE-wrapped, so
    // a slot that publishes nothing must still ride the inner projection
    // for the WHERE to reference it, where a bare table can simply omit
    // the column.
    let visible: Vec<crate::names::ColId> = schema_cols
        .iter()
        .filter(|column| identities.addressing(**column) != crate::names::Addressing::Hygienic)
        .copied()
        .collect();
    let selections =
        super::PatternResolver::new().positional_to_selections(pattern_exprs, &visible)?;

    let mut sel_iter = selections.into_iter();
    let mut output_for_input = std::collections::HashMap::new();
    for column in schema_cols {
        if identities.addressing(column) == crate::names::Addressing::Hygienic {
            let output = identities.republish_column(
                column,
                output_scope,
                crate::names::Republish::Passthrough,
                identities.published(column),
                crate::names::Addressing::Hygienic,
                |_| {},
            );
            output_for_input.insert(column, output);
            continue;
        }
        let sel = sel_iter
            .next()
            .expect("arity checked above: one selection per visible column");
        match sel.constraint {
            // Introduces no name: publishes nothing, contributes nothing.
            Some(super::pattern_resolver::PatternConstraint::Skip) => {
                let output = identities.republish_column(
                    column,
                    output_scope,
                    crate::names::Republish::Passthrough,
                    identities.published(column),
                    crate::names::Addressing::Hygienic,
                    |_| {},
                );
                output_for_input.insert(column, output);
            }
            // Introduces no name: publishes nothing, contributes a filter.
            Some(super::pattern_resolver::PatternConstraint::Literal(value)) => {
                let output = identities.republish_column(
                    column,
                    output_scope,
                    crate::names::Republish::Passthrough,
                    identities.published(column),
                    crate::names::Addressing::Hygienic,
                    |_| {},
                );
                output_for_input.insert(column, output);
                where_constraints.push(ast_resolved::TruthExpression::Comparison(Comparison {
                    operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                    left: Box::new(resolved_ref(output)),
                    right: Box::new(ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(value),
                    )),
                }));
            }
            // Introduces no name: publishes nothing, contributes an
            // equality. Null-safe — a within-row selection cannot
            // multiply rows, so a both-null row instances the pattern.
            Some(super::pattern_resolver::PatternConstraint::SelfUnify { first_position }) => {
                let output = identities.republish_column(
                    column,
                    output_scope,
                    crate::names::Republish::Passthrough,
                    identities.published(column),
                    crate::names::Addressing::Hygienic,
                    |_| {},
                );
                output_for_input.insert(column, output);
                let first = output_for_input
                    .get(&visible[first_position])
                    .copied()
                    .unwrap_or(visible[first_position]);
                where_constraints.push(ast_resolved::TruthExpression::Comparison(Comparison {
                    operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                    left: Box::new(resolved_ref(first)),
                    right: Box::new(resolved_ref(output)),
                }));
            }
            // Introduces a name: publishes under it.
            None => {
                let published = identities.intern(&sel.output_name, false);
                let output = identities.republish_column(
                    column,
                    output_scope,
                    crate::names::Republish::Rename,
                    Some(published),
                    crate::names::Addressing::Bare,
                    |_| {},
                );
                output_for_input.insert(column, output);
            }
            other => {
                return Err(DelightQLError::validation_error_categorized(
                    "resolution/ho_access/pattern_shape",
                    format!(
                        "'{function}' access pattern position {}: \
                         only bare names, `_`, and literals bind on a parameterized-rule \
                         access pattern for now. Got: {other:?}. Bind the column to a name \
                         and constrain it with an explicit predicate instead.",
                        sel.source_position + 1,
                    ),
                    "parameterized-rule access pattern",
                ));
            }
        }
    }
    let output_columns = identities.known_heading(output_scope)?;

    let mut expr = expr;
    update_relation_cpr_schema(&mut expr, output_scope);

    if !where_constraints.is_empty() {
        let combined = combine_where_constraints(where_constraints);
        expr = expr.then(ast_resolved::Continuation::Restrict {
            condition: combined,
            origin: ast_resolved::FilterOrigin::PositionalLiteral {
                source: output_scope,
            },
            cpr_schema: output_scope,
        });
    }

    let final_bubbled = super::BubbledState::resolved(output_columns.to_vec(), identities);
    let final_bubbled = if let Some(alias) = user_alias {
        relabel_bubbled_with_alias(final_bubbled, alias, BoundaryAnswering::Silent, identities)
    } else {
        final_bubbled
    };
    Ok((expr, final_bubbled))
}

fn expand_ho_view_body(
    function: &str,
    entity: &crate::resolution::registry::ConsultedEntity,
    scalar_spec: &ast_unresolved::Access,
    table_bindings: crate::pipeline::query_features::HoParamBindings,
    pipe_source: Option<ast_unresolved::Chain>,
    join_input: Option<ast_unresolved::Chain>,
    join_input_columns: Option<&[crate::names::ColId]>,
    data_ns: Option<&ast_unresolved::NamespacePath>,
    grounding: &ast_unresolved::GroundedPath,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    user_alias: Option<SqlIdentifier>,
) -> Result<(ast_resolved::Chain, super::BubbledState, bool)> {
    log::debug!(
        "Expanding HO view '{}' (unified) from namespace '{}'",
        function,
        entity.namespace,
    );
    let had_pipe_source = pipe_source.is_some();

    // Validate arity for argumentative params that received table references.
    super::grounding::validate_argumentative_arity(&table_bindings, registry)?;

    // Build remap from argumentative lvar names to actual column names.
    // E.g., V(k, l) bound to refs(key, label) → {k → key, l → label}.

    // Validate mixed ground params from position analysis.
    let group = crate::ddl::reconstruct::group(&entity.definition).ok();
    let positions = if !entity.positions.is_empty() {
        entity.positions.clone()
    } else {
        group
            .as_ref()
            .map(super::grounding::build_ho_position_analysis)
            .unwrap_or_default()
    };
    let defs: &[crate::pipeline::asts::ddl::Clause] = group.as_ref().map_or(&[], |g| g.clauses());
    let positions = super::grounding::ensure_position_column_names(positions, defs);

    // Validate scalar_spec against positions: reject unbound identifiers at MixedGround positions.
    validate_scalar_spec_mixed_ground(
        scalar_spec,
        &positions,
        function,
        had_pipe_source || outer_context.is_some(),
    )?;

    // A provable miss is an error, not an empty relation: a call-site
    // literal at a PureGround position matching no clause head refuses
    // with the declared spellings.
    super::grounding::refuse_provable_ground_miss(function, scalar_spec, &positions)?;

    let pipe_source_cte = match (pipe_source, table_bindings.pipe_carrier.clone()) {
        (Some(source), Some((formal, scope)))
            if table_bindings.table_scope_params.get(&formal) == Some(&scope) =>
        {
            Some((formal, scope, source))
        }
        (None, None) => None,
        _ => {
            return Err(DelightQLError::parse_error(
                "a higher-order pipe source and its structural landing disagree",
            ))
        }
    };

    // A call-site lvar makes the caller input a structural carrier inside
    // every clause, where free heads bind it and ground heads constrain it.
    let has_free_scalars = table_bindings.scalar_params.values().any(|expr| {
        matches!(
            expr,
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn { .. }
            )))
        )
    });
    let (join_input_cte, absorbed_join_input) = if has_free_scalars {
        if let Some(input_expr) = join_input {
            let scope = registry.identities.mint_derived_scope(
                crate::names::ScopeOrigin::HoCarrier {
                    role: crate::names::HoRole::ScalarInput,
                },
                crate::names::Hint::Prefix("ho"),
            );
            (Some((scope, input_expr)), true)
        } else {
            (None, false)
        }
    } else {
        (None, false)
    };

    // Capture which scalar params are literal-bound BEFORE table_bindings is moved.
    // Used later to mark ground scalar columns as hygienic in glob call-sites.
    let ground_literal_scalar_names: Vec<String> = table_bindings
        .scalar_params
        .iter()
        .filter(|(_, expr)| {
            matches!(
                expr,
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(_)
                )
            )
        })
        .map(|(name, _)| name.clone())
        .collect();

    // Build the squished relation (ALL clauses, no pre-filtering)
    let squished_query = super::grounding::build_squished_relation(
        function,
        entity,
        table_bindings,
        pipe_source_cte,
        join_input_cte,
        data_ns,
        config.resolution_namespace.clone(),
    )?;

    // The squished body resolves under the entity's scope. Caller-authored
    // pipe/join/argument carriers each carry their concrete authored namespace
    // in CteResolutionOwner, so the inline resolver changes scope only for
    // those terms. This is shared by grounded, qualified, enlisted, relation,
    // and piped invocation forms; no alias dance or ambient caller override.
    let entity_config = if entity.namespace.is_empty() || entity.namespace == "main" {
        config.clone()
    } else {
        ResolutionConfig {
            resolution_namespace: Some(entity.namespace.clone()),
            ..config.clone()
        }
    };
    let retained_outer;
    let body_outer_context = if absorbed_join_input {
        let absorbed_columns = join_input_columns.unwrap_or_default();
        retained_outer = outer_context.map(|columns| {
            columns
                .iter()
                .copied()
                .filter(|column| !absorbed_columns.contains(column))
                .collect::<Vec<_>>()
        });
        retained_outer.as_deref()
    } else {
        outer_context
    };
    let resolve_result = super::resolve_query_inline(
        squished_query,
        registry,
        body_outer_context,
        &entity_config,
        Some(grounding),
    );

    let (resolved_query, bubbled) = resolve_result?;

    // Convert to ConsultedView relation
    let (resolved_expr, bubbled) = ho_view_query_to_relational(
        resolved_query,
        bubbled,
        function,
        user_alias.clone(),
        &registry.identities,
    )?;

    // Hygienic binders (clause-head-catechism item 14a): a plain scalar param
    // (PureUnbound — a `Scalar` in every clause, injected as no column of its
    // own) whose name collides with a column the body would otherwise resolve
    // to silently CAPTURES it. `g(age)(*) :- users(*), age > 40` with a
    // users.age column: the substitution turns `age > 40` into the tautology
    // `50 > 40` AND the scalar-column hygiene pass consumes users.age from the
    // glob output — both silent. Refuse loudly instead. Checked here at
    // expansion (call) time, where body relations carry real schemas: this
    // catches both concretely-named bodies (users) and glob-param bodies (T(*))
    // once the call supplies a concrete table. GroundScalar/MixedGround
    // positions are exempt — their same-named column is a synthetic
    // discriminator injected by inject_scalar_columns, not a captured body
    // column (e.g. tagged("young", T(*))(*)).
    {
        use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode};
        let body_schema = super::helpers::extraction::extract_cpr_schema(&resolved_expr);
        let body_cols: Vec<crate::names::ColId> =
            registry.identities.known_heading(body_schema)?.to_vec();
        for pos in &positions {
            if !matches!(pos.column_kind, HoColumnKind::Scalar)
                || pos.ground_mode != HoGroundMode::PureUnbound
            {
                continue;
            }
            let Some(param_name) = pos.column_name.as_deref() else {
                continue;
            };
            let spelling = registry.identities.intern(param_name, false);
            let param = registry.identities.canonical(spelling);
            let collisions: Vec<String> = body_cols
                .iter()
                .filter_map(|column| {
                    if registry.identities.published_sym(*column) != Some(param) {
                        return None;
                    }
                    let mut occurrence = *column;
                    loop {
                        if matches!(
                            registry
                                .identities
                                .origin_of(registry.identities.scope_of(occurrence)),
                            crate::names::ScopeOrigin::HoCarrier {
                                role: crate::names::HoRole::ScalarInput
                            }
                        ) {
                            return None;
                        }
                        match registry.identities.origin_of_col(occurrence) {
                            crate::names::ColumnOrigin::Republished { from, .. } => {
                                occurrence = from;
                            }
                            _ => break,
                        }
                    }
                    let source = registry.identities.progenitor(*column);
                    Some(match registry.identities.origin_of_col(source) {
                        crate::names::ColumnOrigin::CatalogColumn { entity, .. } => {
                            let mut relation = String::new();
                            registry
                                .identities
                                .write_entity(entity, &mut crate::names::Teaching(&mut relation));
                            format!(
                                "column '{param_name}' of relation '{relation}' in the view body"
                            )
                        }
                        _ => format!("column '{param_name}' in the view body"),
                    })
                })
                .collect();
            if !collisions.is_empty() {
                return Err(crate::error::DelightQLError::validation_error_categorized(
                    "ho/param_shadows_column",
                    format!(
                        "Scalar parameter '{param}' of higher-order view '{func}' collides with \
                         {collisions}. The parameter would \
                         silently capture the column: body constraints on '{param}' tautologize and \
                         the column drops from the output. Rename the parameter (e.g. '{param}_arg') \
                         so it no longer shadows the column.",
                        param = param_name,
                        func = function,
                        collisions = collisions.join(", "),
                    ),
                    "HO parameter validation",
                ));
            }
        }
    }

    // Apply PatternResolver to first-parens (scalar positions) via combined Access.
    //
    // The squished relation has schema [output_cols..., scalar_cols...] (glob-head)
    // or [scalar_cols..., output_cols...] (argumentative-head).
    // We build a combined Access covering ALL columns:
    //   - Scalar positions: expressions from scalar_spec (Literal → WHERE, Lvar → rename)
    //   - Output positions: pass-through Lvars (keep original name)
    // One PatternResolver call handles everything.
    if matches!(scalar_spec, ast_unresolved::Access::All) {
        // For glob call-sites, mark ground-bound scalar columns as hygienic
        // so they don't leak into the output (e.g., `label` from `tagged("young", T(*))(*)`).
        // Only hide columns where the call-site bound a LITERAL, not a free variable.
        let ground_scalar_col_names: Vec<&str> = positions
            .iter()
            .filter(|p| {
                matches!(
                    p.column_kind,
                    crate::pipeline::asts::ddl::HoColumnKind::Scalar
                )
            })
            .filter(|p| {
                // Only hide if the call-site explicitly bound a literal for this position.
                // Check scalar_params (captured before move) for a literal expression.
                p.column_name.as_ref().map_or(false, |name| {
                    ground_literal_scalar_names.iter().any(|n| n == name)
                })
            })
            .filter_map(|p| p.column_name.as_deref())
            .collect();

        if !ground_scalar_col_names.is_empty() {
            let body_schema = super::helpers::extraction::extract_cpr_schema(&resolved_expr);
            {
                let input_scope = &body_schema;
                let ground_names: Vec<_> = ground_scalar_col_names
                    .iter()
                    .map(|name| {
                        let spelling = registry.identities.intern(name, false);
                        registry.identities.canonical(spelling)
                    })
                    .collect();
                let output_scope = registry.identities.mint_derived_scope(
                    crate::names::ScopeOrigin::Wrap {
                        input: *input_scope,
                        why: crate::names::WrapReason::Projection,
                    },
                    crate::names::Hint::None,
                );
                for column in registry.identities.known_heading(*input_scope)? {
                    let hide = registry
                        .identities
                        .published_sym(column)
                        .is_some_and(|name| ground_names.contains(&name));
                    registry.identities.republish_column(
                        column,
                        output_scope,
                        crate::names::Republish::Passthrough,
                        registry.identities.published(column),
                        if hide {
                            crate::names::Addressing::Hygienic
                        } else {
                            registry.identities.addressing(column)
                        },
                        |_| {},
                    );
                }
                let mut expr = resolved_expr;
                update_relation_cpr_schema(&mut expr, output_scope);
                let bubbled = BubbledState::resolved(
                    registry.identities.known_heading(output_scope)?.to_vec(),
                    &registry.identities,
                );
                return Ok((expr, bubbled, absorbed_join_input));
            }
        }

        return Ok((resolved_expr, bubbled, absorbed_join_input));
    }

    let body_schema = super::helpers::extraction::extract_cpr_schema(&resolved_expr);
    let scalar_exprs = match scalar_spec {
        ast_unresolved::Access::Slots(exprs) => exprs,
        _ => return Ok((resolved_expr, bubbled, absorbed_join_input)),
    };

    // Identify scalar column names from position analysis
    let scalar_col_names: Vec<Option<&str>> = positions
        .iter()
        .filter(|p| {
            matches!(
                p.column_kind,
                crate::pipeline::asts::ddl::HoColumnKind::Scalar
            )
        })
        .map(|p| p.column_name.as_deref())
        .collect();

    // Build WHERE constraints and column filtering for scalar positions.
    // We construct the filter directly rather than going through apply_call_site_pattern,
    // because HO ConsultedViews get CTE-wrapped and the qualifier would be wrong.
    let input_scope = body_schema;
    let schema_cols = registry.identities.known_heading(input_scope)?;
    let output_scope = registry.identities.mint_derived_scope(
        crate::names::ScopeOrigin::Wrap {
            input: input_scope,
            why: crate::names::WrapReason::Projection,
        },
        crate::names::Hint::None,
    );
    let scalar_col_names: Vec<_> = scalar_col_names
        .into_iter()
        .map(|name| {
            name.map(|name| {
                let spelling = registry.identities.intern(name, false);
                registry.identities.canonical(spelling)
            })
        })
        .collect();

    let mut where_constraints = Vec::new();
    let mut scalar_idx = 0;

    for col in schema_cols.iter().copied() {
        let col_name = registry.identities.published_sym(col);
        let is_scalar = scalar_col_names.iter().any(|name| *name == col_name);
        if is_scalar && scalar_idx < scalar_exprs.len() {
            let scalar_expr = scalar_exprs[scalar_idx].term();
            scalar_idx += 1;
            let Some(scalar_expr) = scalar_expr else {
                continue;
            };
            match scalar_expr {
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(value),
                ) => {
                    // Literal → WHERE constraint + hide column (hygienic)
                    // Use unqualified column ref to avoid qualifier mismatch with CTE wrapping
                    let col_ref = ast_resolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(ColumnOccurrence {
                            column: col,
                            explicit_qualifier: false,
                        }),
                    ));
                    let lit_val = ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(value.clone()),
                    );
                    where_constraints.push(ast_resolved::TruthExpression::Comparison(Comparison {
                        operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                        left: Box::new(col_ref),
                        right: Box::new(lit_val),
                    }));
                    registry.identities.republish_column(
                        col,
                        output_scope,
                        crate::names::Republish::Passthrough,
                        registry.identities.published(col),
                        crate::names::Addressing::Hygienic,
                        |_| {},
                    );
                }
                ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                    AuthoredColumn { name, .. },
                ))) => {
                    let published = registry
                        .identities
                        .intern(name.as_str(), name.is_stropped());
                    let existing: Vec<_> = if absorbed_join_input {
                        schema_cols
                            .iter()
                            .copied()
                            .filter(|candidate| {
                                *candidate != col
                                    && registry.identities.published_sym(*candidate)
                                        == Some(registry.identities.canonical(published))
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    match existing.as_slice() {
                        // The scalar slot is a dispatch witness for the caller
                        // lvar already carried in the body. It constrains that
                        // occurrence and publishes no second column.
                        [_] => {
                            registry.identities.republish_column(
                                col,
                                output_scope,
                                crate::names::Republish::Passthrough,
                                registry.identities.published(col),
                                crate::names::Addressing::Hygienic,
                                |_| {},
                            );
                        }
                        [] => {
                            registry.identities.republish_column(
                                col,
                                output_scope,
                                crate::names::Republish::Rename,
                                Some(published),
                                crate::names::Addressing::Bare,
                                |_| {},
                            );
                        }
                        _ => {
                            return Err(DelightQLError::validation_error_categorized(
                                "resolution/ambiguous",
                                format!(
                                    "Higher-order scalar '{}' matches more than one published caller column",
                                    name
                                ),
                                "in higher-order scalar output",
                            ));
                        }
                    }
                }
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Open(
                        ast_unresolved::DomainHole::Disregarded,
                    ),
                ) => {
                    registry.identities.republish_column(
                        col,
                        output_scope,
                        crate::names::Republish::Passthrough,
                        registry.identities.published(col),
                        crate::names::Addressing::Hygienic,
                        |_| {},
                    );
                }
                _ => {
                    registry.identities.republish_column(
                        col,
                        output_scope,
                        crate::names::Republish::Passthrough,
                        registry.identities.published(col),
                        registry.identities.addressing(col),
                        |_| {},
                    );
                }
            }
        } else {
            registry.identities.republish_column(
                col,
                output_scope,
                crate::names::Republish::Passthrough,
                registry.identities.published(col),
                registry.identities.addressing(col),
                |_| {},
            );
        }
    }
    let output_columns = registry.identities.known_heading(output_scope)?;

    // Update schema on the inner relation
    let mut expr = resolved_expr;
    update_relation_cpr_schema(&mut expr, output_scope);

    // Wrap in Filter if there are WHERE constraints
    if !where_constraints.is_empty() {
        let combined = combine_where_constraints(where_constraints);
        expr = expr.then(ast_resolved::Continuation::Restrict {
            condition: combined,
            origin: ast_resolved::FilterOrigin::HoGroundScalar,
            cpr_schema: output_scope,
        });
    }

    let final_bubbled = BubbledState::resolved(output_columns.to_vec(), &registry.identities);
    // The transparent (no-CTE) HO path carries a call-site alias only in the
    // bubbled lexical state; rebuilding that state after applying scalar
    // arguments must not silently discard the alias.
    let final_bubbled = if let Some(alias) = user_alias {
        relabel_bubbled_with_alias(
            final_bubbled,
            &alias,
            BoundaryAnswering::Silent,
            &registry.identities,
        )
    } else {
        final_bubbled
    };

    Ok((expr, final_bubbled, absorbed_join_input))
}

/// Apply call-site positional patterns to an already-resolved consulted entity expression.
///
/// When a consulted view/fact is invoked with positional args (e.g., `active_users(1, fn, ln, ...)`),
/// the call-site access specifies column selection, renaming, and literal filtering.
/// This function applies those patterns on top of the resolved body expression — the same
/// work that `apply_pattern_resolver` does for Ground tables, but for ConsultedView/InnerRelation.
fn apply_call_site_pattern(
    access: &ast_unresolved::Access,
    expr: ast_resolved::Chain,
    body_schema: crate::names::ScopeId,
    entity_name: &str,
    display_name: &str,
    outer_context: Option<&[crate::names::ColId]>,
    identities: &crate::names::Registry,
    formal_frame: Option<&super::FormalFrame>,
    instantiation: Option<super::SlotInstantiation<'_>>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    // Get base columns and relabel with entity_name so WHERE constraints
    // reference the correct alias (e.g., "t0"."id" = 1)
    let base_cols = relabel_columns_with_alias(
        body_schema,
        &Some(entity_name.to_string().into()),
        identities,
    );

    // Exact arity, same as base tables: a short pattern must never bind
    // a prefix and silently drop the tail. Hygienic carriers are not
    // part of the declared heading.
    if let ast_unresolved::Access::Slots(patterns) = access {
        let visible = base_cols
            .iter()
            .filter(|column| identities.addressing(**column) != crate::names::Addressing::Hygienic)
            .count();
        if patterns.len() != visible {
            return Err(DelightQLError::validation_error(
                format!(
                    "Positional pattern incomplete - rule '{}' has {} columns but pattern specifies {} elements",
                    display_name, visible, patterns.len()
                ),
                "Positional pattern validation".to_string(),
            ));
        }
    }

    let pattern_resolver = PatternResolver::with_formals(formal_frame, instantiation);
    let join_context = outer_context.map(JoinContext::from);

    let pattern_result = pattern_resolver.resolve_pattern(
        access,
        &base_cols,
        entity_name,
        join_context.as_ref(),
        identities,
    )?;

    let output_scope = pattern_result.output_scope;
    let output_columns = pattern_result.output_columns.into_vec();

    let mut expr = expr;
    update_relation_cpr_schema(&mut expr, output_scope);

    // Wrap in Filter if there are WHERE constraints from literal patterns
    if !pattern_result.where_constraints.is_empty() {
        let combined = combine_where_constraints(pattern_result.where_constraints);
        expr = expr.then(ast_resolved::Continuation::Restrict {
            condition: combined,
            origin: ast_resolved::FilterOrigin::PositionalLiteral {
                source: output_scope,
            },
            cpr_schema: output_scope,
        });
    }

    Ok((
        expr,
        BubbledState::resolved(output_columns.to_vec(), identities),
    ))
}

/// Combine multiple WHERE constraints into a single AND chain.
fn combine_where_constraints(
    constraints: Vec<ast_resolved::TruthExpression>,
) -> ast_resolved::TruthExpression {
    ast_resolved::TruthExpression::all(constraints)
        .expect("caller only combines a non-empty constraint list")
}

/// Update the cpr_schema on a relation expression (ConsultedView or InnerRelation).
fn update_relation_cpr_schema(expr: &mut ast_resolved::Chain, new_scope: crate::names::ScopeId) {
    if let ast_resolved::Grelex::Reference(rel) = &mut expr.head {
        match rel {
            ast_resolved::Relation::ConsultedView { scoped, .. } => {
                *scoped = new_scope;
            }
            ast_resolved::Relation::InnerRelation { cpr_schema, .. } => {
                *cpr_schema = new_scope;
            }
            other => panic!(
                "catch-all hit in relation_resolver.rs update_scoped_schema: {:?}",
                other
            ),
        }
    }
}

