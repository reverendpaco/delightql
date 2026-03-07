// CFE definition precompilation - query-level and single CFE processing

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::asts::{resolved, unresolved};
use crate::pipeline::resolver::{self, DatabaseSchema};

use super::postprocessing::{
    replace_param_lvars_with_param_types, replace_params_with_explicit_context,
    replace_params_with_implicit_context,
};
use super::refining::refine_domain_expression;

/// Precompile all CFE definitions in a query
///
/// Converts Query::WithCfes (unresolved) → Query::WithPrecompiledCfes (resolved+refined bodies)
pub fn precompile_query_cfes(
    query: unresolved::Query,
    schema: &dyn DatabaseSchema,
    system: Option<&crate::system::DelightQLSystem>,
) -> Result<unresolved::Query> {
    match query {
        unresolved::Query::WithCfes {
            cfes,
            query: inner_query,
        } => {
            log::debug!("🎯 Precompiling {} CFE definitions", cfes.len());
            // Precompile each CFE body
            let precompiled_cfes: Vec<unresolved::PrecompiledCfeDefinition> = cfes
                .into_iter()
                .map(|cfe| {
                    log::debug!("  → CFE '{}' with params {:?}", cfe.name, cfe.parameters);
                    precompile_cfe_definition(cfe, schema, system)
                })
                .collect::<Result<Vec<_>>>()?;

            // Recursively process inner query (might have more CFEs or CTEs)
            let inner = precompile_query_cfes(*inner_query, schema, system)?;

            Ok(unresolved::Query::WithPrecompiledCfes {
                cfes: precompiled_cfes,
                query: Box::new(inner),
            })
        }
        unresolved::Query::Relational(expr) => {
            // No CFEs, just return as-is (still unresolved at this point)
            Ok(unresolved::Query::Relational(expr))
        }
        unresolved::Query::WithCtes { ctes, query } => {
            // CTEs don't need precompilation, but inner query might have CFEs
            let inner =
                precompile_query_cfes(unresolved::Query::Relational(query), schema, system)?;
            match inner {
                unresolved::Query::Relational(rel_expr) => Ok(unresolved::Query::WithCtes {
                    ctes,
                    query: rel_expr,
                }),
                unresolved::Query::WithPrecompiledCfes { .. } => {
                    // Inner query has precompiled CFEs, keep the structure
                    Ok(inner)
                }
                other => panic!("catch-all hit in cfe_precompiler/definition.rs precompile_query_cfes: unexpected inner Query variant: {:?}", other),
            }
        }
        unresolved::Query::WithPrecompiledCfes { .. } => Err(DelightQLError::parse_error(
            "Cannot precompile query that already has precompiled CFEs",
        )),
        unresolved::Query::ReplTempTable { query, table_name } => {
            let inner = precompile_query_cfes(*query, schema, system)?;
            Ok(unresolved::Query::ReplTempTable {
                query: Box::new(inner),
                table_name,
            })
        }
        unresolved::Query::ReplTempView { query, view_name } => {
            let inner = precompile_query_cfes(*query, schema, system)?;
            Ok(unresolved::Query::ReplTempView {
                query: Box::new(inner),
                view_name,
            })
        }
        unresolved::Query::WithErContext { context, query } => {
            // ER-context wrapper: recurse into the inner query, preserve the wrapper
            let inner = precompile_query_cfes(*query, schema, system)?;
            Ok(unresolved::Query::WithErContext {
                context,
                query: Box::new(inner),
            })
        }
    }
}

/// Precompile a single CFE definition through resolve + refine
pub(crate) fn precompile_cfe_definition(
    cfe: unresolved::CfeDefinition,
    schema: &dyn DatabaseSchema,
    system: Option<&crate::system::DelightQLSystem>,
) -> Result<unresolved::PrecompiledCfeDefinition> {
    // STEP 1: Create fake ColumnMetadata for ALL parameters (curried + regular)
    // This allows the body to resolve column references that will later become parameter holes
    let mut fake_columns: Vec<resolved::ColumnMetadata> = Vec::new();

    // Add fake columns for curried parameters first
    for (idx, param) in cfe.curried_params.iter().enumerate() {
        fake_columns.push(resolved::ColumnMetadata::new_with_name_flag(
            resolved::ColumnProvenance::from_column(param.clone()),
            resolved::FqTable {
                parents_path: NamespacePath::empty(),
                name: resolved::TableName::Named("__cfe_curried_params__".into()),
                backend_schema: resolved::PhaseBox::from_optional_schema(None), // Synthetic table
            },
            Some(idx),
            true, // has_user_name
        ));
    }

    // Add fake columns for regular parameters
    let curried_count = cfe.curried_params.len();
    for (idx, param) in cfe.parameters.iter().enumerate() {
        fake_columns.push(resolved::ColumnMetadata::new_with_name_flag(
            resolved::ColumnProvenance::from_column(param.clone()),
            resolved::FqTable {
                parents_path: NamespacePath::empty(),
                name: resolved::TableName::Named("__cfe_params__".into()),
                backend_schema: resolved::PhaseBox::from_optional_schema(None), // Synthetic table
            },
            Some(curried_count + idx),
            true, // has_user_name
        ));
    }

    // Add fake columns for explicit context params (if any)
    let param_count = curried_count + cfe.parameters.len();
    if let unresolved::ContextMode::Explicit(ref ctx_params) = cfe.context_mode {
        for (idx, ctx_param) in ctx_params.iter().enumerate() {
            fake_columns.push(resolved::ColumnMetadata::new_with_name_flag(
                resolved::ColumnProvenance::from_column(ctx_param.clone()),
                resolved::FqTable {
                    parents_path: NamespacePath::empty(),
                    name: resolved::TableName::Named("__cfe_context__".into()),
                    backend_schema: resolved::PhaseBox::from_optional_schema(None), // Synthetic table
                },
                Some(param_count + idx),
                true, // has_user_name
            ));
        }
    }

    // STEP 1.5: Inline borrowed functions in the CFE body before resolution.
    // Without this, references to consulted functions (e.g. `double:(x)` from a
    // borrowed namespace) would pass through as literal SQL function calls.
    let body = if let Some(sys) = system {
        let consult = crate::resolution::registry::ConsultRegistry::new_with_system(sys);
        resolver::grounding::inline_in_domain_expr_borrowed(cfe.body, &consult, None)?
    } else {
        cfe.body
    };

    // STEP 2: Resolve the body with fake parameter context
    // For explicit context modes, let resolver validate everything using fake schema
    // For implicit context modes, we still need in_correlation=true because we don't know the context params yet
    let in_correlation = matches!(cfe.context_mode, unresolved::ContextMode::Implicit);
    log::debug!(
        "Precompiling CFE '{}' with context_mode={:?}, in_correlation={}",
        cfe.name,
        cfe.context_mode,
        in_correlation
    );
    let mut registry = if let Some(sys) = system {
        crate::resolution::EntityRegistry::new_with_system(schema, sys)
    } else {
        crate::resolution::EntityRegistry::new(schema)
    };
    let resolved_body = resolver::resolving::resolve_domain_expr_via_registry(
        body,
        &mut registry,
        &fake_columns,
        in_correlation,
    )?;

    // STEP 3: Refine the resolved body (handles embedded subqueries)
    // Pass parameter lists to populate provenance
    let context_params_list = match &cfe.context_mode {
        unresolved::ContextMode::Explicit(params) => params.clone(),
        unresolved::ContextMode::None | unresolved::ContextMode::Implicit => vec![],
    };
    let refined_body = refine_domain_expression(
        resolved_body,
        &cfe.curried_params,
        &cfe.parameters,
        &context_params_list,
    )?;

    // STEP 4: Post-process based on context mode
    let (final_body, context_params, allows_positional) = match cfe.context_mode {
        unresolved::ContextMode::None => {
            // STRICT MODE: Error if CFE body references non-parameter columns
            let body = replace_param_lvars_with_param_types(
                refined_body,
                &cfe.curried_params,
                &cfe.parameters,
            )?;
            (body, vec![], false)
        }

        unresolved::ContextMode::Implicit => {
            // IMPLICIT DISCOVERY MODE: Auto-discover context params from body
            let mut discovered_context = Vec::new();
            let body = replace_params_with_implicit_context(
                refined_body,
                &cfe.curried_params,
                &cfe.parameters,
                &mut discovered_context,
            )?;
            // Implicit mode does NOT allow positional calls (context params not declared)
            (body, discovered_context, false)
        }

        unresolved::ContextMode::Explicit(ref declared_context) => {
            // EXPLICIT CONTEXT MODE: Validate only declared context params are used
            let body = replace_params_with_explicit_context(
                refined_body,
                &cfe.curried_params,
                &cfe.parameters,
                declared_context,
            )?;

            // Validate: empty explicit context is not allowed (enforces clarity)
            // If someone writes ..{} but doesn't use any context, they should just use regular CFE
            if declared_context.is_empty() {
                return Err(DelightQLError::ParseError {
                    message: format!(
                        "CFE '{}' declares empty explicit context '..{{}}' but this is unnecessary.\n\
                         \n\
                         Empty explicit context serves no purpose - the body only uses regular parameters.\n\
                         \n\
                         Solution: Remove the context marker entirely:\n\
                         {}:({}): ...",
                        cfe.name,
                        cfe.name,
                        cfe.parameters.join(", ")
                    ),
                    source: None,
                    subcategory: None,
                });
            }

            // Explicit mode DOES allow positional calls (context params are declared)
            (body, declared_context.clone(), true)
        }
    };

    Ok(unresolved::PrecompiledCfeDefinition {
        name: cfe.name,
        curried_params: cfe.curried_params,
        parameters: cfe.parameters,
        context_params,
        allows_positional_context_call: allows_positional,
        body: final_body,
    })
}
