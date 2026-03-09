//! Grounding support: function inlining and view expansion
//!
//! When a query uses the grounding operator (^), consulted definitions from
//! grounded namespaces are applied at the unresolved AST level before normal
//! resolution proceeds.
//!
//! **Function inlining**: `double:(x) :- x * 2` in namespace `lib::math` causes
//! `data::test^lib::math.users(*) |> (first_name, double:(balance) as doubled)` to become
//! `... |> (first_name, (balance * 2) as doubled)` before resolution.
//!
//! **View expansion**: `high_balance(*) :- users(*), balance > 1000` causes
//! `data::test^lib::views.high_balance(*)` to expand into the view body with
//! unqualified table references patched to use the data namespace.

use crate::ddl::ddl_builder;
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::{
    walk_transform_domain, walk_transform_inner_relation, walk_transform_operator,
    walk_transform_relation, AstTransform,
};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::metadata::GroundedPath;
use crate::pipeline::asts::core::{
    CfeDefinition, ContextMode, DomainExpression, FunctionExpression, PipeExpression, Relation,
    UnaryRelationalOperator, Unresolved,
};
use crate::pipeline::asts::ddl::{DdlDefinition, DdlHead, ViewHeadItem};
use crate::resolution::registry::ConsultRegistry;
use delightql_types::SqlIdentifier;
use log::debug;
use std::collections::HashMap;

/// Convert a NamespacePath to a namespace FQ string for ConsultRegistry lookup
pub(crate) fn namespace_path_to_fq(ns: &ast_unresolved::NamespacePath) -> String {
    let parts: Vec<&str> = ns.iter().map(|i| i.name.as_str()).collect();
    parts.join("::")
}

/// Inline consulted functions in a unary relational operator (grounded path).
///
/// Walks the operator's domain expressions, collecting DDL function definitions
/// as CfeDefinitions for precompilation. Returns the operator and collected CFEs.
pub(super) fn inline_consulted_functions_in_operator(
    operator: ast_unresolved::UnaryRelationalOperator,
    grounding: &GroundedPath,
    consult: &ConsultRegistry,
) -> Result<(ast_unresolved::UnaryRelationalOperator, Vec<CfeDefinition>)> {
    let mut inliner = GroundedInliner {
        grounding,
        consult,
        collected_cfes: vec![],
    };
    let op = inliner.transform_operator(operator)?;
    Ok((op, inliner.collected_cfes))
}

/// Extract function name and namespace from a Curried function with empty arguments.
/// Returns None if the function isn't a zero-arg Curried expression.
fn extract_empty_curried_name(
    func: &ast_unresolved::FunctionExpression,
) -> Option<(String, Option<ast_unresolved::NamespacePath>)> {
    match func {
        ast_unresolved::FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            ..
        } if arguments.is_empty() => Some((name.to_string(), namespace.clone())),
        // Non-Curried or non-empty-args: not a zero-arg curried call
        // (Regular, Bracket, Infix, Lambda, StringTemplate, CaseExpression,
        //  HigherOrder, Window, Curly, MetadataTreeGroup, Array, JsonPath,
        //  or Curried with non-empty args)
        _ => None,
    }
}

/// Look up a function entity by name: if namespace is specified, look in that
/// namespace; otherwise search across all borrowed namespaces.
fn lookup_borrowed_function(
    name: &str,
    namespace: Option<&ast_unresolved::NamespacePath>,
    consult: &ConsultRegistry,
) -> Result<Option<crate::resolution::registry::ConsultedEntity>> {
    if let Some(ns) = namespace {
        let fq = namespace_path_to_fq(ns);
        Ok(consult
            .lookup_entity(name, &fq)
            .filter(|e| e.entity_type == EntityType::DqlFunctionExpression.as_i32()))
    } else {
        consult.lookup_enlisted_function(name)
    }
}

/// Look up a context-aware function entity (type=3) by name.
fn lookup_borrowed_context_aware_function(
    name: &str,
    namespace: Option<&ast_unresolved::NamespacePath>,
    consult: &ConsultRegistry,
) -> Result<Option<crate::resolution::registry::ConsultedEntity>> {
    if let Some(ns) = namespace {
        let fq = namespace_path_to_fq(ns);
        Ok(consult
            .lookup_entity(name, &fq)
            .filter(|e| e.entity_type == EntityType::DqlContextAwareFunctionExpression.as_i32()))
    } else {
        consult.lookup_enlisted_context_aware_function(name)
    }
}

/// Convert a consulted entity (type=1 or type=3) into a CfeDefinition for
/// precompilation.
///
/// Re-parses the stored definition text to extract the context_mode and body,
/// then assembles a CfeDefinition that the CFE precompiler can process.
/// For multi-clause definitions (disjunctive functions), synthesizes a CASE
/// expression with parameter Lvars intact for the precompiler.
pub(crate) fn consulted_entity_to_cfe_definition(
    entity: &crate::resolution::registry::ConsultedEntity,
) -> Result<CfeDefinition> {
    let ddl_defs = ddl_builder::build_ddl_file(&entity.definition)?;
    if ddl_defs.is_empty() {
        return Err(DelightQLError::parse_error(format!(
            "No definition found for function '{}'",
            entity.name
        )));
    }

    // Split params into curried (callable) and regular based on FunctionParam.callable
    let (curried_params, parameters) = match &ddl_defs[0].head {
        DdlHead::Function { params, .. } => {
            let curried: Vec<String> = params.iter().filter(|p| p.callable).map(|p| p.name.clone()).collect();
            let regular: Vec<String> = params.iter().filter(|p| !p.callable).map(|p| p.name.clone()).collect();
            (curried, regular)
        }
        _ => (vec![], entity.params.iter().map(|p| p.name.clone()).collect()),
    };

    if ddl_defs.len() == 1 {
        let def = ddl_defs.into_iter().next().unwrap();

        // Extract context_mode BEFORE consuming def
        let context_mode = match &def.head {
            DdlHead::Function { context_mode, .. } => context_mode.clone(),
            _ => ContextMode::None,
        };

        let body = def.into_domain_expr().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Expected scalar body for function '{}', got relational",
                entity.name
            ))
        })?;

        Ok(CfeDefinition {
            name: entity.name.to_string(),
            curried_params,
            parameters,
            context_mode,
            body,
            source_namespace: Some(entity.namespace.clone()),
        })
    } else {
        // Multi-clause: synthesize CASE expression with parameter Lvars intact
        let context_mode = match &ddl_defs[0].head {
            DdlHead::Function { context_mode, .. } => context_mode.clone(),
            _ => ContextMode::None,
        };
        let body = build_case_body_from_clauses(ddl_defs)?;

        Ok(CfeDefinition {
            name: entity.name.to_string(),
            curried_params,
            parameters,
            context_mode,
            body,
            source_namespace: Some(entity.namespace.clone()),
        })
    }
}

// ============================================================================
// Recursive discovery of nested function references in CFE bodies
// ============================================================================

/// Walk a domain expression looking for function calls to consulted entities.
/// For each found, create a CfeDefinition and recursively discover in that
/// entity's body too. Returns all transitively discovered CfeDefinitions.
///
/// This lets us collect `double` when `doubled_value:(x) :- double:(x)` is
/// collected — even though `double` isn't directly referenced in the user query.
fn discover_nested_cfes(
    body: &DomainExpression<Unresolved>,
    source_ns: &str,
    consult: &ConsultRegistry,
    data_ns: Option<&ast_unresolved::NamespacePath>,
    already_collected: &[CfeDefinition],
) -> Result<Vec<CfeDefinition>> {
    let mut seen: std::collections::HashSet<String> = already_collected
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let mut result = Vec::new();
    discover_nested_cfes_inner(body, source_ns, consult, data_ns, &mut seen, &mut result)?;
    Ok(result)
}

fn discover_nested_cfes_inner(
    body: &DomainExpression<Unresolved>,
    source_ns: &str,
    consult: &ConsultRegistry,
    data_ns: Option<&ast_unresolved::NamespacePath>,
    seen: &mut std::collections::HashSet<String>,
    out: &mut Vec<CfeDefinition>,
) -> Result<()> {
    match body {
        DomainExpression::Function(func) => {
            match func {
                FunctionExpression::Regular {
                    name, namespace, arguments, ..
                }
                | FunctionExpression::Curried {
                    name, namespace, arguments, ..
                } => {
                    let name_str = name.to_string();
                    if !seen.contains(&name_str) {
                        // Activate namespace-local enlistments for the source namespace
                        let activated =
                            consult.activate_namespace_local_enlists_into_main(source_ns);
                        let entity = lookup_borrowed_function(
                            &name_str,
                            namespace.as_ref(),
                            consult,
                        );
                        // Also try context-aware functions
                        let ccafe_entity = if entity.as_ref().ok().and_then(|e| e.as_ref()).is_none() {
                            lookup_borrowed_context_aware_function(
                                &name_str,
                                namespace.as_ref(),
                                consult,
                            )
                        } else {
                            Ok(None)
                        };
                        consult.deactivate_namespace_local_enlists(&activated);

                        let entity = entity?.or(ccafe_entity?);
                        if let Some(entity) = entity {
                            seen.insert(name_str);
                            let mut cfe_def = consulted_entity_to_cfe_definition(&entity)?;
                            if let Some(ns) = data_ns {
                                cfe_def.body =
                                    patch_data_ns_in_domain_expr(cfe_def.body, ns);
                            }
                            // Recurse into this entity's body
                            discover_nested_cfes_inner(
                                &cfe_def.body,
                                &entity.namespace,
                                consult,
                                data_ns,
                                seen,
                                out,
                            )?;
                            out.push(cfe_def);
                        }
                    }
                    // Recurse into arguments
                    for arg in arguments {
                        discover_nested_cfes_inner(arg, source_ns, consult, data_ns, seen, out)?;
                    }
                }
                FunctionExpression::CaseExpression { arms, .. } => {
                    for arm in arms {
                        match arm {
                            ast_unresolved::CaseArm::Searched { result, .. } => {
                                discover_nested_cfes_inner(result, source_ns, consult, data_ns, seen, out)?;
                            }
                            ast_unresolved::CaseArm::Simple { test_expr, result, .. } => {
                                discover_nested_cfes_inner(test_expr, source_ns, consult, data_ns, seen, out)?;
                                discover_nested_cfes_inner(result, source_ns, consult, data_ns, seen, out)?;
                            }
                            ast_unresolved::CaseArm::Default { result } => {
                                discover_nested_cfes_inner(result, source_ns, consult, data_ns, seen, out)?;
                            }
                            _ => {}
                        }
                    }
                }
                FunctionExpression::Infix { left, right, .. } => {
                    discover_nested_cfes_inner(left, source_ns, consult, data_ns, seen, out)?;
                    discover_nested_cfes_inner(right, source_ns, consult, data_ns, seen, out)?;
                }
                FunctionExpression::Window { arguments, .. } => {
                    for arg in arguments {
                        discover_nested_cfes_inner(arg, source_ns, consult, data_ns, seen, out)?;
                    }
                }
                _ => {}
            }
        }
        DomainExpression::PipedExpression { value, .. } => {
            discover_nested_cfes_inner(value, source_ns, consult, data_ns, seen, out)?;
        }
        DomainExpression::Parenthesized { inner, .. } => {
            discover_nested_cfes_inner(inner, source_ns, consult, data_ns, seen, out)?;
        }
        DomainExpression::ScalarSubquery { .. } => {
            // Scalar subqueries reference tables — we still collect nested function refs
            // but the subquery itself will be resolved by the precompiler with the real schema.
        }
        _ => {}
    }
    Ok(())
}

fn discover_nested_cfes_inner_func(
    func: &FunctionExpression<Unresolved>,
    source_ns: &str,
    consult: &ConsultRegistry,
    data_ns: Option<&ast_unresolved::NamespacePath>,
    seen: &mut std::collections::HashSet<String>,
    out: &mut Vec<CfeDefinition>,
) -> Result<()> {
    // Wrap as DomainExpression::Function for uniform handling
    let as_domain = DomainExpression::Function(func.clone());
    discover_nested_cfes_inner(&as_domain, source_ns, consult, data_ns, seen, out)
}

fn discover_nested_cfes_in_boolean(
    _cond: &ast_unresolved::BooleanExpression,
    _source_ns: &str,
    _consult: &ConsultRegistry,
    _data_ns: Option<&ast_unresolved::NamespacePath>,
    _seen: &mut std::collections::HashSet<String>,
    _out: &mut Vec<CfeDefinition>,
) -> Result<()> {
    // Boolean conditions in CASE arms rarely contain function calls to consulted entities.
    // Skip for now — can be extended if needed.
    Ok(())
}

// ============================================================================
// Borrowed inlining — BorrowedInliner fold
// ============================================================================

/// Collects consulted functions as CfeDefinitions for precompilation.
/// Overrides transform_domain for function collection and piped-expression chain
/// processing, transform_operator for MapCover/EmbedMapCover conversion, and
/// transform_pipe for conditional operator processing (skip when data_ns is None).
struct BorrowedInliner<'a> {
    consult: &'a ConsultRegistry,
    data_ns: Option<&'a ast_unresolved::NamespacePath>,
    /// Functions discovered during fold, to be precompiled and injected
    /// as WithPrecompiledCfes by the resolver.
    collected_ccafe_cfes: Vec<CfeDefinition>,
    /// When true, skip type=1 collection but still discover type=3 CCAFEs.
    /// Used inside pipe operators when data_ns is None: we need to discover
    /// CCAFEs for precompilation but can't collect type=1 functions without
    /// data_ns patching (that's handled by the per-pipe handler in mod.rs).
    discovery_only: bool,
}

impl AstTransform<Unresolved, Unresolved> for BorrowedInliner<'_> {
    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Unresolved>> {
        match expr {
            DomainExpression::Function(func) => {
                // Extract name/namespace/arguments from Regular/Curried
                let (name_str, namespace, arguments, alias) = match &func {
                    FunctionExpression::Regular {
                        name,
                        namespace,
                        arguments,
                        alias,
                        ..
                    } => (
                        name.to_string(),
                        namespace.clone(),
                        arguments.clone(),
                        alias.clone(),
                    ),
                    FunctionExpression::Curried {
                        name,
                        namespace,
                        arguments,
                        ..
                    } => (name.to_string(), namespace.clone(), arguments.clone(), None),
                    _ => {
                        // Non-Regular/Curried: recurse into children
                        return Ok(DomainExpression::Function(self.transform_function(func)?));
                    }
                };

                // Lookup entity in borrowed namespaces (type=1 — regular functions)
                if !self.discovery_only {
                    let entity =
                        lookup_borrowed_function(&name_str, namespace.as_ref(), self.consult)?;

                    if let Some(entity) = entity {
                        debug!(
                            "Collecting DDL function '{}' from namespace '{}' for precompilation",
                            name_str, entity.namespace
                        );
                        let mut cfe_def = consulted_entity_to_cfe_definition(&entity)?;
                        if let Some(ns) = self.data_ns {
                            cfe_def.body = patch_data_ns_in_domain_expr(cfe_def.body, ns);
                        }
                        if !self
                            .collected_ccafe_cfes
                            .iter()
                            .any(|c| c.name == cfe_def.name)
                        {
                            // Recursively discover nested function refs in the body
                            let nested = discover_nested_cfes(
                                &cfe_def.body,
                                &entity.namespace,
                                self.consult,
                                self.data_ns,
                                &self.collected_ccafe_cfes,
                            )?;
                            self.collected_ccafe_cfes.extend(nested);
                            self.collected_ccafe_cfes.push(cfe_def);
                        }
                        // Pass through — will be substituted after precompilation
                        return Ok(DomainExpression::Function(self.transform_function(func)?));
                    }
                }

                // Try context-aware function (type=3) — same treatment
                let ccafe_entity = lookup_borrowed_context_aware_function(
                    &name_str,
                    namespace.as_ref(),
                    self.consult,
                )?;
                if let Some(entity) = ccafe_entity {
                    debug!(
                        "Collecting DDL context-aware function '{}' from namespace '{}' for precompilation",
                        name_str, entity.namespace
                    );
                    let cfe_def = consulted_entity_to_cfe_definition(&entity)?;
                    if !self
                        .collected_ccafe_cfes
                        .iter()
                        .any(|c| c.name == cfe_def.name)
                    {
                        self.collected_ccafe_cfes.push(cfe_def);
                    }
                    // Don't inline — pass through for CFE substitution after precompilation
                    return Ok(DomainExpression::Function(self.transform_function(func)?));
                }

                // Not a consulted function — recurse into children
                Ok(DomainExpression::Function(self.transform_function(func)?))
            }
            DomainExpression::PipedExpression {
                value,
                transforms,
                alias,
            } => {
                let mut current_value = self.transform_domain(*value)?;
                let mut remaining_transforms = Vec::new();

                for transform in transforms {
                    let transform = self.transform_function(transform)?;
                    let (name, namespace, args) = match &transform {
                        FunctionExpression::Curried {
                            name,
                            namespace,
                            arguments,
                            ..
                        } => (name.clone(), namespace.clone(), arguments.clone()),
                        FunctionExpression::Regular {
                            name,
                            namespace,
                            arguments,
                            ..
                        } => (name.clone(), namespace.clone(), arguments.clone()),
                        _ => {
                            remaining_transforms.push(transform);
                            continue;
                        }
                    };

                    let mut full_args = vec![current_value.clone()];
                    full_args.extend(args);
                    let synthetic = DomainExpression::Function(FunctionExpression::Regular {
                        name,
                        namespace,
                        arguments: full_args,
                        alias: None,
                        conditioned_on: None,
                    });

                    let inlined = self.transform_domain(synthetic)?;
                    let was_inlined = !matches!(
                        &inlined,
                        DomainExpression::Function(FunctionExpression::Regular { .. })
                    );

                    if was_inlined {
                        current_value = inlined;
                    } else {
                        remaining_transforms.push(transform);
                    }
                }

                if remaining_transforms.is_empty() {
                    Ok(current_value)
                } else {
                    Ok(DomainExpression::PipedExpression {
                        value: Box::new(current_value),
                        transforms: remaining_transforms,
                        alias,
                    })
                }
            }
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_operator(
        &mut self,
        op: UnaryRelationalOperator<Unresolved>,
    ) -> Result<UnaryRelationalOperator<Unresolved>> {
        match op {
            UnaryRelationalOperator::MapCover {
                function,
                columns,
                containment_semantic,
                conditioned_on,
            } => {
                let inlined_cols = columns
                    .into_iter()
                    .map(|e| self.transform_domain(e))
                    .collect::<Result<Vec<_>>>()?;

                // Check if the function is a consulted entity with empty curried args.
                if !self.discovery_only {
                    if let Some((name, namespace)) = extract_empty_curried_name(&function) {
                        let entity =
                            lookup_borrowed_function(&name, namespace.as_ref(), self.consult)?;

                        if entity.is_some() {
                            let transformations: Result<Vec<_>> = inlined_cols
                                .into_iter()
                                .map(|col| {
                                    let col_name = match &col {
                                        DomainExpression::Lvar { name, .. } => name.to_string(),
                                        _ => "__expr__".to_string(),
                                    };
                                    let synthetic =
                                        DomainExpression::Function(FunctionExpression::Regular {
                                            name: SqlIdentifier::from(name.as_str()),
                                            namespace: namespace.clone(),
                                            arguments: vec![col],
                                            alias: None,
                                            conditioned_on: None,
                                        });
                                    let inlined = self.transform_domain(synthetic)?;
                                    Ok((inlined, col_name, None))
                                })
                                .collect();

                            return Ok(UnaryRelationalOperator::Transform {
                                transformations: transformations?,
                                conditioned_on,
                            });
                        }
                    }
                }

                let inlined_func = self.transform_function(function)?;
                Ok(UnaryRelationalOperator::MapCover {
                    function: inlined_func,
                    columns: inlined_cols,
                    containment_semantic,
                    conditioned_on,
                })
            }
            UnaryRelationalOperator::EmbedMapCover {
                function,
                selector,
                alias_template,
                containment_semantic,
            } => {
                if !self.discovery_only {
                    if let Some((name, namespace)) = extract_empty_curried_name(&function) {
                        let entity =
                            lookup_borrowed_function(&name, namespace.as_ref(), self.consult)?;

                        if entity.is_some() {
                            let target_exprs = match selector {
                                ast_unresolved::ColumnSelector::Explicit(exprs) => exprs,
                                other_sel => {
                                    return Ok(UnaryRelationalOperator::EmbedMapCover {
                                        function,
                                        selector: other_sel,
                                        alias_template,
                                        containment_semantic,
                                    });
                                }
                            };

                            let expressions: Result<Vec<_>> = target_exprs
                                .into_iter()
                                .map(|col| {
                                    let col_name = match &col {
                                        DomainExpression::Lvar { name, .. } => name.to_string(),
                                        _ => "__expr__".to_string(),
                                    };
                                    let alias_str = match &alias_template {
                                        Some(ast_unresolved::ColumnAlias::Template(t)) => {
                                            t.template.replace("{@}", &col_name)
                                        }
                                        Some(ast_unresolved::ColumnAlias::Literal(lit)) => {
                                            lit.clone()
                                        }
                                        None => format!("{}_transformed", col_name),
                                    };
                                    let synthetic =
                                        DomainExpression::Function(FunctionExpression::Regular {
                                            name: SqlIdentifier::from(name.as_str()),
                                            namespace: namespace.clone(),
                                            arguments: vec![col],
                                            alias: Some(SqlIdentifier::from(alias_str.as_str())),
                                            conditioned_on: None,
                                        });
                                    self.transform_domain(synthetic)
                                })
                                .collect();

                            let mut all_exprs =
                                vec![ast_unresolved::DomainExpression::glob_builder().build()];
                            all_exprs.extend(expressions?);

                            return Ok(UnaryRelationalOperator::General {
                                containment_semantic,
                                expressions: all_exprs,
                            });
                        }
                    }
                }

                // Not a consulted function — fold children
                let inlined_selector = match selector {
                    ast_unresolved::ColumnSelector::Explicit(exprs) => {
                        let folded = exprs
                            .into_iter()
                            .map(|e| self.transform_domain(e))
                            .collect::<Result<Vec<_>>>()?;
                        ast_unresolved::ColumnSelector::Explicit(folded)
                    }
                    other_sel => other_sel,
                };
                Ok(UnaryRelationalOperator::EmbedMapCover {
                    function: self.transform_function(function)?,
                    selector: inlined_selector,
                    alias_template,
                    containment_semantic,
                })
            }
            other => walk_transform_operator(self, other),
        }
    }

    fn transform_pipe(
        &mut self,
        p: PipeExpression<Unresolved>,
    ) -> Result<PipeExpression<Unresolved>> {
        let source = self.transform_relational(p.source)?;
        let operator = if self.data_ns.is_some() {
            // With data_ns: full processing
            self.transform_operator(p.operator)?
        } else {
            // Without data_ns: discovery-only mode for type=3 CCAFEs.
            // Type=1 functions in pipe operators are collected by the
            // per-pipe handler in resolver_fold.rs which has grounding context.
            let prev = self.discovery_only;
            self.discovery_only = true;
            let op = self.transform_operator(p.operator)?;
            self.discovery_only = prev;
            op
        };
        Ok(PipeExpression {
            source,
            operator,
            cpr_schema: p.cpr_schema,
        })
    }
}

/// Inline consulted functions from borrowed namespaces in a unary relational operator.
///
/// Returns the transformed operator and any collected CfeDefinitions (type=1 and type=3)
/// that need precompilation before the transformer can substitute them.
pub(super) fn inline_consulted_functions_in_operator_borrowed(
    operator: ast_unresolved::UnaryRelationalOperator,
    consult: &ConsultRegistry,
    data_ns: Option<&ast_unresolved::NamespacePath>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, Vec<CfeDefinition>)> {
    let mut inliner = BorrowedInliner {
        consult,
        data_ns,
        collected_ccafe_cfes: vec![],
        discovery_only: false,
    };
    let op = inliner.transform_operator(operator)?;
    Ok((op, inliner.collected_ccafe_cfes))
}

// ============================================================================
// GroundedInliner — consulted function inlining (grounded path)
// ============================================================================

struct GroundedInliner<'a> {
    grounding: &'a GroundedPath,
    consult: &'a ConsultRegistry,
    collected_cfes: Vec<CfeDefinition>,
}

impl AstTransform<Unresolved, Unresolved> for GroundedInliner<'_> {
    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Unresolved>> {
        match expr {
            DomainExpression::Function(func) => {
                let (name, namespace, arguments, alias) = match &func {
                    FunctionExpression::Regular {
                        name,
                        namespace,
                        arguments,
                        alias,
                        ..
                    } => (
                        name.clone(),
                        namespace.clone(),
                        arguments.clone(),
                        alias.clone(),
                    ),
                    FunctionExpression::Curried {
                        name,
                        namespace,
                        arguments,
                        ..
                    } => (name.clone(), namespace.clone(), arguments.clone(), None),
                    _ => return walk_transform_domain(self, DomainExpression::Function(func)),
                };

                // Look up consulted entity — explicit namespace or grounded_ns search
                let entity = if let Some(ns) = &namespace {
                    let fq = namespace_path_to_fq(ns);
                    self.consult
                        .lookup_entity(&name, &fq)
                        .filter(|e| e.entity_type == EntityType::DqlFunctionExpression.as_i32())
                } else {
                    self.grounding.grounded_ns.iter().find_map(|ns| {
                        let fq = namespace_path_to_fq(ns);
                        self.consult
                            .lookup_entity(&name, &fq)
                            .filter(|e| e.entity_type == EntityType::DqlFunctionExpression.as_i32())
                    })
                };

                if let Some(entity) = entity {
                    debug!(
                        "Collecting DDL function '{}' from grounded path for precompilation",
                        name
                    );
                    let mut cfe_def = consulted_entity_to_cfe_definition(&entity)?;
                    let data_ns = &self.grounding.data_ns;
                    cfe_def.body = patch_data_ns_in_domain_expr(cfe_def.body, data_ns);
                    if !self.collected_cfes.iter().any(|c| c.name == cfe_def.name) {
                        let nested = discover_nested_cfes(
                            &cfe_def.body,
                            &entity.namespace,
                            self.consult,
                            Some(data_ns),
                            &self.collected_cfes,
                        )?;
                        self.collected_cfes.extend(nested);
                        self.collected_cfes.push(cfe_def);
                    }
                    // Pass through — will be substituted after precompilation
                    Ok(DomainExpression::Function(self.transform_function(func)?))
                } else {
                    walk_transform_domain(self, DomainExpression::Function(func))
                }
            }
            other => walk_transform_domain(self, other),
        }
    }
}

// ============================================================================
// Multi-clause CASE synthesis
// ============================================================================

/// Unwrap a `DomainExpression::Predicate` to its inner `BooleanExpression`.
///
/// Guard expressions like `n % 15 = 0` are parsed by `body_parser` as
/// `DomainExpression::Predicate { expr }`. This unwraps that wrapper so the
/// guard can be used as a `CaseArm::Searched` condition.
fn domain_expr_to_boolean(
    expr: ast_unresolved::DomainExpression,
) -> Result<ast_unresolved::BooleanExpression> {
    match expr {
        ast_unresolved::DomainExpression::Predicate { expr, .. } => Ok(*expr),
        other => Err(DelightQLError::parse_error(format!(
            "Expected boolean guard expression, got: {:?}",
            other
        ))),
    }
}

/// Synthesize a `CaseExpression` from multiple guarded function clauses,
/// leaving parameter Lvars intact (no substitution).
///
/// Used when converting multi-clause DDL functions into CfeDefinitions. The
/// precompiler will handle parameter resolution via fake columns.
fn build_case_body_from_clauses(
    clauses: Vec<DdlDefinition>,
) -> Result<ast_unresolved::DomainExpression> {
    let mut arms: Vec<ast_unresolved::CaseArm> = Vec::new();

    for clause in &clauses {
        let params = match &clause.head {
            DdlHead::Function { params, .. } => params,
            _ => {
                return Err(DelightQLError::parse_error(
                    "Multi-clause CASE synthesis requires function definitions",
                ));
            }
        };

        let body = clause.as_domain_expr().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Expected scalar body for multi-clause function '{}', got relational",
                clause.name
            ))
        })?;

        let has_guard = params.iter().any(|p| p.guard.is_some());
        if has_guard {
            let guard_expr = params
                .iter()
                .find_map(|p| p.guard.as_ref())
                .unwrap()
                .clone();
            let guard_bool = domain_expr_to_boolean(guard_expr)?;
            arms.push(ast_unresolved::CaseArm::Searched {
                condition: Box::new(guard_bool),
                result: Box::new(body.clone()),
            });
        } else {
            arms.push(ast_unresolved::CaseArm::Default {
                result: Box::new(body.clone()),
            });
        }
    }

    Ok(ast_unresolved::DomainExpression::Function(
        ast_unresolved::FunctionExpression::CaseExpression { arms, alias: None },
    ))
}

// ============================================================================
// Alias application
// ============================================================================

/// Apply an alias to a domain expression.
fn apply_alias(expr: &mut ast_unresolved::DomainExpression, alias: SqlIdentifier) {
    match expr {
        ast_unresolved::DomainExpression::Lvar {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::DomainExpression::Literal {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::DomainExpression::Function(func) => {
            apply_alias_to_func(func, alias);
        }
        ast_unresolved::DomainExpression::ScalarSubquery {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::DomainExpression::Parenthesized {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::DomainExpression::Predicate {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        // For other expression types, alias is lost (shouldn't happen in practice)
        other => panic!("catch-all hit in grounding.rs apply_alias: {:?}", other),
    }
}

fn apply_alias_to_func(func: &mut ast_unresolved::FunctionExpression, alias: SqlIdentifier) {
    match func {
        ast_unresolved::FunctionExpression::Regular {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::FunctionExpression::Infix {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::FunctionExpression::Curried { .. } => {
            // Curried doesn't have alias — this shouldn't happen after inlining
        }
        ast_unresolved::FunctionExpression::HigherOrder {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::FunctionExpression::Bracket {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::FunctionExpression::Lambda {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        ast_unresolved::FunctionExpression::CaseExpression {
            alias: ref mut a, ..
        } => {
            *a = Some(alias);
        }
        other => panic!(
            "catch-all hit in grounding.rs apply_alias_to_func: {:?}",
            other
        ),
    }
}

// ============================================================================
// Parameter substitution (used by sigma predicates)
// ============================================================================

/// Replaces `Lvar` nodes whose names appear in `param_map` with the
/// corresponding argument expression. All other nodes are structurally
/// descended by the default `walk_*` functions.
struct ParamSubstituter<'a> {
    param_map: &'a HashMap<&'a str, &'a ast_unresolved::DomainExpression>,
}

impl AstTransform<Unresolved, Unresolved> for ParamSubstituter<'_> {
    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Unresolved>> {
        match expr {
            DomainExpression::Lvar {
                ref name,
                ref alias,
                ..
            } => {
                if let Some(&replacement) = self.param_map.get(name.as_str()) {
                    let mut result = replacement.clone();
                    if let Some(a) = alias.clone() {
                        apply_alias(&mut result, a);
                    }
                    Ok(result)
                } else {
                    Ok(expr)
                }
            }
            other => walk_transform_domain(self, other),
        }
    }
}

/// Substitute parameter Lvars in a domain expression with argument expressions.
/// Used by sigma predicate inlining in predicates.rs.
pub(crate) fn substitute_in_domain_expr(
    expr: ast_unresolved::DomainExpression,
    param_map: &HashMap<&str, &ast_unresolved::DomainExpression>,
) -> ast_unresolved::DomainExpression {
    ParamSubstituter { param_map }
        .transform_domain(expr)
        .expect("substitution is infallible")
}

// ============================================================================
// View expansion
// ============================================================================

/// Expand a consulted view body into an unresolved Query.
///
/// Parses the view body source and patches all unqualified table references
/// to use the data namespace from the grounding context. Returns a full Query
/// (not just RelationalExpression) to preserve CTEs in view definitions.
///
/// For multi-clause (disjunctive) view definitions, synthesizes same-name CTEs
/// so the resolver's CTE merge infrastructure produces UNION ALL automatically.
pub(super) fn expand_consulted_view(
    body_source: &str,
    grounding: &GroundedPath,
) -> Result<ast_unresolved::Query> {
    let defs = ddl_builder::build_ddl_file(body_source)?;
    if defs.is_empty() {
        return Err(DelightQLError::parse_error(
            "No definition found in view body source",
        ));
    }

    // Enforce argumentative head contracts by translating to glob heads with projections
    let has_argumentative = defs
        .iter()
        .any(|d| matches!(d.head, DdlHead::ArgumentativeView { .. }));
    let defs = if has_argumentative {
        desugar_argumentative_defs(defs)?
    } else {
        defs
    };

    if defs.len() == 1 {
        // Fast path: single clause (existing behavior)
        let ddl_def = defs.into_iter().next().unwrap();
        let query = ddl_def.into_query().ok_or_else(|| {
            DelightQLError::parse_error("Expected relational body for view, got scalar")
        })?;
        return Ok(patch_data_ns_query(query, &grounding.data_ns));
    }

    // Multi-clause: synthesize disjunctive CTEs
    expand_multi_clause_view(defs, Some(&grounding.data_ns))
}

/// Synthesize a disjunctive view from multiple clause definitions.
///
/// Creates same-name CTE bindings for each clause body, then wraps them
/// in a `Query::WithCtes` with a `view_name(*)` main query. The resolver's
/// CTE merge infrastructure groups same-name CTEs into UNION ALL.
pub(super) fn expand_multi_clause_view(
    defs: Vec<crate::pipeline::asts::ddl::DdlDefinition>,
    data_ns: Option<&ast_unresolved::NamespacePath>,
) -> Result<ast_unresolved::Query> {
    let view_name = defs[0].name.clone();
    let mut all_ctes = Vec::new();

    for def in defs {
        let query = def.into_query().ok_or_else(|| {
            DelightQLError::parse_error(
                "Expected relational body for disjunctive view clause, got scalar",
            )
        })?;
        let patched = if let Some(ns) = data_ns {
            patch_data_ns_query(query, ns)
        } else {
            query
        };

        match patched {
            ast_unresolved::Query::Relational(expr) => {
                all_ctes.push(ast_unresolved::CteBinding {
                    name: view_name.clone(),
                    expression: expr,
                    is_recursive: ast_unresolved::PhaseBox::phantom(),
                });
            }
            ast_unresolved::Query::WithCtes {
                ctes,
                query: main_expr,
            } => {
                // Clause body has its own CTEs — hoist them into outer list first,
                // then add the main expression as the disjunctive CTE.
                for cte in ctes {
                    all_ctes.push(cte);
                }
                all_ctes.push(ast_unresolved::CteBinding {
                    name: view_name.clone(),
                    expression: main_expr,
                    is_recursive: ast_unresolved::PhaseBox::phantom(),
                });
            }
            other => {
                // WithCfes or other variants — extract as best we can
                return Err(DelightQLError::parse_error(format!(
                    "Unsupported query form in disjunctive view clause: {:?}",
                    std::mem::discriminant(&other)
                )));
            }
        }
    }

    // Main query: view_name(*) — a ground relation referencing the CTE
    let main_query =
        ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::Ground {
            identifier: ast_unresolved::QualifiedName {
                namespace_path: ast_unresolved::NamespacePath::empty(),
                name: view_name.into(),
                grounding: None,
            },
            canonical_name: ast_unresolved::PhaseBox::phantom(),
            domain_spec: ast_unresolved::DomainSpec::Glob,
            alias: None,
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: ast_unresolved::PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        });

    Ok(ast_unresolved::Query::WithCtes {
        ctes: all_ctes,
        query: main_query,
    })
}

// ============================================================================
// Argumentative Head Contract Enforcement
//
// Argumentative heads declare a closed schema contract: the entity exposes
// exactly the named columns, in order. This is semantically different from
// glob heads, which are open (inherit schema from body, corresponding union).
//
// The compiler enforces the contract by translating each clause to a glob-head
// definition with an appended projection. This is the enforcement mechanism,
// not the semantics. The head IS the schema declaration; the projection is
// how we apply it to the body.
// ============================================================================

/// Compute canonical column names from argumentative view head items across clauses.
///
/// For each position, find the free-variable name from any clause.
/// If all clauses are ground at a position, generate synthetic name `_col{pos+1}`.
fn compute_canonical_column_names(heads: &[&Vec<ViewHeadItem>]) -> Vec<String> {
    if heads.is_empty() {
        return vec![];
    }
    let arity = heads[0].len();
    (0..arity)
        .map(|pos| {
            // Find first free variable at this position across all clauses
            for items in heads {
                if let Some(ViewHeadItem::Free(name)) = items.get(pos) {
                    return name.clone();
                }
            }
            // All ground at this position — synthetic name
            format!("_col{}", pos + 1)
        })
        .collect()
}

/// Extract the body text from a definition's full source by finding the neck `:-`.
///
/// Returns the text after `:-`, trimmed.
fn extract_body_text(full_source: &str) -> Result<&str> {
    let neck_pos = full_source.find(":-").ok_or_else(|| {
        DelightQLError::parse_error("Argumentative view definition missing :- neck")
    })?;
    Ok(full_source[neck_pos + 2..].trim())
}

/// Enforce a single argumentative clause's head contract by generating
/// glob-head DQL with an appended projection.
///
/// Generates `name(*) :- body |> (projection_items)` where:
/// - Free variables become column references (body must produce them)
/// - Ground terms become `literal as canonical_name` (injected constants)
fn desugar_single_clause(
    name: &str,
    items: &[ViewHeadItem],
    canonical_names: &[String],
    body_text: &str,
) -> String {
    let proj_items: Vec<String> = items
        .iter()
        .zip(canonical_names.iter())
        .map(|(item, canon_name)| match item {
            ViewHeadItem::Free(col_name) => {
                if col_name == canon_name {
                    col_name.clone()
                } else {
                    // Free variable name differs from canonical — use alias
                    format!("{} as {}", col_name, canon_name)
                }
            }
            ViewHeadItem::Ground(literal) => {
                format!("{} as {}", literal, canon_name)
            }
        })
        .collect();

    format!(
        "{}(*) :- {} |> ({})",
        name,
        body_text,
        proj_items.join(", ")
    )
}

/// Enforce argumentative head contracts by translating to glob-head definitions
/// with appended projections.
///
/// The argumentative head declares a closed schema contract. This function
/// validates the contract (arity agreement, name consistency across clauses)
/// and then enforces it by generating glob-head DQL where each clause body
/// gets `|> (projection)` appended.
///
/// Example: `young(first_name, age) :- users(*), age < 30` compiles as
/// `young(*) :- users(*), age < 30 |> (first_name, age)`
///
/// Ground terms inject constants:
/// `bracket("young", fn, age) :- users(*), age < 30` compiles as
/// `bracket(*) :- users(*), age < 30 |> ("young" as status, fn, age)`
pub(super) fn desugar_argumentative_defs(defs: Vec<DdlDefinition>) -> Result<Vec<DdlDefinition>> {
    let view_name = defs[0].name.clone();

    // Validate: no mixing glob and argumentative head forms
    let has_glob = defs.iter().any(|d| matches!(d.head, DdlHead::View));
    let has_argumentative = defs
        .iter()
        .any(|d| matches!(d.head, DdlHead::ArgumentativeView { .. }));

    if has_glob && has_argumentative {
        return Err(DelightQLError::validation_error_categorized(
            "ddl/head/mixed_forms",
            format!(
                "Entity '{}': cannot mix glob (*) and argumentative head forms \
                 across clauses. Use all glob or all argumentative.",
                view_name
            ),
            "Head form mismatch",
        ));
    }

    // Collect all argumentative head items for canonical name computation
    let arg_heads: Vec<&Vec<ViewHeadItem>> = defs
        .iter()
        .filter_map(|d| match &d.head {
            DdlHead::ArgumentativeView { items } => Some(items),
            _ => None,
        })
        .collect();

    if arg_heads.is_empty() {
        // No argumentative heads — nothing to desugar
        return Ok(defs);
    }

    // Validate: arity agreement and name agreement across clauses
    if arg_heads.len() >= 2 {
        let first_items = arg_heads[0];
        for (i, items) in arg_heads.iter().enumerate().skip(1) {
            if items.len() != first_items.len() {
                return Err(DelightQLError::validation_error_categorized(
                    "ddl/head/arity",
                    format!(
                        "Entity '{}': clause {} has {} head item(s) but clause 1 has {}. \
                         All argumentative clauses must have the same arity.",
                        view_name,
                        i + 1,
                        items.len(),
                        first_items.len()
                    ),
                    "Head arity mismatch",
                ));
            }
        }

        let arity = first_items.len();
        for pos in 0..arity {
            let mut free_name: Option<&str> = None;
            for (clause_idx, items) in arg_heads.iter().enumerate() {
                if let ViewHeadItem::Free(name) = &items[pos] {
                    if let Some(existing) = free_name {
                        if existing != name.as_str() {
                            return Err(DelightQLError::validation_error_categorized(
                                "ddl/head/name_conflict",
                                format!(
                                    "Entity '{}': position {} is named '{}' in clause {} \
                                     but '{}' in an earlier clause. \
                                     Free variables at each position must agree.",
                                    view_name,
                                    pos + 1,
                                    name,
                                    clause_idx + 1,
                                    existing
                                ),
                                "Head name conflict",
                            ));
                        }
                    } else {
                        free_name = Some(name.as_str());
                    }
                }
            }
        }
    }

    let canonical_names = compute_canonical_column_names(&arg_heads);

    // Build desugared DQL text for each clause
    let mut desugared_lines = Vec::new();
    for def in &defs {
        if let DdlHead::ArgumentativeView { items } = &def.head {
            let body_text = extract_body_text(&def.full_source)?;
            desugared_lines.push(desugar_single_clause(
                &view_name,
                items,
                &canonical_names,
                body_text,
            ));
        } else {
            // Non-argumentative defs pass through (shouldn't happen after validation)
            desugared_lines.push(def.full_source.clone());
        }
    }

    let desugared_source = desugared_lines.join("\n");
    debug!(
        "Desugared argumentative view '{}': {}",
        view_name, desugared_source
    );
    ddl_builder::build_ddl_file(&desugared_source)
}

/// Inline consulted functions in a Query.
///
/// Returns the folded query and any collected context-aware function definitions
/// (type=3) that need CFE precompilation before resolution can proceed.
pub(super) fn inline_in_query_borrowed(
    query: ast_unresolved::Query,
    consult: &ConsultRegistry,
    data_ns: Option<&ast_unresolved::NamespacePath>,
) -> Result<(ast_unresolved::Query, Vec<CfeDefinition>)> {
    let mut inliner = BorrowedInliner {
        consult,
        data_ns,
        collected_ccafe_cfes: vec![],
        discovery_only: false,
    };
    let folded = inliner.transform_query(query)?;
    Ok((folded, inliner.collected_ccafe_cfes))
}

// ============================================================================
// Ground scalar expansion for HO views
// ============================================================================

use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode, HoParam, HoPositionInfo};

/// Compute cross-clause unified position analysis for all HO parameter positions.
///
/// For each position 0..max_params across all clauses:
/// - Determines column_kind: Glob/Argumentative/Scalar
/// - Determines ground_mode from the Scalar/GroundScalar distribution
/// - Collects ground_values: Vec<(ordinal, value)>
/// - Determines column_name: from free-variable clauses (must agree)
///
/// This replaces `extract_ground_scalar_info()` + `validate_mixed_ground_params()`
/// with a single, complete analysis computed at consult time.
pub(crate) fn build_ho_position_analysis(
    defs: &[crate::pipeline::asts::ddl::DdlDefinition],
) -> Vec<HoPositionInfo> {
    use crate::pipeline::asts::ddl::DdlHead;

    let heads: Vec<&Vec<HoParam>> = defs
        .iter()
        .filter_map(|d| match &d.head {
            DdlHead::HoView { params, .. } => Some(params),
            _ => None,
        })
        .collect();

    build_ho_position_analysis_from_heads(&heads)
}

/// Build position analysis from a set of HO head param lists.
///
/// Accepts pre-extracted heads so callers that only have heads (not full
/// DdlDefinitions) can use this directly — e.g., the deferred-body HO view
/// path in system.rs where each clause's head is parsed individually.
pub(crate) fn build_ho_position_analysis_from_heads(
    heads: &[&Vec<HoParam>],
) -> Vec<HoPositionInfo> {
    use crate::pipeline::asts::ddl::HoParamKind;

    if heads.is_empty() {
        return Vec::new();
    }

    let max_params = heads.iter().map(|h| h.len()).max().unwrap_or(0);
    let mut positions = Vec::with_capacity(max_params);

    for pos in 0..max_params {
        let mut has_glob = false;
        let mut has_argumentative = false;
        let mut arg_columns: Option<Vec<String>> = None;
        let mut has_scalar = false;
        let mut has_ground_scalar = false;
        let mut ground_values: Vec<(usize, String)> = Vec::new();
        let mut column_name: Option<String> = None;

        for (clause_ordinal, head) in heads.iter().enumerate() {
            if let Some(param) = head.get(pos) {
                match &param.kind {
                    HoParamKind::Glob => {
                        has_glob = true;
                        // Glob contributes canonical name (table parameter name, e.g., "T")
                        if column_name.is_none() {
                            column_name = Some(param.name.clone());
                        }
                    }
                    HoParamKind::Argumentative(cols) => {
                        has_argumentative = true;
                        if arg_columns.is_none() {
                            arg_columns = Some(cols.clone());
                        }
                        // Argumentative contributes canonical name (table parameter name)
                        if column_name.is_none() {
                            column_name = Some(param.name.clone());
                        }
                    }
                    HoParamKind::Scalar => {
                        has_scalar = true;
                        // Free variable — contributes canonical name
                        if column_name.is_none() {
                            column_name = Some(param.name.clone());
                        }
                    }
                    HoParamKind::GroundScalar(value) => {
                        has_ground_scalar = true;
                        ground_values.push((clause_ordinal, value.clone()));
                        // GroundScalar doesn't contribute a column name because
                        // its "name" field is the literal value, not a variable name.
                        // The canonical name comes from Scalar clauses.
                    }
                }
            }
        }

        let column_kind = if has_glob {
            HoColumnKind::TableGlob
        } else if has_argumentative {
            HoColumnKind::TableArgumentative(arg_columns.unwrap_or_default())
        } else {
            HoColumnKind::Scalar
        };

        let ground_mode = if has_glob || has_argumentative {
            HoGroundMode::InputOnly
        } else if has_ground_scalar && !has_scalar {
            HoGroundMode::PureGround
        } else if has_ground_scalar && has_scalar {
            HoGroundMode::MixedGround
        } else {
            HoGroundMode::PureUnbound
        };

        positions.push(HoPositionInfo {
            position: pos,
            column_kind,
            ground_mode,
            ground_values,
            column_name,
        });
    }

    positions
}

/// Inject ground scalar constants as real AST columns into a clause body.
///
/// For each position where this clause has GroundScalar, wraps the body's
/// main query expression with a General (embed) operator:
///   `body |> (*, "ground_value" as column_name)`
///
/// Column names come from cross-clause position analysis:
/// - MixedGround positions: canonical name from Scalar (free-variable) clauses
/// - PureGround positions: DDL param name
///
/// If `output_head` is Some, also applies the argumentative output projection.
pub(super) fn inject_scalar_columns(
    query: ast_unresolved::Query,
    clause_params: &[HoParam],
    positions: &[HoPositionInfo],
    output_head: Option<&[ViewHeadItem]>,
) -> ast_unresolved::Query {
    use crate::pipeline::asts::core::{ContainmentSemantic, UnaryRelationalOperator};

    // Collect ground scalar injections: (column_name, literal_value)
    let mut ground_injections: Vec<(String, String)> = Vec::new();
    for pos_info in positions {
        if let Some(clause_param) = clause_params.get(pos_info.position) {
            if let crate::pipeline::asts::ddl::HoParamKind::GroundScalar(ref clause_val) =
                clause_param.kind
            {
                if let Some(name) = pos_info.column_name.clone() {
                    ground_injections.push((name, clause_val.clone()));
                }
            }
        }
    }

    if ground_injections.is_empty() && output_head.is_none() {
        return query;
    }

    // Build the embed expressions
    let mut embed_exprs: Vec<ast_unresolved::DomainExpression> = Vec::new();

    if output_head.is_some() {
        // When there's an output head, inject ground constants as part of the projection
        // First: ground scalar constants
        for (col_name, literal_val) in &ground_injections {
            let literal = parse_literal_value(literal_val);
            embed_exprs.push(ast_unresolved::DomainExpression::Literal {
                value: literal,
                alias: Some(col_name.clone().into()),
            });
        }
        // Then: output head items
        if let Some(items) = output_head {
            for item in items {
                match item {
                    ViewHeadItem::Free(name) => {
                        embed_exprs.push(
                            ast_unresolved::DomainExpression::lvar_builder(name.clone()).build(),
                        );
                    }
                    ViewHeadItem::Ground(literal) => {
                        let val = parse_literal_value(literal);
                        embed_exprs.push(ast_unresolved::DomainExpression::Literal {
                            value: val,
                            alias: Some("_ground".into()),
                        });
                    }
                }
            }
        }
    } else {
        // No output head (glob) — use embed: (*, "value" as name, ...)
        embed_exprs.push(ast_unresolved::DomainExpression::glob_builder().build());
        for (col_name, literal_val) in &ground_injections {
            let literal = parse_literal_value(literal_val);
            embed_exprs.push(ast_unresolved::DomainExpression::Literal {
                value: literal,
                alias: Some(col_name.clone().into()),
            });
        }
    }

    let operator = UnaryRelationalOperator::General {
        containment_semantic: ContainmentSemantic::Parenthesis,
        expressions: embed_exprs,
    };

    // Wrap the main query expression with the pipe operator
    wrap_query_with_pipe(query, operator)
}

/// Parse a literal value string (e.g., `"young"` or `42`) into a LiteralValue.
fn parse_literal_value(s: &str) -> crate::pipeline::asts::core::LiteralValue {
    use crate::pipeline::asts::core::LiteralValue;

    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        // Quoted string — strip quotes
        LiteralValue::String(s[1..s.len() - 1].to_string())
    } else if s.parse::<f64>().is_ok() {
        LiteralValue::Number(s.to_string())
    } else if s == "true" || s == "false" {
        LiteralValue::Boolean(s == "true")
    } else if s == "null" {
        LiteralValue::Null
    } else {
        // Treat as string
        LiteralValue::String(s.to_string())
    }
}

/// Wrap a Query's main expression with a pipe operator.
///
/// For `Query::Relational(expr)`: produces `Query::Relational(Pipe(expr, op))`
/// For `Query::WithCtes { ctes, query }`: wraps the main query expression
fn wrap_query_with_pipe(
    query: ast_unresolved::Query,
    operator: ast_unresolved::UnaryRelationalOperator,
) -> ast_unresolved::Query {
    use crate::pipeline::asts::core::PipeExpression;

    match query {
        ast_unresolved::Query::Relational(expr) => {
            ast_unresolved::Query::Relational(ast_unresolved::RelationalExpression::Pipe(Box::new(
                stacksafe::StackSafe::new(PipeExpression {
                    source: expr,
                    operator,
                    cpr_schema: ast_unresolved::PhaseBox::phantom(),
                }),
            )))
        }
        ast_unresolved::Query::WithCtes { ctes, query: main } => {
            let wrapped_main = ast_unresolved::RelationalExpression::Pipe(Box::new(
                stacksafe::StackSafe::new(PipeExpression {
                    source: main,
                    operator,
                    cpr_schema: ast_unresolved::PhaseBox::phantom(),
                }),
            ));
            ast_unresolved::Query::WithCtes {
                ctes,
                query: wrapped_main,
            }
        }
        ast_unresolved::Query::WithErContext {
            context,
            query: inner,
        } => {
            let wrapped_inner = wrap_query_with_pipe(*inner, operator);
            ast_unresolved::Query::WithErContext {
                context,
                query: Box::new(wrapped_inner),
            }
        }
        other => other, // Other query forms pass through unchanged
    }
}

/// Split first-parens DomainSpec into table bindings and scalar DomainSpec.
///
/// For each position in `entity.params`:
/// - Table param (Glob/Argumentative): extract value from first_parens, put in HoParamBindings
/// - Scalar param (Scalar/GroundScalar): leave in the scalar DomainSpec for PatternResolver
/// - @ (ValuePlaceholder): mark that position as pipe target
///
/// Returns: (table_bindings, scalar_domain_spec, pipe_target_index)
pub(super) fn split_ho_first_parens(
    first_parens_spec: &ast_unresolved::DomainSpec,
    entity: &crate::resolution::registry::ConsultedEntity,
    pipe_source: Option<&ast_unresolved::RelationalExpression>,
    argument_groups: Option<&[crate::pipeline::asts::core::operators::HoCallGroup]>,
) -> Result<(HoParamBindings, ast_unresolved::DomainSpec, Option<usize>)> {
    use crate::resolution::registry::HoParamKind;

    // Compute position analysis for MixedGround detection
    let positions = if !entity.positions.is_empty() {
        entity.positions.clone()
    } else {
        let defs = crate::ddl::ddl_builder::build_ddl_file(&entity.definition).unwrap_or_default();
        build_ho_position_analysis(&defs)
    };

    let exprs = match first_parens_spec {
        ast_unresolved::DomainSpec::Positional(exprs) => exprs,
        ast_unresolved::DomainSpec::Glob | ast_unresolved::DomainSpec::Bare => {
            // No explicit args — but if piped, bind first table param to pipe source
            let mut bindings = HoParamBindings::default();
            let mut pipe_target_idx = None;
            if pipe_source.is_some() && !entity.params.is_empty() {
                // Find the first table-value parameter (Glob or Argumentative)
                let first_table_param = entity.params.iter().enumerate().find(|(_, p)| {
                    matches!(p.kind, HoParamKind::Glob | HoParamKind::Argumentative(_))
                });
                if first_table_param.is_none() {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "Higher-order view '{}' has no table-value parameter to receive pipe input \
                             (all parameters are scalar)",
                            entity.name
                        ),
                        "A piped HO view must have at least one table-value parameter (e.g. T(*)) \
                         as the target for the pipe input",
                    ));
                }
                pipe_target_idx = Some(0);
                let first_param = &entity.params[0];
                let cte_name = "_ho_pipe_src".to_string();
                match &first_param.kind {
                    HoParamKind::Argumentative(columns) => {
                        let col_exprs: Vec<ast_unresolved::DomainExpression> = columns
                            .iter()
                            .map(|c| {
                                ast_unresolved::DomainExpression::lvar_builder(c.clone()).build()
                            })
                            .collect();
                        let cte_rel = ast_unresolved::RelationalExpression::Relation(
                            ast_unresolved::Relation::Ground {
                                identifier: ast_unresolved::QualifiedName {
                                    namespace_path: ast_unresolved::NamespacePath::empty(),
                                    name: cte_name.into(),
                                    grounding: None,
                                },
                                canonical_name: ast_unresolved::PhaseBox::phantom(),
                                domain_spec: ast_unresolved::DomainSpec::Positional(col_exprs),
                                alias: None,
                                outer: false,
                                mutation_target: false,
                                passthrough: false,
                                cpr_schema: ast_unresolved::PhaseBox::phantom(),
                                hygienic_injections: Vec::new(),
                            },
                        );
                        bindings
                            .table_expr_params
                            .insert(first_param.name.clone(), cte_rel);
                    }
                    _ => {
                        bindings
                            .table_params
                            .insert(first_param.name.clone(), cte_name);
                    }
                }
            }
            let spec = if matches!(first_parens_spec, ast_unresolved::DomainSpec::Glob) {
                ast_unresolved::DomainSpec::Glob
            } else {
                ast_unresolved::DomainSpec::Bare
            };
            return Ok((bindings, spec, pipe_target_idx));
        }
        _ => {
            return Ok((HoParamBindings::default(), first_parens_spec.clone(), None));
        }
    };

    let mut bindings = HoParamBindings::default();
    let mut scalar_exprs = Vec::new();
    let mut pipe_target_idx = None;
    let mut expr_idx = 0;
    let mut group_idx = 0usize; // tracks which &-group we're in for Argumentative params

    // Check if any expression is @. If piped with no @, the first table param
    // gets the pipe source implicitly — we must skip it in the expr-matching loop.
    let has_at = exprs
        .iter()
        .any(|e| matches!(e, ast_unresolved::DomainExpression::ValuePlaceholder { .. }));
    let implicit_pipe_target = pipe_source.is_some() && !has_at;

    for (param_idx, param) in entity.params.iter().enumerate() {
        // Implicit pipe target: first table param gets pipe source, skip it
        if implicit_pipe_target
            && pipe_target_idx.is_none()
            && matches!(
                param.kind,
                HoParamKind::Glob | HoParamKind::Argumentative(_)
            )
        {
            pipe_target_idx = Some(param_idx);
            let cte_name = "_ho_pipe_src".to_string();
            match &param.kind {
                HoParamKind::Argumentative(columns) => {
                    let col_exprs: Vec<ast_unresolved::DomainExpression> = columns
                        .iter()
                        .map(|c| ast_unresolved::DomainExpression::lvar_builder(c.clone()).build())
                        .collect();
                    let cte_rel = ast_unresolved::RelationalExpression::Relation(
                        ast_unresolved::Relation::Ground {
                            identifier: ast_unresolved::QualifiedName {
                                namespace_path: ast_unresolved::NamespacePath::empty(),
                                name: cte_name.into(),
                                grounding: None,
                            },
                            canonical_name: ast_unresolved::PhaseBox::phantom(),
                            domain_spec: ast_unresolved::DomainSpec::Positional(col_exprs),
                            alias: None,
                            outer: false,
                            mutation_target: false,
                            passthrough: false,
                            cpr_schema: ast_unresolved::PhaseBox::phantom(),
                            hygienic_injections: Vec::new(),
                        },
                    );
                    bindings
                        .table_expr_params
                        .insert(param.name.clone(), cte_rel);
                }
                _ => {
                    bindings.table_params.insert(param.name.clone(), cte_name);
                }
            }
            continue; // Don't consume an expr for this param
        }

        if expr_idx >= exprs.len() {
            break;
        }

        let expr = &exprs[expr_idx];

        // Check for @ (explicit pipe target)
        if matches!(
            expr,
            ast_unresolved::DomainExpression::ValuePlaceholder { .. }
        ) {
            pipe_target_idx = Some(param_idx);
            if pipe_source.is_some() {
                let cte_name = "_ho_pipe_src".to_string();
                match &param.kind {
                    HoParamKind::Argumentative(columns) => {
                        let col_exprs: Vec<ast_unresolved::DomainExpression> = columns
                            .iter()
                            .map(|c| {
                                ast_unresolved::DomainExpression::lvar_builder(c.clone()).build()
                            })
                            .collect();
                        let cte_rel = ast_unresolved::RelationalExpression::Relation(
                            ast_unresolved::Relation::Ground {
                                identifier: ast_unresolved::QualifiedName {
                                    namespace_path: ast_unresolved::NamespacePath::empty(),
                                    name: cte_name.into(),
                                    grounding: None,
                                },
                                canonical_name: ast_unresolved::PhaseBox::phantom(),
                                domain_spec: ast_unresolved::DomainSpec::Positional(col_exprs),
                                alias: None,
                                outer: false,
                                mutation_target: false,
                                passthrough: false,
                                cpr_schema: ast_unresolved::PhaseBox::phantom(),
                                hygienic_injections: Vec::new(),
                            },
                        );
                        bindings
                            .table_expr_params
                            .insert(param.name.clone(), cte_rel);
                    }
                    _ => {
                        bindings.table_params.insert(param.name.clone(), cte_name);
                    }
                }
            }
            expr_idx += 1;
            continue;
        }

        match &param.kind {
            HoParamKind::Glob => {
                // Glob table param: extract name from expression
                match expr {
                    ast_unresolved::DomainExpression::Lvar { name, .. } => {
                        bindings
                            .table_params
                            .insert(param.name.clone(), name.to_string());
                    }
                    ast_unresolved::DomainExpression::Literal {
                        value: crate::pipeline::asts::core::LiteralValue::String(s),
                        ..
                    } => {
                        bindings.table_params.insert(param.name.clone(), s.clone());
                    }
                    _ => {
                        return Err(DelightQLError::validation_error(
                            format!(
                                "Expected table name at position {} for param '{}', got {:?}",
                                param_idx, param.name, expr
                            ),
                            "Glob table parameter must be a table name or variable",
                        ));
                    }
                }
                expr_idx += 1;
                group_idx += 1;
            }
            HoParamKind::Argumentative(columns) => {
                // Argumentative table param: either a table ref (Lvar) or scalar lift
                match expr {
                    ast_unresolved::DomainExpression::Lvar { name, .. } => {
                        // Table reference — bind by name, register for arity check
                        bindings
                            .table_params
                            .insert(param.name.clone(), name.to_string());
                        bindings.argumentative_table_refs.push((
                            param.name.clone(),
                            name.to_string(),
                            columns.len(),
                            columns.clone(),
                        ));
                        expr_idx += 1;
                        group_idx += 1;
                    }
                    _ => {
                        // Scalar lift: consume rows of N exprs each and build anon table.
                        // Multiple rows arise from `;` separator: pivot_by("Maths";"Music").
                        let n_cols = columns.len();
                        let mut all_rows = Vec::new();

                        // When argument_groups are available, use the group's row count
                        // to bound consumption. This prevents greedy consumption when
                        // multiple table params have consecutive scalar lifts (& separator).
                        let max_rows = argument_groups
                            .and_then(|groups| groups.get(group_idx))
                            .map(|g| g.rows.len());

                        loop {
                            if expr_idx >= exprs.len() {
                                break;
                            }
                            // If we know the exact row count from groups, stop when reached
                            if let Some(max) = max_rows {
                                if all_rows.len() >= max {
                                    break;
                                }
                            }
                            // Check if the next expr is a literal (part of this row)
                            // or an Lvar (next param / end of scalar lift)
                            let next = &exprs[expr_idx];
                            let is_literal =
                                matches!(next, ast_unresolved::DomainExpression::Literal { .. });
                            if !is_literal && all_rows.is_empty() {
                                // First value is not a literal — error
                                return Err(DelightQLError::validation_error(
                                    format!(
                                        "Argumentative param '{}' expects literal values for scalar lift, \
                                         got {:?}",
                                        param.name, next
                                    ),
                                    "Scalar lift values must be literals",
                                ));
                            }
                            if !is_literal {
                                // Non-literal after at least one row → stop consuming
                                break;
                            }

                            let mut row_values = Vec::with_capacity(n_cols);
                            for col_idx in 0..n_cols {
                                if expr_idx + col_idx >= exprs.len() {
                                    return Err(DelightQLError::validation_error(
                                        format!(
                                            "Argumentative param '{}' expects {} values per row, \
                                             but only {} remain at position {}",
                                            param.name,
                                            n_cols,
                                            exprs.len() - expr_idx,
                                            param_idx
                                        ),
                                        "Not enough values for argumentative scalar lift row",
                                    ));
                                }
                                let val_expr = &exprs[expr_idx + col_idx];
                                let val_str = match val_expr {
                                    ast_unresolved::DomainExpression::Literal {
                                        value: crate::pipeline::asts::core::LiteralValue::String(s),
                                        ..
                                    } => format!("\"{}\"", s),
                                    ast_unresolved::DomainExpression::Literal {
                                        value: crate::pipeline::asts::core::LiteralValue::Number(n),
                                        ..
                                    } => n.clone(),
                                    other => {
                                        return Err(DelightQLError::validation_error(
                                            format!(
                                                "Unsupported expression in scalar lift for param '{}' column {}: {:?}",
                                                param.name, col_idx, other
                                            ),
                                            "Scalar lift values must be literals",
                                        ));
                                    }
                                };
                                row_values.push(val_str);
                            }
                            expr_idx += n_cols;
                            all_rows.push(row_values);
                        }

                        if all_rows.is_empty() {
                            return Err(DelightQLError::validation_error(
                                format!(
                                    "Argumentative param '{}' got no values for scalar lift",
                                    param.name,
                                ),
                                "No values for argumentative scalar lift",
                            ));
                        }

                        let anon_table = lift_scalars_to_anonymous_table(columns, &all_rows)?;
                        bindings
                            .table_expr_params
                            .insert(param.name.clone(), anon_table);
                        group_idx += 1;
                    }
                }
            }
            HoParamKind::Scalar => {
                // Check if this position is MixedGround — needs BOTH text substitution
                // AND PatternResolver filtering
                let is_mixed_ground = positions.iter().any(|pi| {
                    pi.position == param_idx
                        && pi.ground_mode == crate::pipeline::asts::ddl::HoGroundMode::MixedGround
                });

                // Text substitution for free-variable clauses
                bindings
                    .scalar_params
                    .insert(param.name.clone(), expr.clone());

                if is_mixed_ground {
                    // MixedGround: also add to scalar_exprs for PatternResolver
                    scalar_exprs.push(expr.clone());
                }
                expr_idx += 1;
            }
            HoParamKind::GroundScalar(_) => {
                // GroundScalar: goes to PatternResolver via scalar_exprs
                scalar_exprs.push(expr.clone());
                expr_idx += 1;
            }
        }
    }

    // If piped but no table-value parameter was found, error out
    if pipe_source.is_some() && pipe_target_idx.is_none() {
        return Err(DelightQLError::validation_error(
            format!(
                "Higher-order view '{}' has no table-value parameter to receive pipe input \
                 (all parameters are scalar)",
                entity.name
            ),
            "A piped HO view must have at least one table-value parameter (e.g. T(*)) \
             as the target for the pipe input",
        ));
    }

    let scalar_spec = if scalar_exprs.is_empty() {
        ast_unresolved::DomainSpec::Glob
    } else {
        ast_unresolved::DomainSpec::Positional(scalar_exprs)
    };

    Ok((bindings, scalar_spec, pipe_target_idx))
}

/// Ensure all HO position infos have column names.
/// For Scalar (free-variable) positions, use the DDL param variable name.
/// For PureGround (all-literal) positions, generate `_label_N`.
pub(super) fn ensure_position_column_names(
    positions: Vec<HoPositionInfo>,
    defs: &[crate::pipeline::asts::ddl::DdlDefinition],
) -> Vec<HoPositionInfo> {
    positions
        .into_iter()
        .map(|mut pi| {
            if pi.column_name.is_none() {
                for def in defs {
                    if let DdlHead::HoView { params, .. } = &def.head {
                        if let Some(p) = params.get(pi.position) {
                            if matches!(p.kind, crate::pipeline::asts::ddl::HoParamKind::Scalar) {
                                pi.column_name = Some(p.name.clone());
                                break;
                            }
                        }
                    }
                }
                if pi.column_name.is_none() {
                    pi.column_name = Some(format!("_label_{}", pi.position));
                }
            }
            pi
        })
        .collect()
}

/// Extract CTE bindings from a clause query, handling WithErContext by unwrapping.
/// Returns the CTEs and any ER context that needs to wrap the final output.
fn extract_clause_ctes(
    clause_query: ast_unresolved::Query,
    function: &str,
    all_ctes: &mut Vec<ast_unresolved::CteBinding>,
    er_context: &mut Option<crate::pipeline::asts::core::ErContextSpec>,
) -> Result<()> {
    match clause_query {
        ast_unresolved::Query::Relational(expr) => {
            all_ctes.push(ast_unresolved::CteBinding {
                name: function.to_string(),
                expression: expr,
                is_recursive: ast_unresolved::PhaseBox::phantom(),
            });
        }
        ast_unresolved::Query::WithCtes {
            ctes,
            query: main_expr,
        } => {
            for cte in ctes {
                all_ctes.push(cte);
            }
            all_ctes.push(ast_unresolved::CteBinding {
                name: function.to_string(),
                expression: main_expr,
                is_recursive: ast_unresolved::PhaseBox::phantom(),
            });
        }
        ast_unresolved::Query::WithErContext {
            context,
            query: inner,
        } => {
            // Capture ER context from first clause that has one
            if er_context.is_none() {
                *er_context = Some(context);
            }
            // Recursively process the inner query
            extract_clause_ctes(*inner, function, all_ctes, er_context)?;
        }
        other => {
            return Err(DelightQLError::parse_error(format!(
                "Unsupported query form in squished HO view clause: {:?}",
                std::mem::discriminant(&other)
            )));
        }
    }
    Ok(())
}

/// Inject a cross-join with the input table into a clause body's FROM clause.
/// Used by the inverted CTE strategy: when a clause body has free scalar params,
/// the input source (caller's data) must be in the FROM so the free vars resolve.
///
/// Wraps: `body` → `_input(*), body`
fn inject_input_table_into_query(
    clause_query: ast_unresolved::Query,
    input_table_name: &str,
) -> ast_unresolved::Query {
    let input_table =
        ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::Ground {
            identifier: ast_unresolved::QualifiedName {
                namespace_path: ast_unresolved::NamespacePath::empty(),
                name: input_table_name.into(),
                grounding: None,
            },
            canonical_name: ast_unresolved::PhaseBox::phantom(),
            domain_spec: ast_unresolved::DomainSpec::Glob,
            alias: None,
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: ast_unresolved::PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        });

    match clause_query {
        ast_unresolved::Query::Relational(expr) => {
            ast_unresolved::Query::Relational(ast_unresolved::RelationalExpression::Join {
                left: Box::new(input_table),
                right: Box::new(expr),
                join_condition: None,
                join_type: Some(crate::pipeline::asts::core::JoinType::Inner),
                cpr_schema: ast_unresolved::PhaseBox::phantom(),
            })
        }
        ast_unresolved::Query::WithCtes { ctes, query } => {
            // If the body has its own CTEs, inject into the main query part
            ast_unresolved::Query::WithCtes {
                ctes,
                query: ast_unresolved::RelationalExpression::Join {
                    left: Box::new(input_table),
                    right: Box::new(query),
                    join_condition: None,
                    join_type: Some(crate::pipeline::asts::core::JoinType::Inner),
                    cpr_schema: ast_unresolved::PhaseBox::phantom(),
                },
            }
        }
        other => other, // ErContext etc. — pass through unchanged
    }
}

/// Build the SQUISHED relation: ALL clauses as a UNION ALL, with scalar
/// positions injected as columns. No clause pre-filtering — PatternResolver
/// handles filtering via WHERE constraints after resolution.
///
/// Returns an unresolved Query with CTEs: one per clause (named `function`),
/// plus an optional pipe source CTE. The main query is `function(*)`.
pub(super) fn build_squished_relation(
    function: &str,
    entity: &crate::resolution::registry::ConsultedEntity,
    table_bindings: crate::pipeline::query_features::HoParamBindings,
    pipe_source_cte: Option<(String, ast_unresolved::RelationalExpression)>,
    join_input_cte: Option<(String, ast_unresolved::RelationalExpression)>,
    data_ns: Option<&ast_unresolved::NamespacePath>,
) -> Result<ast_unresolved::Query> {
    let defs = crate::ddl::ddl_builder::build_ddl_file(&entity.definition).unwrap_or_default();

    let positions = if !entity.positions.is_empty() {
        entity.positions.clone()
    } else {
        build_ho_position_analysis(&defs)
    };

    let positions = ensure_position_column_names(positions, &defs);

    let mut all_ctes = Vec::new();
    let mut er_context: Option<crate::pipeline::asts::core::ErContextSpec> = None;

    // Prepend pipe source CTE if present
    if let Some((cte_name, source_expr)) = pipe_source_cte {
        all_ctes.push(ast_unresolved::CteBinding {
            expression: source_expr,
            name: cte_name,
            is_recursive: ast_unresolved::PhaseBox::phantom(),
        });
    }

    // Prepend join input CTE if present (inverted CTE strategy for free scalar params).
    // The join_input_cte_name is used to inject a FROM reference into correlated clause bodies.
    let join_input_cte_name = if let Some((cte_name, source_expr)) = join_input_cte {
        let name = cte_name.clone();
        all_ctes.push(ast_unresolved::CteBinding {
            expression: source_expr,
            name: cte_name,
            is_recursive: ast_unresolved::PhaseBox::phantom(),
        });
        Some(name)
    } else {
        None
    };

    // Detect which scalar params are free (Lvar call-site expressions).
    // These need the _input table injected into correlated clause bodies.
    let free_scalar_param_names: Vec<String> = if join_input_cte_name.is_some() {
        table_bindings
            .scalar_params
            .iter()
            .filter(|(_, expr)| matches!(expr, ast_unresolved::DomainExpression::Lvar { .. }))
            .map(|(name, _)| name.clone())
            .collect()
    } else {
        Vec::new()
    };

    if defs.len() > 1 {
        // Multi-clause: each clause becomes a CTE
        for def in &defs {
            let clause_params = match &def.head {
                DdlHead::HoView { params, .. } => params.clone(),
                _ => Vec::new(),
            };
            let output_head = match &def.head {
                DdlHead::HoView { output_head, .. } => output_head.as_deref(),
                _ => None,
            };

            // Create per-clause bindings: for GroundScalar positions that are Scalar
            // in this clause, bind the ground value as a scalar param.
            let clause_bindings = table_bindings.clone();
            for (pos, cp) in clause_params.iter().enumerate() {
                if let crate::pipeline::asts::ddl::HoParamKind::Scalar = &cp.kind {
                    if !clause_bindings.scalar_params.contains_key(&cp.name) {
                        if let Some(pos_info) = positions.iter().find(|pi| pi.position == pos) {
                            if pos_info.ground_mode
                                == crate::pipeline::asts::ddl::HoGroundMode::MixedGround
                            {
                                // MixedGround: handled by caller providing scalar_params in bindings.
                            }
                        }
                    }
                }
            }

            let clause_query = {
                let q = crate::ddl::body_parser::parse_view_body_with_bindings(
                    &def.full_source,
                    clause_bindings,
                )?;
                // Inverted CTE: inject _input table BEFORE inject_scalar_columns,
                // so the embed pipe wraps the join (not vice versa). This ensures
                // anonymous tables with column refs are on the right side of a join
                // where the MeltTable/json_each strategy can handle them.
                let q = if let Some(ref input_name) = join_input_cte_name {
                    let clause_uses_free_scalar = clause_params.iter().any(|cp| {
                        matches!(cp.kind, crate::pipeline::asts::ddl::HoParamKind::Scalar)
                            && free_scalar_param_names.contains(&cp.name)
                    });
                    if clause_uses_free_scalar {
                        inject_input_table_into_query(q, input_name)
                    } else {
                        q
                    }
                } else {
                    q
                };
                inject_scalar_columns(q, &clause_params, &positions, output_head)
            };
            let clause_query = if let Some(dns) = data_ns {
                patch_data_ns_query(clause_query, dns)
            } else {
                clause_query
            };

            extract_clause_ctes(clause_query, function, &mut all_ctes, &mut er_context)?;
        }
    } else {
        // Single clause
        let clause_query = {
            let q = crate::ddl::body_parser::parse_view_body_with_bindings(
                &entity.definition,
                table_bindings,
            )?;
            if let Some(def) = defs.first() {
                let clause_params = match &def.head {
                    DdlHead::HoView { params, .. } => params.clone(),
                    _ => Vec::new(),
                };
                let output_head = match &def.head {
                    DdlHead::HoView { output_head, .. } => output_head.as_deref(),
                    _ => None,
                };
                // Inverted CTE: inject _input table BEFORE inject_scalar_columns,
                // so the embed pipe wraps the join (not vice versa). This ensures
                // anonymous tables with column refs are on the right side of a join
                // where the MeltTable/json_each strategy can handle them.
                let q = if let Some(ref input_name) = join_input_cte_name {
                    let clause_uses_free_scalar = clause_params.iter().any(|cp| {
                        matches!(cp.kind, crate::pipeline::asts::ddl::HoParamKind::Scalar)
                            && free_scalar_param_names.contains(&cp.name)
                    });
                    if clause_uses_free_scalar {
                        inject_input_table_into_query(q, input_name)
                    } else {
                        q
                    }
                } else {
                    q
                };
                inject_scalar_columns(q, &clause_params, &positions, output_head)
            } else {
                q
            }
        };
        let clause_query = if let Some(dns) = data_ns {
            patch_data_ns_query(clause_query, dns)
        } else {
            clause_query
        };

        extract_clause_ctes(clause_query, function, &mut all_ctes, &mut er_context)?;
    }

    // Main query: function(*) referencing the CTE
    let main_query =
        ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::Ground {
            identifier: ast_unresolved::QualifiedName {
                namespace_path: ast_unresolved::NamespacePath::empty(),
                name: function.into(),
                grounding: None,
            },
            canonical_name: ast_unresolved::PhaseBox::phantom(),
            domain_spec: ast_unresolved::DomainSpec::Glob,
            alias: None,
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: ast_unresolved::PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        });

    let result = ast_unresolved::Query::WithCtes {
        ctes: all_ctes,
        query: main_query,
    };

    // If any clause had an ER context, wrap the final query with it
    if let Some(context) = er_context {
        Ok(ast_unresolved::Query::WithErContext {
            context,
            query: Box::new(result),
        })
    } else {
        Ok(result)
    }
}

/// Result of binding call-site arguments to HO view parameters using kind metadata.
pub(crate) use crate::pipeline::query_features::HoParamBindings;

/// Create synthetic "proffer" bindings for an HO view's parameters.
///
/// Used at consult time to parse the view body with placeholder values,
/// enabling early validation of syntax and structure without real call-site args.
///
/// - Glob params get `__proffer__<name>` table names
/// - Argumentative params get anonymous tables with NULL data
/// - Scalar params get `Literal(Null)`
pub(crate) fn create_proffer_bindings(
    head: &crate::pipeline::asts::ddl::DdlHead,
) -> HoParamBindings {
    use crate::pipeline::asts::ddl::{DdlHead, HoParamKind};
    match head {
        DdlHead::HoView { params, .. } => {
            let mut bindings = HoParamBindings::default();
            for param in params {
                match &param.kind {
                    HoParamKind::Glob => {
                        bindings
                            .table_params
                            .insert(param.name.clone(), format!("__proffer__{}", param.name));
                    }
                    HoParamKind::Argumentative(columns) => {
                        let null_row: Vec<String> =
                            columns.iter().map(|_| "null".to_string()).collect();
                        match lift_scalars_to_anonymous_table(columns, &[null_row]) {
                            Ok(anon) => {
                                bindings.table_expr_params.insert(param.name.clone(), anon);
                            }
                            Err(_) => {
                                // Fallback: treat as glob
                                bindings.table_params.insert(
                                    param.name.clone(),
                                    format!("__proffer__{}", param.name),
                                );
                            }
                        }
                    }
                    HoParamKind::Scalar => {
                        bindings.scalar_params.insert(
                            param.name.clone(),
                            ast_unresolved::DomainExpression::Literal {
                                value: crate::pipeline::asts::core::LiteralValue::Null,
                                alias: None,
                            },
                        );
                        bindings
                            .table_params
                            .insert(param.name.clone(), format!("__proffer__{}", param.name));
                    }
                    HoParamKind::GroundScalar(value) => {
                        // Ground scalars are constants, not parameters — use the literal value
                        bindings.scalar_params.insert(
                            param.name.clone(),
                            ast_unresolved::DomainExpression::Literal {
                                value: crate::pipeline::asts::core::LiteralValue::Null,
                                alias: None,
                            },
                        );
                        bindings
                            .table_params
                            .insert(param.name.clone(), value.clone());
                    }
                }
            }
            bindings
        }
        other => panic!(
            "catch-all hit in grounding.rs extract_ho_bindings (FunctionExpression): {:?}",
            other
        ),
    }
}

/// Synthesize an anonymous table `_(col1, col2 ---- v1, v2; v3, v4)` from column names and rows.
///
/// Routes through the DQL body parser — no mini-pipeline.
pub(crate) fn lift_scalars_to_anonymous_table(
    column_names: &[String],
    rows: &[Vec<String>],
) -> Result<ast_unresolved::RelationalExpression> {
    // Build the DQL text: _(col1, col2 ---- v1, v2; v3, v4)
    let headers = column_names.join(", ");
    let row_strs: Vec<String> = rows.iter().map(|row| row.join(", ")).collect();
    let data = row_strs.join("; ");
    let anon_source = format!("_({} ---- {})", headers, data);

    debug!("Lifting scalars to anonymous table: {}", anon_source);

    let query = crate::ddl::body_parser::parse_view_body(&anon_source)?;
    match query {
        ast_unresolved::Query::Relational(expr) => Ok(expr),
        _ => Err(DelightQLError::parse_error(format!(
            "Expected relational expression from anonymous table '{}', got CTE",
            anon_source
        ))),
    }
}

/// Validate arity for argumentative params that received table references.
///
/// Argumentative params declare exact width: `V(k, l)` means the passed table
/// must have exactly 2 columns. This checks pending arity constraints against
/// the registry (CTEs, ground tables).
pub(super) fn validate_argumentative_arity(
    bindings: &HoParamBindings,
    registry: &crate::resolution::EntityRegistry,
) -> Result<()> {
    use crate::pipeline::ast_resolved::CprSchema;

    for (param_name, table_name, expected_cols, col_names) in &bindings.argumentative_table_refs {
        // Try CTE first, then ground table
        let actual_cols = if let Some(schema) = registry.query_local.lookup_cte(table_name) {
            match schema {
                CprSchema::Resolved(cols) => Some(cols.len()),
                other => panic!(
                    "catch-all hit in grounding.rs validate_argumentative_arity (CTE lookup): {:?}",
                    other
                ),
            }
        } else if let Some(schema) = registry.database.lookup_table(table_name) {
            match schema {
                CprSchema::Resolved(cols) => Some(cols.len()),
                other => panic!("catch-all hit in grounding.rs validate_argumentative_arity (table lookup): {:?}", other),
            }
        } else {
            // Table not found here — will fail during resolution with a "table not found" error
            None
        };

        if let Some(actual) = actual_cols {
            if actual != *expected_cols {
                return Err(DelightQLError::validation_error_categorized(
                    "constraint/ho_param/argumentative_functor/arity",
                    format!(
                        "Argumentative parameter '{}({})' expects {} column{} but table '{}' has {}",
                        param_name,
                        col_names.join(", "),
                        expected_cols,
                        if *expected_cols == 1 { "" } else { "s" },
                        table_name,
                        actual,
                    ),
                    "HO parameter arity mismatch",
                ));
            }
        }
    }
    Ok(())
}

/// Build a remap from argumentative lvar names to (table_name, actual_column_name).
///
/// For `V(k, l)` bound to `refs(key, label)`, produces `{k → ("refs", "key"), l → ("refs", "label")}`.
/// This allows the body parser to substitute bare lvars with qualified column references.
pub(super) fn build_argumentative_column_remap(
    bindings: &crate::pipeline::query_features::HoParamBindings,
    registry: &crate::resolution::EntityRegistry,
) -> HashMap<String, (String, String)> {
    use crate::pipeline::ast_resolved::CprSchema;

    let mut remap = HashMap::new();
    for (_param_name, table_name, _expected_cols, col_names) in &bindings.argumentative_table_refs {
        let actual_col_names = if let Some(schema) = registry.query_local.lookup_cte(table_name) {
            match schema {
                CprSchema::Resolved(cols) => Some(
                    cols.iter()
                        .map(|c| c.info.name().unwrap_or("?").to_string())
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            }
        } else if let Some(schema) = registry.database.lookup_table(table_name) {
            match schema {
                CprSchema::Resolved(cols) => Some(
                    cols.iter()
                        .map(|c| c.info.name().unwrap_or("?").to_string())
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            }
        } else {
            None
        };

        if let Some(actual_names) = actual_col_names {
            for (lvar_name, actual_name) in col_names.iter().zip(actual_names.iter()) {
                remap.insert(lvar_name.clone(), (table_name.clone(), actual_name.clone()));
            }
        }
    }
    remap
}

// ============================================================================
// Namespace patching — DataNsPatcher fold
// ============================================================================

/// Patches unqualified table references (Ground, TVF, InnerRelation identifiers,
/// ScalarSubquery identifiers) to use the data namespace. The default walk
/// functions recurse into all children, so filter conditions, operator
/// expressions, and nested domain expressions also get patched.
struct DataNsPatcher<'a> {
    data_ns: &'a ast_unresolved::NamespacePath,
}

impl AstTransform<Unresolved, Unresolved> for DataNsPatcher<'_> {
    fn transform_relation(&mut self, r: Relation<Unresolved>) -> Result<Relation<Unresolved>> {
        match r {
            Relation::Ground {
                mut identifier,
                canonical_name,
                domain_spec,
                alias,
                outer,
                mutation_target,
                passthrough,
                cpr_schema,
                hygienic_injections,
            } => {
                if identifier.namespace_path.is_empty() {
                    identifier.namespace_path = self.data_ns.clone();
                }
                // Don't recurse further — Ground's children (domain_spec) don't
                // contain table references that need patching. Use walk_relation
                // only if we want full recursion into domain_spec expressions.
                Ok(Relation::Ground {
                    identifier,
                    canonical_name,
                    domain_spec,
                    alias,
                    outer,
                    mutation_target,
                    passthrough,
                    cpr_schema,
                    hygienic_injections,
                })
            }
            Relation::TVF {
                function,
                arguments,
                argument_groups,
                first_parens_spec,
                domain_spec,
                alias,
                mut namespace,
                grounding,
                cpr_schema,
            } => {
                if namespace.is_none() {
                    namespace = Some(self.data_ns.clone());
                }
                Ok(Relation::TVF {
                    function,
                    arguments,
                    argument_groups,
                    first_parens_spec,
                    domain_spec,
                    alias,
                    namespace,
                    grounding,
                    cpr_schema,
                })
            }
            // InnerRelation: delegate to transform_inner_relation for identifier patching
            other => walk_transform_relation(self, other),
        }
    }

    fn transform_inner_relation(
        &mut self,
        i: InnerRelationPattern<Unresolved>,
    ) -> Result<InnerRelationPattern<Unresolved>> {
        match i {
            InnerRelationPattern::Indeterminate {
                mut identifier,
                subquery,
            } => {
                if identifier.namespace_path.is_empty() {
                    identifier.namespace_path = self.data_ns.clone();
                }
                Ok(InnerRelationPattern::Indeterminate {
                    identifier,
                    subquery: Box::new(self.transform_relational(*subquery)?),
                })
            }
            InnerRelationPattern::UncorrelatedDerivedTable {
                mut identifier,
                subquery,
                is_consulted_view,
            } => {
                if identifier.namespace_path.is_empty() {
                    identifier.namespace_path = self.data_ns.clone();
                }
                Ok(InnerRelationPattern::UncorrelatedDerivedTable {
                    identifier,
                    subquery: Box::new(self.transform_relational(*subquery)?),
                    is_consulted_view,
                })
            }
            other => walk_transform_inner_relation(self, other),
        }
    }

    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Unresolved>> {
        match expr {
            DomainExpression::ScalarSubquery {
                mut identifier,
                subquery,
                alias,
            } => {
                if identifier.namespace_path.is_empty() {
                    identifier.namespace_path = self.data_ns.clone();
                }
                let patched_subquery = self.transform_relational(*subquery)?;
                Ok(DomainExpression::ScalarSubquery {
                    identifier,
                    subquery: Box::new(patched_subquery),
                    alias,
                })
            }
            other => walk_transform_domain(self, other),
        }
    }
}

/// Patch data namespace on all table references in a Query.
pub(super) fn patch_data_ns_query(
    query: ast_unresolved::Query,
    data_ns: &ast_unresolved::NamespacePath,
) -> ast_unresolved::Query {
    DataNsPatcher { data_ns }
        .transform_query(query)
        .expect("namespace patching is infallible")
}

/// Patch data_ns on ScalarSubquery identifiers within a domain expression.
fn patch_data_ns_in_domain_expr(
    expr: ast_unresolved::DomainExpression,
    data_ns: &ast_unresolved::NamespacePath,
) -> ast_unresolved::DomainExpression {
    DataNsPatcher { data_ns }
        .transform_domain(expr)
        .expect("namespace patching is infallible")
}
