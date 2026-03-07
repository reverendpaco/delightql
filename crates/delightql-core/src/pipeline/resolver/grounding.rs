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
use crate::pipeline::ast_fold::{
    walk_domain, walk_inner_relation, walk_operator, walk_relation, AstFold,
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
pub(super) fn namespace_path_to_fq(ns: &ast_unresolved::NamespacePath) -> String {
    let parts: Vec<&str> = ns.iter().map(|i| i.name.as_str()).collect();
    parts.join("::")
}

/// Inline consulted functions in a unary relational operator.
///
/// Walks the operator's domain expressions, replacing function calls
/// that match consulted definitions with their inlined bodies.
pub(super) fn inline_consulted_functions_in_operator(
    operator: ast_unresolved::UnaryRelationalOperator,
    grounding: &GroundedPath,
    consult: &ConsultRegistry,
) -> Result<ast_unresolved::UnaryRelationalOperator> {
    GroundedInliner { grounding, consult }.fold_operator(operator)
}

// ============================================================================
// Shared inlining helper
// ============================================================================

/// Inline a consulted entity body: parse DDL, substitute params, patch namespace,
/// re-fold the result, and apply alias. Used by both BorrowedInliner and GroundedInliner.
fn inline_entity_body(
    entity: &crate::resolution::registry::ConsultedEntity,
    arguments: &[ast_unresolved::DomainExpression],
    alias: Option<SqlIdentifier>,
    data_ns: Option<&ast_unresolved::NamespacePath>,
    fold: &mut impl AstFold<Unresolved>,
) -> Result<ast_unresolved::DomainExpression> {
    let ddl_defs = ddl_builder::build_ddl_file(&entity.definition)?;
    if ddl_defs.is_empty() {
        return Err(DelightQLError::parse_error(format!(
            "No definitions found for function '{}'",
            entity.name
        )));
    }

    let substituted = if ddl_defs.len() == 1 {
        let body = ddl_defs
            .into_iter()
            .next()
            .unwrap()
            .into_domain_expr()
            .ok_or_else(|| {
                DelightQLError::parse_error(format!(
                    "Expected scalar body for function '{}', got relational",
                    entity.name
                ))
            })?;

        let param_map: HashMap<&str, &ast_unresolved::DomainExpression> = entity
            .params
            .iter()
            .zip(arguments.iter())
            .map(|(p, a)| (p.name.as_str(), a))
            .collect();

        let substituted = substitute_in_domain_expr(body, &param_map);
        if let Some(ns) = data_ns {
            patch_data_ns_in_domain_expr(substituted, ns)
        } else {
            substituted
        }
    } else {
        build_case_from_clauses(ddl_defs, arguments, data_ns)?
    };

    let mut inlined = fold.fold_domain(substituted)?;
    if let Some(alias_name) = alias {
        apply_alias(&mut inlined, alias_name);
    }
    Ok(inlined)
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

/// Convert a consulted entity (type=3) into a CfeDefinition for precompilation.
///
/// Re-parses the stored definition text to extract the context_mode and body,
/// then assembles a CfeDefinition that the CFE precompiler can process.
pub(super) fn consulted_entity_to_cfe_definition(
    entity: &crate::resolution::registry::ConsultedEntity,
) -> Result<CfeDefinition> {
    let ddl_defs = ddl_builder::build_ddl_file(&entity.definition)?;
    let def = ddl_defs.into_iter().next().ok_or_else(|| {
        DelightQLError::parse_error(format!(
            "No definition found for context-aware function '{}'",
            entity.name
        ))
    })?;

    // Extract context_mode BEFORE consuming def
    let context_mode = match &def.head {
        DdlHead::Function { context_mode, .. } => context_mode.clone(),
        _ => ContextMode::None,
    };

    let body = def.into_domain_expr().ok_or_else(|| {
        DelightQLError::parse_error(format!(
            "Expected scalar body for context-aware function '{}', got relational",
            entity.name
        ))
    })?;
    let parameters: Vec<String> = entity.params.iter().map(|p| p.name.clone()).collect();

    Ok(CfeDefinition {
        name: entity.name.to_string(),
        curried_params: vec![],
        parameters,
        context_mode,
        body,
    })
}

// ============================================================================
// Borrowed inlining — BorrowedInliner fold
// ============================================================================

/// Inlines consulted functions found via borrowed (engaged) namespace lookup.
/// Overrides fold_domain for function inlining and piped-expression chain
/// processing, fold_operator for MapCover/EmbedMapCover conversion, and
/// fold_pipe for conditional operator processing (skip when data_ns is None).
struct BorrowedInliner<'a> {
    consult: &'a ConsultRegistry,
    data_ns: Option<&'a ast_unresolved::NamespacePath>,
    /// Context-aware functions (type=3) discovered during fold, to be
    /// precompiled and injected as WithPrecompiledCfes by the resolver.
    collected_ccafe_cfes: Vec<CfeDefinition>,
    /// When true, skip type=1 inlining but still discover type=3 CCAFEs.
    /// Used inside pipe operators when data_ns is None: we need to discover
    /// CCAFEs for precompilation but can't inline type=1 functions without
    /// data_ns patching (that's handled by the per-pipe handler in mod.rs).
    discovery_only: bool,
}

impl AstFold<Unresolved> for BorrowedInliner<'_> {
    fn fold_domain(
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
                        return Ok(DomainExpression::Function(self.fold_function(func)?));
                    }
                };

                // Lookup entity in borrowed namespaces (type=1 — regular functions)
                if !self.discovery_only {
                    let entity =
                        lookup_borrowed_function(&name_str, namespace.as_ref(), self.consult)?;

                    if let Some(entity) = entity {
                        debug!(
                            "Inlining engaged consulted function '{}' from namespace '{}'",
                            name_str, entity.namespace
                        );
                        // Activate namespace-local enlists/aliases into "main" so the
                        // function body can resolve entities from namespaces enlisted
                        // inside its DDL (lookup_enlisted_function searches main).
                        let activated_enlists = self
                            .consult
                            .activate_namespace_local_enlists_into_main(&entity.namespace);
                        let activated_aliases = self
                            .consult
                            .activate_namespace_local_aliases(&entity.namespace);
                        let data_ns = self.data_ns;
                        let result = inline_entity_body(&entity, &arguments, alias, data_ns, self);
                        self.consult
                            .deactivate_namespace_local_aliases(&activated_aliases);
                        self.consult
                            .deactivate_namespace_local_enlists(&activated_enlists);
                        return result;
                    }
                }

                // Try context-aware function (type=3) — don't inline, collect for CFE precompilation
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
                    return Ok(DomainExpression::Function(self.fold_function(func)?));
                }

                // Not a consulted function — recurse into children
                Ok(DomainExpression::Function(self.fold_function(func)?))
            }
            DomainExpression::PipedExpression {
                value,
                transforms,
                alias,
            } => {
                let mut current_value = self.fold_domain(*value)?;
                let mut remaining_transforms = Vec::new();

                for transform in transforms {
                    let transform = self.fold_function(transform)?;
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

                    let inlined = self.fold_domain(synthetic)?;
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
            other => walk_domain(self, other),
        }
    }

    fn fold_operator(
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
                    .map(|e| self.fold_domain(e))
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
                                    let inlined = self.fold_domain(synthetic)?;
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

                let inlined_func = self.fold_function(function)?;
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
                                    self.fold_domain(synthetic)
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
                            .map(|e| self.fold_domain(e))
                            .collect::<Result<Vec<_>>>()?;
                        ast_unresolved::ColumnSelector::Explicit(folded)
                    }
                    other_sel => other_sel,
                };
                Ok(UnaryRelationalOperator::EmbedMapCover {
                    function: self.fold_function(function)?,
                    selector: inlined_selector,
                    alias_template,
                    containment_semantic,
                })
            }
            other => walk_operator(self, other),
        }
    }

    fn fold_pipe(&mut self, p: PipeExpression<Unresolved>) -> Result<PipeExpression<Unresolved>> {
        let source = self.fold_relational(p.source)?;
        let operator = if self.data_ns.is_some() {
            // With data_ns: full inlining (type=1 with namespace patching + type=3 discovery)
            self.fold_operator(p.operator)?
        } else {
            // Without data_ns: discovery-only mode for type=3 CCAFEs.
            // Type=1 functions in operators are handled by the per-pipe handler
            // in mod.rs which has access to grounding context for data_ns patching.
            let prev = self.discovery_only;
            self.discovery_only = true;
            let op = self.fold_operator(p.operator)?;
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
pub(super) fn inline_consulted_functions_in_operator_borrowed(
    operator: ast_unresolved::UnaryRelationalOperator,
    consult: &ConsultRegistry,
    data_ns: Option<&ast_unresolved::NamespacePath>,
) -> Result<ast_unresolved::UnaryRelationalOperator> {
    let mut inliner = BorrowedInliner {
        consult,
        data_ns,
        collected_ccafe_cfes: vec![],
        discovery_only: false,
    };
    inliner.fold_operator(operator)
}

pub(crate) fn inline_in_domain_expr_borrowed(
    expr: ast_unresolved::DomainExpression,
    consult: &ConsultRegistry,
    data_ns: Option<&ast_unresolved::NamespacePath>,
) -> Result<ast_unresolved::DomainExpression> {
    let mut inliner = BorrowedInliner {
        consult,
        data_ns,
        collected_ccafe_cfes: vec![],
        discovery_only: false,
    };
    inliner.fold_domain(expr)
}

// ============================================================================
// GroundedInliner — consulted function inlining (grounded path)
// ============================================================================

struct GroundedInliner<'a> {
    grounding: &'a GroundedPath,
    consult: &'a ConsultRegistry,
}

impl AstFold<Unresolved> for GroundedInliner<'_> {
    fn fold_domain(
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
                    _ => return walk_domain(self, DomainExpression::Function(func)),
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
                    debug!("Inlining consulted function '{}' (grounded path)", name);
                    let data_ns = self.grounding.data_ns.clone();
                    inline_entity_body(&entity, &arguments, alias, Some(&data_ns), self)
                } else {
                    walk_domain(self, DomainExpression::Function(func))
                }
            }
            other => walk_domain(self, other),
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

/// Synthesize a `CaseExpression` from multiple guarded function clauses.
///
/// Each clause's guard becomes a `CaseArm::Searched` condition, and the clause
/// body becomes the result. An unguarded clause becomes `CaseArm::Default`.
/// Parameters are substituted with the call-site arguments before building arms.
fn build_case_from_clauses(
    clauses: Vec<DdlDefinition>,
    arguments: &[ast_unresolved::DomainExpression],
    data_ns: Option<&ast_unresolved::NamespacePath>,
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

        // Build param → argument substitution map
        let param_map: HashMap<&str, &ast_unresolved::DomainExpression> = params
            .iter()
            .map(|p| p.name.as_str())
            .zip(arguments.iter())
            .collect();

        let substituted_body = substitute_in_domain_expr(body.clone(), &param_map);
        let substituted_body = if let Some(ns) = data_ns {
            patch_data_ns_in_domain_expr(substituted_body, ns)
        } else {
            substituted_body
        };

        let has_guard = params.iter().any(|p| p.guard.is_some());
        if has_guard {
            // Find the guard expression and substitute params in it too
            let guard_expr = params
                .iter()
                .find_map(|p| p.guard.as_ref())
                .unwrap()
                .clone();
            let guard_substituted = substitute_in_domain_expr(guard_expr, &param_map);
            let guard_substituted = if let Some(ns) = data_ns {
                patch_data_ns_in_domain_expr(guard_substituted, ns)
            } else {
                guard_substituted
            };
            let guard_bool = domain_expr_to_boolean(guard_substituted)?;
            arms.push(ast_unresolved::CaseArm::Searched {
                condition: Box::new(guard_bool),
                result: Box::new(substituted_body),
            });
        } else {
            // No guard → default case (ELSE)
            arms.push(ast_unresolved::CaseArm::Default {
                result: Box::new(substituted_body),
            });
        }
    }

    Ok(ast_unresolved::DomainExpression::Function(
        ast_unresolved::FunctionExpression::CaseExpression { arms, alias: None },
    ))
}

// ============================================================================
// Parameter substitution — ParamSubstituter fold
// ============================================================================

/// Replaces `Lvar` nodes whose names appear in `param_map` with the
/// corresponding argument expression. All other nodes are structurally
/// descended by the default `walk_*` functions.
struct ParamSubstituter<'a> {
    param_map: &'a HashMap<&'a str, &'a ast_unresolved::DomainExpression>,
}

impl AstFold<Unresolved> for ParamSubstituter<'_> {
    fn fold_domain(
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
            other => walk_domain(self, other),
        }
    }
}

/// Substitute parameter Lvars in a domain expression with argument expressions.
pub(crate) fn substitute_in_domain_expr(
    expr: ast_unresolved::DomainExpression,
    param_map: &HashMap<&str, &ast_unresolved::DomainExpression>,
) -> ast_unresolved::DomainExpression {
    ParamSubstituter { param_map }
        .fold_domain(expr)
        .expect("substitution is infallible")
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
    let folded = inliner.fold_query(query)?;
    Ok((folded, inliner.collected_ccafe_cfes))
}

// ============================================================================
// Ground scalar expansion for HO views
// ============================================================================

use crate::pipeline::asts::ddl::HoParam;

/// Determine if a call-site argument is a ground literal (string or number)
/// vs a free variable (bare identifier).
fn is_ground_call_site_arg(arg: &str) -> bool {
    arg.starts_with('"') || arg.starts_with('\'') || arg.parse::<f64>().is_ok()
}

/// Information about a call-site ground scalar position.
pub(super) struct GroundScalarCallInfo {
    /// Index in the HO param list
    pub position: usize,
    /// The raw call-site value (e.g., "\"engineering\"" or "dept")
    pub call_value: String,
    /// Whether the call-site argument is ground (literal) or free (identifier)
    pub is_ground_call: bool,
}

/// Extract ground scalar call info from bindings.
pub(super) fn extract_ground_scalar_info(bindings: &HoParamBindings) -> Vec<GroundScalarCallInfo> {
    bindings
        .ground_scalar_call_args
        .iter()
        .map(|(pos, val)| GroundScalarCallInfo {
            position: *pos,
            call_value: val.clone(),
            is_ground_call: is_ground_call_site_arg(val),
        })
        .collect()
}

/// Reject unbound identifiers at MixedGroundParam positions.
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
) -> Result<String> {
    let body_text = extract_body_text(full_source)?;

    // Collect ground scalar injection info: (call_var_name, clause_literal_value)
    let mut ground_injections: Vec<(&str, &str)> = Vec::new();
    for info in ground_info {
        if info.is_ground_call {
            continue; // Ground call-site arg → filtering only, no injection
        }
        if let Some(clause_param) = clause_params.get(info.position) {
            if let crate::pipeline::asts::ddl::HoParamKind::GroundScalar(clause_val) = &clause_param.kind {
                ground_injections.push((&info.call_value, clause_val.as_str()));
            }
        }
    }

    let mut result_body = body_text.to_string();

    if let Some(items) = output_head {
        // When there's an output head, combine ground scalar injections INTO
        // the projection. This avoids naming collisions (embed + existing column).
        let mut proj_items: Vec<String> = Vec::new();

        // First: inject ground scalar constants as literal projections
        for (var_name, literal_val) in &ground_injections {
            proj_items.push(format!("{} as {}", literal_val, var_name));
        }

        // Then: add output head items
        for item in items {
            match item {
                ViewHeadItem::Free(name) => proj_items.push(name.clone()),
                ViewHeadItem::Ground(literal) => {
                    proj_items.push(format!("{} as _ground", literal));
                }
            }
        }

        result_body = format!("{} |> ({})", result_body, proj_items.join(", "));
    } else if !ground_injections.is_empty() {
        // No output head (glob) — use embed to add constant columns without
        // losing existing columns.
        let embeds: Vec<String> = ground_injections
            .iter()
            .map(|(var_name, literal_val)| format!("{} as {}", literal_val, var_name))
            .collect();
        result_body = format!("{} |> +({})", result_body, embeds.join(", "));
    }

    // Reconstruct full definition: name(*) :- desugared_body
    Ok(format!("{}(*) :- {}", view_name, result_body))
}

// ============================================================================
// Kind-aware HO parameter binding
// ============================================================================

use crate::resolution::registry::{HoParamInfo, HoParamKind};

/// Result of binding call-site arguments to HO view parameters using kind metadata.
pub(crate) use crate::pipeline::query_features::HoParamBindings;

/// Bind flat call-site arguments to HO view parameters using kind metadata.
///
/// For **Glob** and **Scalar** params that look like table names (not parseable as expression):
///   arg goes into `table_params`.
/// For **Scalar** params that parse as an expression: goes into `scalar_params`.
/// For **Argumentative** params: consumes N args (where N = column count), synthesizes
///   anonymous table, goes into `table_expr_params`.
pub(super) fn bind_ho_params(
    params: &[HoParamInfo],
    arguments: &[String],
) -> Result<HoParamBindings> {
    let mut bindings = HoParamBindings {
        table_params: HashMap::new(),
        table_expr_params: HashMap::new(),
        scalar_params: HashMap::new(),
        argumentative_table_refs: Vec::new(),
        ground_scalar_call_args: Vec::new(),
    };

    let mut arg_idx = 0;
    for (param_pos, param) in params.iter().enumerate() {
        match &param.kind {
            HoParamKind::Glob => {
                // Glob param: bind the next arg as a table name
                if arg_idx >= arguments.len() {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "Not enough arguments for HO view: expected table for glob param '{}'",
                            param.name
                        ),
                        "Missing HO view argument",
                    ));
                }
                bindings
                    .table_params
                    .insert(param.name.clone(), arguments[arg_idx].clone());
                arg_idx += 1;
            }
            HoParamKind::Argumentative(columns) => {
                let n_cols = columns.len();
                // Check if the next argument is a bare identifier (table reference).
                // If so, bind as a table with positional columns rather than scalar lift.
                if arg_idx < arguments.len() && looks_like_bare_identifier(&arguments[arg_idx]) {
                    let table_name = arguments[arg_idx].clone();
                    let table_rel = build_argumentative_table_ref(&table_name, columns);
                    bindings
                        .table_expr_params
                        .insert(param.name.clone(), table_rel);
                    bindings.argumentative_table_refs.push((
                        param.name.clone(),
                        table_name,
                        n_cols,
                        columns.clone(),
                    ));
                    arg_idx += 1;
                } else {
                    // Scalar lift: consume N args (one per column) and synthesize anonymous table
                    if arg_idx + n_cols > arguments.len() {
                        return Err(DelightQLError::validation_error(
                            format!(
                                "Not enough arguments for HO view: expected {} values for argumentative param '{}({})', got {}",
                                n_cols, param.name, columns.join(", "), arguments.len() - arg_idx
                            ),
                            "Missing HO view argument",
                        ));
                    }
                    let values: Vec<String> = arguments[arg_idx..arg_idx + n_cols].to_vec();
                    let anon_table = lift_scalars_to_anonymous_table(columns, &[values])?;
                    bindings
                        .table_expr_params
                        .insert(param.name.clone(), anon_table);
                    arg_idx += n_cols;
                }
            }
            HoParamKind::Scalar => {
                // Scalar param: try to parse as expression, else as table name
                if arg_idx >= arguments.len() {
                    return Err(DelightQLError::validation_error(
                        format!("Not enough arguments for HO view: expected value for scalar param '{}'", param.name),
                        "Missing HO view argument",
                    ));
                }
                let arg = &arguments[arg_idx];
                match crate::ddl::body_parser::parse_function_body(arg) {
                    Ok(domain_expr) => {
                        bindings
                            .scalar_params
                            .insert(param.name.clone(), domain_expr);
                    }
                    Err(_) => {}
                }
                // Also add to table_params for legacy compatibility
                bindings
                    .table_params
                    .insert(param.name.clone(), arg.clone());
                // Record call-site arg for per-clause matching (other clauses may
                // have GroundScalar at this position)
                bindings
                    .ground_scalar_call_args
                    .push((param_pos, arg.clone()));
                arg_idx += 1;
            }
            HoParamKind::GroundScalar(_) => {
                // Ground scalar: per-clause constant. Consume the call-site argument
                // (if provided) and record it for later use in clause filtering/injection.
                if arg_idx < arguments.len() {
                    bindings
                        .ground_scalar_call_args
                        .push((param_pos, arguments[arg_idx].clone()));
                    arg_idx += 1;
                }
            }
        }
    }

    Ok(bindings)
}

/// Bind &-separated argument groups to HO view parameters using kind metadata.
///
/// Each group maps to one parameter. Within an argumentative group,
/// `;`-separated rows become multiple rows in the synthesized anonymous table.
pub(super) fn bind_ho_params_from_groups(
    params: &[HoParamInfo],
    groups: &[crate::pipeline::asts::core::operators::HoCallGroup],
) -> Result<HoParamBindings> {
    let mut bindings = HoParamBindings {
        table_params: HashMap::new(),
        table_expr_params: HashMap::new(),
        scalar_params: HashMap::new(),
        argumentative_table_refs: Vec::new(),
        ground_scalar_call_args: Vec::new(),
    };

    // If no & separators were used, fall back to flat binding
    if groups.len() == 1 && params.len() > 1 {
        // Single group, multiple params → flat binding
        let flat_values = groups[0].flat_values();
        let flat_strings: Vec<String> = flat_values.into_iter().map(String::from).collect();
        return bind_ho_params(params, &flat_strings);
    }

    for (param_pos, (param, group)) in params.iter().zip(groups.iter()).enumerate() {
        match &param.kind {
            HoParamKind::Glob => {
                // Glob: single value that's a table name
                if let Some(val) = group.as_single_value() {
                    bindings
                        .table_params
                        .insert(param.name.clone(), val.to_string());
                } else {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "Glob param '{}' requires a single table name, got {:?}",
                            param.name, group.rows
                        ),
                        "Invalid glob argument",
                    ));
                }
            }
            HoParamKind::Argumentative(columns) => {
                // Check if the group is a single bare identifier (table reference)
                if let Some(val) = group.as_single_value() {
                    if looks_like_bare_identifier(val) {
                        let table_rel = build_argumentative_table_ref(val, columns);
                        bindings
                            .table_expr_params
                            .insert(param.name.clone(), table_rel);
                        bindings.argumentative_table_refs.push((
                            param.name.clone(),
                            val.to_string(),
                            columns.len(),
                            columns.clone(),
                        ));
                        continue;
                    }
                }
                // Scalar lift: rows of values → synthesize anonymous table
                let anon_table = lift_scalars_to_anonymous_table(columns, &group.rows)?;
                bindings
                    .table_expr_params
                    .insert(param.name.clone(), anon_table);
            }
            HoParamKind::Scalar => {
                // Scalar: single value → parse as expression or table name
                if let Some(val) = group.as_single_value() {
                    match crate::ddl::body_parser::parse_function_body(val) {
                        Ok(domain_expr) => {
                            bindings
                                .scalar_params
                                .insert(param.name.clone(), domain_expr);
                        }
                        Err(_) => {}
                    }
                    bindings
                        .table_params
                        .insert(param.name.clone(), val.to_string());
                    // Record call-site arg for per-clause matching (other clauses may
                    // have GroundScalar at this position)
                    bindings
                        .ground_scalar_call_args
                        .push((param_pos, val.to_string()));
                } else {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "Scalar param '{}' requires a single value, got {:?}",
                            param.name, group.rows
                        ),
                        "Invalid scalar argument",
                    ));
                }
            }
            HoParamKind::GroundScalar(_) => {
                // Ground scalar: per-clause constant. Record call-site value for filtering/injection.
                if let Some(val) = group.as_single_value() {
                    bindings
                        .ground_scalar_call_args
                        .push((param_pos, val.to_string()));
                }
            }
        }
    }

    Ok(bindings)
}

/// Check if a string looks like a bare identifier (e.g. a table name) rather than
/// a literal value suitable for an argumentative HO parameter.
///
/// Bare identifiers match `[a-zA-Z_][a-zA-Z0-9_]*` and are NOT:
/// - Boolean literals (`true`, `false`)
/// - Null literal (`null`)
/// - Quoted strings (starting with `"` or `'`)
/// - Numbers (parseable as f64)
fn looks_like_bare_identifier(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Quoted strings are literal values, not identifiers
    if trimmed.starts_with('"') || trimmed.starts_with('\'') {
        return false;
    }
    // Numbers are literal values
    if trimmed.parse::<f64>().is_ok() {
        return false;
    }
    // Check identifier pattern
    let mut chars = trimmed.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return false,
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return false;
    }
    // Exclude known literals
    !matches!(trimmed, "true" | "false" | "null")
}

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
        DdlHead::Companion { .. } => panic!("Companion definition passed to create_proffer_bindings — only HoView heads are valid here"),
        other => panic!("catch-all hit in grounding.rs extract_ho_bindings (FunctionExpression): {:?}", other),
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

/// Build a `Relation::Ground` with positional columns for an argumentative param
/// that received a table reference (bare identifier) instead of scalar values.
///
/// Produces e.g. `refs(k, l)` — the positional domain spec maps the param's column
/// names to the table's columns by position. Arity is validated separately.
fn build_argumentative_table_ref(
    table_name: &str,
    columns: &[String],
) -> ast_unresolved::RelationalExpression {
    let col_exprs: Vec<ast_unresolved::DomainExpression> = columns
        .iter()
        .map(|c| ast_unresolved::DomainExpression::lvar_builder(c.clone()).build())
        .collect();
    ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::Ground {
        identifier: ast_unresolved::QualifiedName {
            namespace_path: ast_unresolved::NamespacePath::empty(),
            name: table_name.to_string().into(),
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
    })
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

impl AstFold<Unresolved> for DataNsPatcher<'_> {
    fn fold_relation(&mut self, r: Relation<Unresolved>) -> Result<Relation<Unresolved>> {
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
                    domain_spec,
                    alias,
                    namespace,
                    grounding,
                    cpr_schema,
                })
            }
            // InnerRelation: delegate to fold_inner_relation for identifier patching
            other => walk_relation(self, other),
        }
    }

    fn fold_inner_relation(
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
                    subquery: Box::new(self.fold_relational(*subquery)?),
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
                    subquery: Box::new(self.fold_relational(*subquery)?),
                    is_consulted_view,
                })
            }
            other => walk_inner_relation(self, other),
        }
    }

    fn fold_domain(
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
                let patched_subquery = self.fold_relational(*subquery)?;
                Ok(DomainExpression::ScalarSubquery {
                    identifier,
                    subquery: Box::new(patched_subquery),
                    alias,
                })
            }
            other => walk_domain(self, other),
        }
    }
}

/// Patch data namespace on all table references in a Query.
pub(super) fn patch_data_ns_query(
    query: ast_unresolved::Query,
    data_ns: &ast_unresolved::NamespacePath,
) -> ast_unresolved::Query {
    DataNsPatcher { data_ns }
        .fold_query(query)
        .expect("namespace patching is infallible")
}

/// Patch data_ns on ScalarSubquery identifiers within a domain expression.
fn patch_data_ns_in_domain_expr(
    expr: ast_unresolved::DomainExpression,
    data_ns: &ast_unresolved::NamespacePath,
) -> ast_unresolved::DomainExpression {
    DataNsPatcher { data_ns }
        .fold_domain(expr)
        .expect("namespace patching is infallible")
}
