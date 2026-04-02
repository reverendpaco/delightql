// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::*;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::{
    walk_transform_boolean, walk_transform_domain, walk_transform_function, AstTransform,
    FoldAction,
};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::Unresolved;
use crate::system::DelightQLSystem;
use std::collections::HashMap;

// =============================================================================
// BubbleCollector — AstTransform-based dependency collector
// =============================================================================

/// Walks the AST collecting column dependencies (`ColumnReference`) into `deps`.
///
/// Implements `AstTransform<Unresolved, Unresolved>` — same-phase identity
/// transform that intercepts leaf nodes (`Lvar`, `ColumnOrdinal`) to record
/// column references, and intercepts subquery-bearing nodes (`InnerExists`,
/// `ScalarSubquery`, `InRelational`) to call `resolve_inner_cpr_during_bubbling`.
struct BubbleCollector<'a> {
    deps: Vec<ColumnReference>,
    schema: &'a dyn DatabaseSchema,
    system: Option<&'a DelightQLSystem>,
    cte_context: &'a mut HashMap<String, ast_resolved::CprSchema>,
    outer_context: Option<&'a [ast_resolved::ColumnMetadata]>,
}

impl BubbleCollector<'_> {
    /// Shared logic for InnerExists / ScalarSubquery / InRelational: attempt
    /// `resolve_inner_cpr_during_bubbling` if the entity is known, otherwise
    /// skip (consulted-view path handles it later).
    fn try_resolve_inner_cpr(
        &mut self,
        table_name: &str,
        subquery: ast_unresolved::RelationalExpression,
    ) {
        let known =
            self.schema.table_exists(None, table_name) || self.cte_context.contains_key(table_name);
        if known {
            if let Ok(bubble_result) = super::helpers::resolve_inner_cpr_during_bubbling(
                subquery,
                self.schema,
                self.system,
                self.cte_context,
                self.outer_context,
            ) {
                *self.cte_context = bubble_result.updated_cte_context;
                self.deps.extend(bubble_result.dependencies);
            }
            // On error: fall through with no deps (matches original behavior)
        }
        // Entity not in DB/CTEs: skip (consulted view resolution handles it)
    }
}

impl AstTransform<Unresolved, Unresolved> for BubbleCollector<'_> {
    fn transform_domain(
        &mut self,
        expr: ast_unresolved::DomainExpression,
    ) -> Result<ast_unresolved::DomainExpression> {
        match expr {
            // Lvar: push Named column reference
            ast_unresolved::DomainExpression::Lvar {
                ref name,
                ref qualifier,
                ref namespace_path,
                ..
            } => {
                self.deps.push(ColumnReference::Named {
                    name: name.to_string(),
                    qualifier: qualifier.as_deref().map(String::from),
                    schema: namespace_path.first().map(|s| s.to_string()),
                });
                Ok(expr)
            }

            // ColumnOrdinal: push Ordinal column reference
            ast_unresolved::DomainExpression::ColumnOrdinal(ref ordinal_box) => {
                let ordinal = ordinal_box.get();
                self.deps.push(ColumnReference::Ordinal {
                    position: ordinal.position,
                    reverse: ordinal.reverse,
                    qualifier: ordinal.qualifier.clone(),
                    alias: ordinal.alias.clone(),
                });
                Ok(expr)
            }

            // ScalarSubquery: resolve inner CPR, don't walk into subquery
            ast_unresolved::DomainExpression::ScalarSubquery {
                ref identifier,
                ref subquery,
                ..
            } => {
                let table_name = identifier.name.clone();
                let sq = (**subquery).clone();
                self.try_resolve_inner_cpr(&table_name, sq);
                Ok(expr)
            }

            // Substitution: error on ContextParameter, otherwise pass through
            ast_unresolved::DomainExpression::Substitution(ref sub) => {
                use crate::pipeline::asts::core::SubstitutionExpr;
                if let SubstitutionExpr::ContextParameter { .. } = sub {
                    return Err(DelightQLError::ParseError {
                        message: "ContextParameter should not appear in unresolved phase"
                            .to_string(),
                        source: None,
                        subcategory: None,
                    });
                }
                Ok(expr)
            }

            // Everything else: delegate to the walk (which recurses into children)
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: ast_unresolved::BooleanExpression,
    ) -> Result<ast_unresolved::BooleanExpression> {
        match expr {
            // InnerExists: resolve inner CPR, don't walk into subquery
            ast_unresolved::BooleanExpression::InnerExists {
                ref identifier,
                ref subquery,
                ..
            } => {
                let table_name = identifier.name.clone();
                let sq = (**subquery).clone();
                self.try_resolve_inner_cpr(&table_name, sq);
                Ok(expr)
            }

            // InRelational: bubble the value expression, then resolve inner CPR
            ast_unresolved::BooleanExpression::InRelational {
                ref identifier,
                ref subquery,
                ref value,
                ..
            } => {
                // Walk the value expression to collect its deps
                let v = (**value).clone();
                let _ = self.transform_domain(v)?;

                let table_name = identifier.name.clone();
                let sq = (**subquery).clone();
                self.try_resolve_inner_cpr(&table_name, sq);

                Ok(expr)
            }

            // Sigma: no deps to collect (matches original behavior)
            ast_unresolved::BooleanExpression::Sigma { .. } => Ok(expr),

            // Everything else: delegate to the walk
            other => walk_transform_boolean(self, other),
        }
    }

    fn transform_function(
        &mut self,
        func: ast_unresolved::FunctionExpression,
    ) -> Result<ast_unresolved::FunctionExpression> {
        match func {
            // CaseExpression: TODO - skip bubbling (matches original behavior)
            ast_unresolved::FunctionExpression::CaseExpression { .. } => Ok(func),

            // Curly, Array, MetadataTreeGroup: no deps to collect
            ast_unresolved::FunctionExpression::Curly { .. }
            | ast_unresolved::FunctionExpression::Array { .. }
            | ast_unresolved::FunctionExpression::MetadataTreeGroup { .. } => Ok(func),

            // Everything else: delegate to the walk
            other => walk_transform_function(self, other),
        }
    }

    // Relational expressions inside subqueries should not be walked —
    // bubbling handles them via resolve_inner_cpr_during_bubbling.
    fn transform_relational_action(
        &mut self,
        e: ast_unresolved::RelationalExpression,
    ) -> Result<FoldAction<ast_unresolved::RelationalExpression>> {
        Ok(FoldAction::Replaced(e))
    }
}

pub(super) fn bubble_unary_operator(
    operator: ast_unresolved::UnaryRelationalOperator,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    match operator {
        ast_unresolved::UnaryRelationalOperator::General {
            containment_semantic,
            expressions,
        } => bubble_general_operator(
            containment_semantic,
            expressions,
            schema,
            system,
            cte_context,
        ),
        ast_unresolved::UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec,
        } => bubble_modulo_operator(containment_semantic, spec, schema, system, cte_context),
        ast_unresolved::UnaryRelationalOperator::TupleOrdering {
            containment_semantic,
            specs,
        } => {
            bubble_tupleordering_operator(containment_semantic, specs, schema, system, cte_context)
        }
        ast_unresolved::UnaryRelationalOperator::MapCover {
            function,
            columns,
            containment_semantic,
            conditioned_on,
        } => bubble_mapcover_operator(
            function,
            columns,
            containment_semantic,
            conditioned_on,
            schema,
            system,
            cte_context,
        ),
        ast_unresolved::UnaryRelationalOperator::ProjectOut {
            containment_semantic,
            expressions,
        } => bubble_projectout_operator(
            containment_semantic,
            expressions,
            schema,
            system,
            cte_context,
        ),
        ast_unresolved::UnaryRelationalOperator::RenameCover { specs } => {
            bubble_renamecover_operator(specs, schema, system, cte_context)
        }
        ast_unresolved::UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => {
            bubble_transform_operator(transformations, conditioned_on, schema, system, cte_context)
        }
        ast_unresolved::UnaryRelationalOperator::AggregatePipe { aggregations } => {
            bubble_aggregatepipe_operator(aggregations, schema, system, cte_context)
        }
        ast_unresolved::UnaryRelationalOperator::Reposition { moves } => {
            bubble_reposition_operator(moves, schema, system, cte_context)
        }
        ast_unresolved::UnaryRelationalOperator::EmbedMapCover {
            function,
            selector,
            alias_template,
            containment_semantic,
        } => {
            // Bubble function and selector components
            let (bubbled_function, func_state) =
                bubble_function_expression(function, schema, system, cte_context)?;

            // Bubble the selector if it contains expressions
            let (bubbled_selector, selector_state) =
                bubble_column_selector(selector, schema, system, cte_context)?;

            // Combine states
            let mut combined_state = func_state;
            combined_state.i_need.extend(selector_state.i_need);

            Ok((
                ast_unresolved::UnaryRelationalOperator::EmbedMapCover {
                    function: bubbled_function,
                    selector: bubbled_selector,
                    alias_template,
                    containment_semantic,
                },
                combined_state,
            ))
        }
        // MetaIze has no expressions to bubble - schema synthesis happens at resolution time
        ast_unresolved::UnaryRelationalOperator::MetaIze { detailed } => Ok((
            ast_unresolved::UnaryRelationalOperator::MetaIze { detailed },
            BubbledState::empty(),
        )),
        // Witness has no expressions to bubble - existence check happens at SQL level
        ast_unresolved::UnaryRelationalOperator::Witness { exists } => Ok((
            ast_unresolved::UnaryRelationalOperator::Witness { exists },
            BubbledState::empty(),
        )),
        // Qualify has no expressions to bubble - it just marks columns as qualified
        ast_unresolved::UnaryRelationalOperator::Qualify => Ok((
            ast_unresolved::UnaryRelationalOperator::Qualify,
            BubbledState::empty(),
        )),
        // Using has no expressions to bubble - columns are simple strings
        ast_unresolved::UnaryRelationalOperator::Using { columns } => Ok((
            ast_unresolved::UnaryRelationalOperator::Using { columns },
            BubbledState::empty(),
        )),
        // UsingAll has no expressions to bubble - validated at join time
        ast_unresolved::UnaryRelationalOperator::UsingAll => Ok((
            ast_unresolved::UnaryRelationalOperator::UsingAll,
            BubbledState::empty(),
        )),
        // DmlTerminal has no expressions to bubble - target is a string literal
        ast_unresolved::UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace,
            domain_spec,
        } => Ok((
            ast_unresolved::UnaryRelationalOperator::DmlTerminal {
                kind,
                target,
                target_namespace,
                domain_spec,
            },
            BubbledState::empty(),
        )),
        // InteriorDrillDown has no expressions to bubble - column/columns are simple strings
        ast_unresolved::UnaryRelationalOperator::InteriorDrillDown {
            column,
            glob,
            columns,
            interior_schema,
            groundings,
        } => Ok((
            ast_unresolved::UnaryRelationalOperator::InteriorDrillDown {
                column,
                glob,
                columns,
                interior_schema,
                groundings,
            },
            BubbledState::empty(),
        )),
        // NarrowingDestructure has no expressions to bubble - column/fields are simple strings
        ast_unresolved::UnaryRelationalOperator::NarrowingDestructure { column, fields } => Ok((
            ast_unresolved::UnaryRelationalOperator::NarrowingDestructure { column, fields },
            BubbledState::empty(),
        )),
        // Exhaustive-match tax: Unresolved-only variants, consumed before resolution.
        ast_unresolved::UnaryRelationalOperator::HoViewApplication { .. }
        | ast_unresolved::UnaryRelationalOperator::DirectiveTerminal { .. } => unreachable!(),
    }
}

/// Bubble a list of domain expressions and collect their dependencies.
fn bubble_expressions_collect_deps(
    exprs: &[ast_unresolved::DomainExpression],
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<Vec<ColumnReference>> {
    let mut deps = Vec::new();
    for expr in exprs {
        let (_, bubbled) =
            bubble_domain_expression(expr.clone(), schema, system, cte_context, None)?;
        deps.extend(bubbled.i_need);
    }
    Ok(deps)
}

pub(super) fn bubble_general_operator(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    expressions: Vec<ast_unresolved::DomainExpression>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let deps = bubble_expressions_collect_deps(&expressions, schema, system, cte_context)?;
    let operator = ast_unresolved::UnaryRelationalOperator::General {
        containment_semantic,
        expressions,
    };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_modulo_operator(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    spec: ast_unresolved::ModuloSpec,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let deps = match &spec {
        ast_unresolved::ModuloSpec::Columns(cols) => {
            bubble_expressions_collect_deps(cols, schema, system, cte_context)?
        }
        ast_unresolved::ModuloSpec::GroupBy {
            reducing_by,
            reducing_on,
            arbitrary,
        } => {
            let mut deps =
                bubble_expressions_collect_deps(reducing_by, schema, system, cte_context)?;
            deps.extend(bubble_expressions_collect_deps(
                reducing_on,
                schema,
                system,
                cte_context,
            )?);
            deps.extend(bubble_expressions_collect_deps(
                arbitrary,
                schema,
                system,
                cte_context,
            )?);
            deps
        }
    };

    let operator = ast_unresolved::UnaryRelationalOperator::Modulo {
        containment_semantic,
        spec,
    };

    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_tupleordering_operator(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    specs: Vec<ast_unresolved::OrderingSpec>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let columns: Vec<_> = specs.iter().map(|s| s.column.clone()).collect();
    let deps = bubble_expressions_collect_deps(&columns, schema, system, cte_context)?;
    let operator = ast_unresolved::UnaryRelationalOperator::TupleOrdering {
        containment_semantic,
        specs,
    };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_mapcover_operator(
    function: ast_unresolved::FunctionExpression,
    columns: Vec<ast_unresolved::DomainExpression>,
    containment_semantic: ast_unresolved::ContainmentSemantic,
    conditioned_on: Option<Box<ast_unresolved::BooleanExpression>>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let (_, func_bubbled) =
        bubble_function_expression(function.clone(), schema, system, cte_context)?;
    let mut deps = func_bubbled.i_need;
    deps.extend(bubble_expressions_collect_deps(
        &columns,
        schema,
        system,
        cte_context,
    )?);

    let operator = ast_unresolved::UnaryRelationalOperator::MapCover {
        function,
        columns,
        containment_semantic,
        conditioned_on,
    };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_projectout_operator(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    expressions: Vec<ast_unresolved::DomainExpression>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let deps = bubble_expressions_collect_deps(&expressions, schema, system, cte_context)?;
    let operator = ast_unresolved::UnaryRelationalOperator::ProjectOut {
        containment_semantic,
        expressions,
    };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_renamecover_operator(
    specs: Vec<ast_unresolved::RenameSpec>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let from_exprs: Vec<_> = specs.iter().map(|s| s.from.clone()).collect();
    let deps = bubble_expressions_collect_deps(&from_exprs, schema, system, cte_context)?;
    let operator = ast_unresolved::UnaryRelationalOperator::RenameCover { specs };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_transform_operator(
    transformations: Vec<(ast_unresolved::DomainExpression, String, Option<String>)>,
    conditioned_on: Option<Box<ast_unresolved::BooleanExpression>>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let exprs: Vec<_> = transformations.iter().map(|(e, _, _)| e.clone()).collect();
    let deps = bubble_expressions_collect_deps(&exprs, schema, system, cte_context)?;
    let operator = ast_unresolved::UnaryRelationalOperator::Transform {
        transformations,
        conditioned_on,
    };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_aggregatepipe_operator(
    aggregations: Vec<ast_unresolved::DomainExpression>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let deps = bubble_expressions_collect_deps(&aggregations, schema, system, cte_context)?;
    let operator = ast_unresolved::UnaryRelationalOperator::AggregatePipe { aggregations };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_reposition_operator(
    moves: Vec<ast_unresolved::RepositionSpec>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let columns: Vec<_> = moves.iter().map(|s| s.column.clone()).collect();
    let deps = bubble_expressions_collect_deps(&columns, schema, system, cte_context)?;
    let operator = ast_unresolved::UnaryRelationalOperator::Reposition { moves };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_domain_expression(
    expr: ast_unresolved::DomainExpression,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
) -> Result<(ast_unresolved::DomainExpression, BubbledState)> {
    let mut collector = BubbleCollector {
        deps: vec![],
        schema,
        system,
        cte_context,
        outer_context,
    };
    let result = collector.transform_domain(expr)?;
    Ok((
        result,
        BubbledState::with_unresolved(Vec::new(), collector.deps),
    ))
}

pub(super) fn bubble_function_expression(
    func: ast_unresolved::FunctionExpression,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::FunctionExpression, BubbledState)> {
    let mut collector = BubbleCollector {
        deps: vec![],
        schema,
        system,
        cte_context,
        outer_context: None,
    };
    let result = collector.transform_function(func)?;
    Ok((
        result,
        BubbledState::with_unresolved(Vec::new(), collector.deps),
    ))
}

pub(super) fn bubble_predicate_expression(
    pred: ast_unresolved::BooleanExpression,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
) -> Result<(ast_unresolved::BooleanExpression, BubbledState)> {
    let mut collector = BubbleCollector {
        deps: vec![],
        schema,
        system,
        cte_context,
        outer_context,
    };
    let result = collector.transform_boolean(pred)?;
    Ok((
        result,
        BubbledState::with_unresolved(Vec::new(), collector.deps),
    ))
}

/// Helper to bubble column selector
fn bubble_column_selector(
    selector: ast_unresolved::ColumnSelector,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::ColumnSelector, BubbledState)> {
    match selector {
        ast_unresolved::ColumnSelector::Explicit(exprs) => {
            let mut bubbled_exprs = Vec::new();
            let mut combined_state = BubbledState::empty();

            for expr in exprs {
                let (bubbled_expr, expr_state) =
                    bubble_domain_expression(expr, schema, system, cte_context, None)?;
                bubbled_exprs.push(bubbled_expr);
                combined_state.i_need.extend(expr_state.i_need);
            }

            Ok((
                ast_unresolved::ColumnSelector::Explicit(bubbled_exprs),
                combined_state,
            ))
        }
        other => Ok((other, BubbledState::empty())),
    }
}
