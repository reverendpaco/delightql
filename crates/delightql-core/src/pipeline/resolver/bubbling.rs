// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::*;
use crate::pipeline::ast_transform::{
    walk_transform_boolean, walk_transform_domain, walk_transform_function, AstTransform,
    FoldAction,
};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::operators::{EmbedMapCover, MapCover};
use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::Unresolved;
use crate::pipeline::asts::core::{Existence, RelationalMembership, SigmaApplication};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::system::DelightQLSystem;
use std::collections::HashMap;
use std::rc::Rc;

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
    cte_context: &'a mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    outer_context: Option<&'a [crate::names::ColId]>,
    identities: &'a Rc<crate::names::Registry>,
}

impl BubbleCollector<'_> {
    /// Shared logic for InnerExists / ScalarSubquery / InRelational: attempt
    /// `resolve_inner_cpr_during_bubbling` if the entity is known, otherwise
    /// skip (consulted-view path handles it later).
    fn try_resolve_inner_cpr(
        &mut self,
        table_name: &delightql_types::SqlIdentifier,
        subquery: ast_unresolved::Chain,
    ) -> Result<()> {
        let known = self.schema.table_exists(None, table_name.as_str())?
            || self.cte_context.contains_key(table_name);
        if known {
            // The inner resolver may reject a subquery for an ordinary
            // semantic reason while bubbling is still allowed to continue;
            // that was deliberately non-fatal.  The schema existence probe
            // above is the fallible boundary: a provider failure reaches the
            // caller, while a semantic miss here remains a bubble miss.
            if let Ok(bubble_result) = super::helpers::resolve_inner_cpr_during_bubbling(
                subquery,
                self.schema,
                self.system,
                self.cte_context,
                self.outer_context,
                self.identities,
            ) {
                *self.cte_context = bubble_result.updated_cte_context;
                self.deps.extend(bubble_result.dependencies);
            }
        }
        // Entity not in DB/CTEs: skip (consulted view resolution handles it)
        Ok(())
    }
}

impl AstTransform<Unresolved, Unresolved> for BubbleCollector<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

    // Stack-safe: one descent per nesting level (S11).
    #[stacksafe::stacksafe]
    fn transform_domain(
        &mut self,
        expr: ast_unresolved::DomainExpression,
    ) -> Result<ast_unresolved::DomainExpression> {
        match expr {
            // Lvar: push Named column reference
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    ref name,
                    ref qualifier,
                    ..
                },
            ))) => {
                self.deps.push(ColumnReference::Named {
                    name: name.clone(),
                    qualifier: qualifier.clone(),
                });
                Ok(expr)
            }

            // ColumnOrdinal: push Ordinal column reference
            ast_unresolved::DomainExpression::Reference(Reference::Ordinal(ref ordinal)) => {
                self.deps.push(ColumnReference::Ordinal {
                    position: ordinal.position,
                    reverse: ordinal.reverse,
                    qualifier: ordinal.qualifier.clone(),
                });
                Ok(expr)
            }

            // A SCALARIZED RELATION: resolve the inner CPR, and do not walk
            // into the body — the compression closes it, and the value's own
            // resolution owns what is under it.
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Scalarized(
                    crate::pipeline::asts::core::ScalarRelation::Named {
                        ref identifier,
                        ref body,
                    },
                ),
            ) => {
                let table_name = identifier.name.clone();
                let attached = (**body).clone().attached();
                self.try_resolve_inner_cpr(&table_name, attached)?;
                Ok(expr)
            }

            // An open body's leaf provides nothing yet: the position that
            // applies the body supplies it later.
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Open(_),
            ) => Ok(expr),

            // Everything else: delegate to the walk (which recurses into children)
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: ast_unresolved::TruthExpression,
    ) -> Result<ast_unresolved::TruthExpression> {
        match expr {
            // InnerExists: resolve inner CPR, don't walk into subquery
            ast_unresolved::TruthExpression::Existence(Existence {
                ref addressing,
                relation: ref subquery,
                ..
            }) => {
                let table_name = addressing.identifier.name.clone();
                let sq = (**subquery).clone();
                self.try_resolve_inner_cpr(&table_name, sq)?;
                Ok(expr)
            }

            // InRelational: bubble the value expression, then resolve inner CPR
            ast_unresolved::TruthExpression::RelationalMembership(RelationalMembership {
                ref addressing,
                relation: ref subquery,
                ref probe,
                ..
            }) => {
                // Walk the probe's values to collect their deps
                for value in probe.values() {
                    let _ = self.transform_domain(value.clone())?;
                }

                let table_name = addressing.identifier.name.clone();
                let sq = (**subquery).clone();
                self.try_resolve_inner_cpr(&table_name, sq)?;

                Ok(expr)
            }

            // Sigma: no deps to collect (matches original behavior)
            ast_unresolved::TruthExpression::Sigma(SigmaApplication { .. }) => Ok(expr),

            // Everything else: delegate to the walk
            other => walk_transform_boolean(self, other),
        }
    }

    fn transform_function(
        &mut self,
        func: ast_unresolved::FunctionApplication,
    ) -> Result<ast_unresolved::FunctionApplication> {
        match func {
            // A case: skip bubbling (matches original behavior)
            ast_unresolved::FunctionApplication::Case(_) => Ok(func),

            // A construction: no deps to collect
            ast_unresolved::FunctionApplication::Enclyph(_) => Ok(func),

            // cast:(x, integer): arg[1] is a TYPE ATOM, not a column — walk
            // only arg[0] so `integer` is never recorded as a dependency.
            // (The resolver's transform_function validates the atom.)
            ast_unresolved::FunctionApplication::Standard(mut application)
                if Some(&application.call().callee).is_some_and(|reference| {
                    reference.namespace_texts().is_empty() && reference.name_text() == "cast"
                }) =>
            {
                // Walk only arg[0]; later args are never columns. Wrong
                // arity still reaches the resolver's teaching error.
                if let crate::pipeline::asts::core::operators::CallArguments::Scalar(members) =
                    &mut application.call_mut().arguments
                {
                    if let Some(member) = members.first_mut() {
                        if let Some(domain) = member.scalar_domain_mut() {
                            *domain = self.transform_domain(domain.clone())?;
                        }
                    }
                }
                Ok(ast_unresolved::FunctionApplication::Standard(application))
            }

            // Everything else: delegate to the walk
            other => walk_transform_function(self, other),
        }
    }

    // Relational expressions inside subqueries should not be walked —
    // bubbling handles them via resolve_inner_cpr_during_bubbling.
    fn transform_relational_action(
        &mut self,
        e: ast_unresolved::Chain,
    ) -> Result<FoldAction<ast_unresolved::Chain>> {
        Ok(FoldAction::Replaced(e))
    }
}

pub(super) fn bubble_unary_operator(
    operator: ast_unresolved::PipeOp,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(ast_unresolved::PipeOp, BubbledState)> {
    match operator {
        ast_unresolved::PipeOp::Project(items) => {
            bubble_general_operator(false, items, schema, system, cte_context, identities)
        }
        ast_unresolved::PipeOp::Embed(items) => {
            bubble_general_operator(true, items, schema, system, cte_context, identities)
        }
        ast_unresolved::PipeOp::Group(spec) => {
            bubble_group_operator(spec, schema, system, cte_context, identities)
        }
        ast_unresolved::PipeOp::MapCover(MapCover {
            callable,
            selector,
            guard,
            cells,
        }) => bubble_mapcover_operator(
            callable,
            selector,
            guard,
            cells,
            schema,
            system,
            cte_context,
            identities,
        ),
        ast_unresolved::PipeOp::ProjectOut(selector) => bubble_projectout_operator(selector),
        ast_unresolved::PipeOp::Rename(specs) => bubble_renamecover_operator(specs),
        ast_unresolved::PipeOp::Transform {
            items: transformations,
            guard: conditioned_on,
        } => bubble_transform_operator(
            transformations,
            conditioned_on,
            schema,
            system,
            cte_context,
            identities,
        ),
        ast_unresolved::PipeOp::EmbedMapCover(EmbedMapCover {
            callable,
            naming,
            selector,
            cells,
        }) => {
            // Bubble function and selector components
            let (bubbled_function, func_state) =
                bubble_callable(callable, schema, system, cte_context, identities)?;

            // The selector addresses columns of the operand; it names no
            // relation, so there is nothing under it to bubble.
            let combined_state = func_state;

            Ok((
                ast_unresolved::PipeOp::EmbedMapCover(EmbedMapCover {
                    callable: bubbled_function,
                    naming,
                    selector,
                    cells,
                }),
                combined_state,
            ))
        }
    }
}

/// Bubble a list of domain expressions and collect their dependencies.
/// A publication item's dependencies are its value's. A spread names columns
/// it does not evaluate, so it depends on nothing a bubble could carry.
fn bubble_items_collect_deps(
    items: &[ast_unresolved::OutItem],
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<Vec<ColumnReference>> {
    let exprs: Vec<_> = items
        .iter()
        .filter_map(ast_unresolved::OutItem::domain_value)
        .cloned()
        .collect();
    bubble_expressions_collect_deps(&exprs, schema, system, cte_context, identities)
}

fn bubble_expressions_collect_deps(
    exprs: &[ast_unresolved::DomainExpression],
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<Vec<ColumnReference>> {
    let mut deps = Vec::new();
    for expr in exprs {
        let (_, bubbled) =
            bubble_domain_expression(expr.clone(), schema, system, cte_context, None, identities)?;
        deps.extend(bubbled.i_need);
    }
    Ok(deps)
}

pub(super) fn bubble_general_operator(
    embed: bool,
    items: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::OutItem>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(ast_unresolved::PipeOp, BubbledState)> {
    let cloned: Vec<_> = items.iter().cloned().collect();
    let deps = bubble_items_collect_deps(&cloned, schema, system, cte_context, identities)?;
    let operator = if embed {
        ast_unresolved::PipeOp::Embed(items)
    } else {
        ast_unresolved::PipeOp::Project(items)
    };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_group_operator(
    spec: ast_unresolved::GroupSpec,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(ast_unresolved::PipeOp, BubbledState)> {
    let deps = match &spec {
        ast_unresolved::GroupSpec::Distinct { keys } => {
            let keys: Vec<_> = keys.iter().cloned().collect();
            bubble_items_collect_deps(&keys, schema, system, cte_context, identities)?
        }
        ast_unresolved::GroupSpec::Reduce {
            keys,
            reductions,
            plan: _,
        } => {
            let mut deps =
                bubble_items_collect_deps(keys, schema, system, cte_context, identities)?;
            // A metadata level's key is a reference into the enclosing
            // relation and its target holds values; both are reached through
            // the reduction item, which knows which it is holding.
            let values: Vec<_> = reductions
                .iter()
                .filter_map(|item| item.domain_value())
                .cloned()
                .collect();
            deps.extend(bubble_expressions_collect_deps(
                &values,
                schema,
                system,
                cte_context,
                identities,
            )?);
            for item in reductions.iter() {
                let ast_unresolved::ReductionItem::Delegate(w) = item else {
                    continue;
                };
                deps.extend(bubble_items_collect_deps(
                    &w.payload,
                    schema,
                    system,
                    cte_context,
                    identities,
                )?);
                let order_cols: Vec<_> = w.order.iter().map(|o| o.column.clone()).collect();
                deps.extend(bubble_expressions_collect_deps(
                    &order_cols,
                    schema,
                    system,
                    cte_context,
                    identities,
                )?);
            }
            deps
        }
    };

    let operator = ast_unresolved::PipeOp::Group(spec);

    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

/// The column needs an ordering's specs carry. An ordering is chain
/// structure, so the bubbling serves the run walk directly rather than an
/// operator dispatch.
pub(in crate::pipeline::resolver) fn bubble_ordering_specs(
    specs: &[ast_unresolved::OrderingSpec],
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<BubbledState> {
    let columns: Vec<_> = specs.iter().map(|s| s.column.clone()).collect();
    let deps = bubble_expressions_collect_deps(&columns, schema, system, cte_context, identities)?;
    Ok(BubbledState::with_unresolved(Vec::new(), deps))
}

pub(super) fn bubble_mapcover_operator(
    callable: ast_unresolved::Callable,
    selector: Vec<ast_unresolved::SelectorItem>,
    guard: Option<Box<ast_unresolved::TruthExpression>>,
    cells: Vec<crate::pipeline::asts::core::operators::AppliedCell<Unresolved>>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(ast_unresolved::PipeOp, BubbledState)> {
    let (_, func_bubbled) =
        bubble_callable(callable.clone(), schema, system, cte_context, identities)?;
    let deps = func_bubbled.i_need;

    let operator = ast_unresolved::PipeOp::MapCover(MapCover {
        callable,
        selector,
        guard,
        cells,
    });
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

/// A SELECTOR ADDRESSES COLUMNS OF THE OPERAND. It names no relation, so
/// there is no binding for it to need and nothing to bubble.
pub(super) fn bubble_projectout_operator(
    selector: Vec<ast_unresolved::SelectorItem>,
) -> Result<(ast_unresolved::PipeOp, BubbledState)> {
    let operator = ast_unresolved::PipeOp::ProjectOut(selector);
    Ok((operator, BubbledState::empty()))
}

pub(super) fn bubble_renamecover_operator(
    specs: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::RenameSpec>,
) -> Result<(ast_unresolved::PipeOp, BubbledState)> {
    let operator = ast_unresolved::PipeOp::Rename(specs);
    Ok((operator, BubbledState::empty()))
}

pub(super) fn bubble_transform_operator(
    transformations: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::NamedOutItem>,
    conditioned_on: Option<Box<ast_unresolved::TruthExpression>>,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(ast_unresolved::PipeOp, BubbledState)> {
    // A transform's target is a DOMAIN value; a published crossing carries
    // no lvar dependency of its own to bubble.
    let exprs: Vec<_> = transformations
        .iter()
        .filter_map(|item| item.expr.domain().cloned())
        .collect();

    let deps = bubble_expressions_collect_deps(&exprs, schema, system, cte_context, identities)?;
    let operator = ast_unresolved::PipeOp::Transform {
        items: transformations,
        guard: conditioned_on,
    };
    Ok((operator, BubbledState::with_unresolved(Vec::new(), deps)))
}

pub(super) fn bubble_domain_expression(
    expr: ast_unresolved::DomainExpression,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    outer_context: Option<&[crate::names::ColId]>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(ast_unresolved::DomainExpression, BubbledState)> {
    let mut collector = BubbleCollector {
        deps: vec![],
        schema,
        system,
        cte_context,
        outer_context,
        identities,
    };
    let result = collector.transform_domain(expr)?;
    Ok((
        result,
        BubbledState::with_unresolved(Vec::new(), collector.deps),
    ))
}

pub(super) fn bubble_callable(
    func: ast_unresolved::Callable,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(ast_unresolved::Callable, BubbledState)> {
    let mut collector = BubbleCollector {
        deps: vec![],
        schema,
        system,
        cte_context,
        outer_context: None,
        identities,
    };
    let result = collector.transform_callable(func)?;
    Ok((
        result,
        BubbledState::with_unresolved(Vec::new(), collector.deps),
    ))
}

pub(super) fn bubble_predicate_expression(
    pred: ast_unresolved::TruthExpression,
    schema: &dyn DatabaseSchema,
    system: Option<&DelightQLSystem>,
    cte_context: &mut HashMap<delightql_types::SqlIdentifier, crate::names::ScopeId>,
    outer_context: Option<&[crate::names::ColId]>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(ast_unresolved::TruthExpression, BubbledState)> {
    let mut collector = BubbleCollector {
        deps: vec![],
        schema,
        system,
        cte_context,
        outer_context,
        identities,
    };
    let result = collector.transform_boolean(pred)?;
    Ok((
        result,
        BubbledState::with_unresolved(Vec::new(), collector.deps),
    ))
}
