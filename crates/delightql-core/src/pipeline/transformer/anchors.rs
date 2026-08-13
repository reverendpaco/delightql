// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// anchors.rs — Publish the values a case must name more than once.
//
// SQL's simple `CASE` makes a null arm dead code — `x = NULL` is never true —
// so a match arm spelling `null` is lowered as a null-safe question, and that
// question names the anchor once per arm. An expression named twice is
// evaluated twice, and a volatile one is then two values, which can reach an
// arm no single value could.
//
// A reference or a literal repeats harmlessly. Anything else is PUBLISHED by
// the row that owns the case: the anchor becomes a column of the level the
// case reads from, and the authored occurrence is replaced by a reference to
// it. Occurrence, not spelling — two identically written anchors are two
// authored evaluations and get a column each.
//
// One evaluation is also not the target's default. A column holding a
// volatile expression is re-evaluated per reference on SQLite through every
// portable subquery boundary, so the published value goes out inside a window
// whose frame is the current row alone: `first_value(e) OVER (ROWS BETWEEN
// CURRENT ROW AND CURRENT ROW)` is `e` of this row, computed into the window's
// own buffer, and every target has it.

use std::rc::Rc;

use crate::error::Result;
use crate::names::{ColId, Registry, ScopeId};
use crate::pipeline::ast_refined;
use crate::pipeline::ast_transform::{
    same_phase_payload_folds, walk_transform_boolean, walk_transform_domain,
    walk_transform_operator, walk_transform_tabular_row, AstTransform,
};
use crate::pipeline::asts::core::columns::ColumnOccurrence;
use crate::pipeline::asts::core::expressions::references::Reference;
use crate::pipeline::asts::core::{FunctionApplication, LiteralValue, Refined};
use crate::pipeline::sql_ast::{
    DomainExpression as SqlDomainExpr, QueryExpression, SelectBuilder, SelectItem, SqlFrameBound,
    SqlFrameMode, SqlWindowFrame, TableExpression,
};
use crate::pipeline::transformer::builder::{Builder, Publication, Unprojected};
use crate::pipeline::transformer::{scalar, TransformCtx};

/// Publish every anchor in `operator` that the row must hold, then hand back
/// the level that publishes them and the operator that reads them.
pub(super) fn publishing_in_operator(
    builder: Builder<Unprojected>,
    operator: crate::pipeline::asts::core::operators::PipeOp<Refined>,
    ctx: &TransformCtx,
) -> Result<(
    Builder<Unprojected>,
    crate::pipeline::asts::core::operators::PipeOp<Refined>,
)> {
    publishing(builder, operator, ctx, |fold, operator| {
        walk_transform_operator(fold, operator)
    })
}

/// The same, for a structural run step — an ordering's expressions can
/// carry an anchored case that must be published by the level below.
pub(super) fn publishing_in_structural_step(
    builder: Builder<Unprojected>,
    step: crate::pipeline::asts::core::StructuralStep<Refined>,
    ctx: &TransformCtx,
) -> Result<(
    Builder<Unprojected>,
    crate::pipeline::asts::core::StructuralStep<Refined>,
)> {
    publishing(builder, step, ctx, |fold, step| {
        crate::pipeline::ast_transform::walk_transform_structural_step(fold, step)
    })
}

/// The same, for a restriction's condition.
pub(super) fn publishing_in_condition(
    builder: Builder<Unprojected>,
    condition: ast_refined::TruthExpression,
    ctx: &TransformCtx,
) -> Result<(Builder<Unprojected>, ast_refined::TruthExpression)> {
    publishing(builder, condition, ctx, |fold, condition| {
        walk_transform_boolean(fold, condition)
    })
}

/// The same, for one literal row of an anonymous relation.
///
/// A literal row has no relation under it to publish into, so what comes back
/// is the anchors themselves; `standing_on` builds the branch that holds them.
pub(super) fn publishing_in_row(
    row: ast_refined::TabularRow<ast_refined::Datum>,
    ctx: &TransformCtx,
) -> Result<(RowAnchors, ast_refined::TabularRow<ast_refined::Datum>)> {
    let at = ctx.identities.mint_derived_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
    );
    let mut fold = Publish {
        identities: &ctx.identities,
        at,
        published: Vec::new(),
    };
    let row = walk_transform_tabular_row(&mut fold, row)?;
    Ok((
        RowAnchors {
            at,
            published: fold.published,
        },
        row,
    ))
}

/// What a literal row asked to have published, and the scope that will.
pub(super) struct RowAnchors {
    at: ScopeId,
    published: Vec<Published>,
}

/// Put a literal row's branch over the anchors it was asked to publish.
///
/// The branch reads a body that holds them; without that body the arms would
/// name each expression once apiece, which for a volatile anchor is one value
/// per arm. An anchor that reads an earlier one stands a layer above it, and
/// every layer carries what the layers under it published — the branch names
/// the columns the fold minted, so the OUTERMOST layer is the one that
/// publishes those and the layers below hold occurrences of their own.
pub(super) fn standing_on(
    branch: SelectBuilder,
    anchors: RowAnchors,
    qualify: &dyn crate::pipeline::transformer::builder::Qualify,
    ctx: &TransformCtx,
) -> Result<SelectBuilder> {
    let RowAnchors { at, published } = anchors;
    if published.is_empty() {
        return Ok(branch);
    }
    let layers = in_layers(published);
    let last = layers.len() - 1;
    let mut carried: Vec<(ColId, ColId)> = Vec::new();
    let mut body: Option<(QueryExpression, ScopeId)> = None;
    for (depth, layer) in layers.into_iter().enumerate() {
        let scope = if depth == last {
            at
        } else {
            ctx.identities.mint_derived_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
            )
        };
        let here = |minted: ColId| -> ColId {
            if depth == last {
                minted
            } else {
                republication(minted, scope, &ctx.identities)
            }
        };
        let mut items = Vec::new();
        let mut columns = Vec::new();
        let mut next: Vec<(ColId, ColId)> = Vec::new();
        for (minted, below) in &carried {
            let slot = here(*minted);
            items.push(SelectItem::expression_with_alias(
                SqlDomainExpr::Column(*below),
                slot,
            ));
            columns.push(crate::pipeline::asts::core::ColumnMetadata::new(slot));
            next.push((*minted, slot));
        }
        for entry in layer {
            let value = scalar::s_lower_expression(entry.anchor, qualify, ctx)?;
            let reading = &carried;
            let value = value.map_columns(&|column| {
                reading
                    .iter()
                    .find(|(minted, _)| *minted == column)
                    .map_or(column, |(_, below)| *below)
            });
            let slot = here(entry.column);
            items.push(SelectItem::expression_with_alias(once_per_row(value), slot));
            columns.push(crate::pipeline::asts::core::ColumnMetadata::new(slot));
            next.push((entry.column, slot));
        }
        let mut sb = SelectBuilder::new().select_all(items);
        if let Some((under, alias)) = body {
            sb = sb.from_tables(vec![TableExpression::subquery(under, alias)]);
        }
        let holding = Publication::at(scope, columns, &ctx.identities)?.publish(sb)?;
        body = Some((QueryExpression::Select(Box::new(holding)), scope));
        carried = next;
    }
    let (under, alias) = body.expect("a layer was built for a non-empty publication");
    Ok(branch.from_tables(vec![TableExpression::subquery(under, alias)]))
}

/// A fresh occurrence of `minted` at `scope`, for a layer that only carries it
/// upward. The chain is what lets a reader addressed at the original find it.
fn republication(minted: ColId, scope: ScopeId, identities: &Rc<Registry>) -> ColId {
    let carried = identities.mint_column(
        scope,
        crate::names::ColumnOrigin::Republished {
            from: minted,
            how: crate::names::Republish::Passthrough,
        },
        None,
        crate::names::Addressing::Hygienic,
        crate::names::ValueFacts::default(),
    );
    carried
}

fn publishing<T>(
    builder: Builder<Unprojected>,
    payload: T,
    ctx: &TransformCtx,
    walk: impl FnOnce(&mut Publish, T) -> Result<T>,
) -> Result<(Builder<Unprojected>, T)> {
    let mut fold = Publish {
        identities: &ctx.identities,
        at: builder.publication().at_scope(),
        published: Vec::new(),
    };
    let payload = walk(&mut fold, payload)?;
    if fold.published.is_empty() {
        return Ok((builder, payload));
    }

    // Lowered against the level they read, which is why anchors that read
    // NOTHING published share one boundary and an anchor that reads an
    // earlier one gets the next: a select list is not visible to itself, so a
    // dependent anchor put beside the one it reads would name a column no
    // item can see.
    let mut builder = builder;
    for layer in in_layers(fold.published) {
        let mut values = Vec::with_capacity(layer.len());
        for entry in layer {
            let value = scalar::s_lower_expression(entry.anchor, &builder, ctx)?;
            values.push((once_per_row(value), entry.column));
        }
        builder = builder.bind_row_values(values)?;
    }
    Ok((builder, payload))
}

/// Group publications so that each layer reads only what the layers under it
/// published. Capture order is children-first, so a dependent anchor always
/// follows the one it reads and its layer is one past the deepest it names.
fn in_layers(published: Vec<Published>) -> Vec<Vec<Published>> {
    let mut layers: Vec<Vec<Published>> = Vec::new();
    for entry in published {
        let depth = entry.depth;
        while layers.len() <= depth {
            layers.push(Vec::new());
        }
        layers[depth].push(entry);
    }
    layers
}

/// The value, computed once per row.
///
/// A window is already its own fence — its argument is evaluated into the
/// window's buffer and the result is read from there — and no target admits a
/// window inside a window's argument, so a value that already carries one is
/// published as it stands. Everything else goes inside a frame of this row
/// alone: `first_value` over a one-row frame IS the row's value and keeps its
/// type, so this changes nothing about WHAT is published, only that the target
/// computes it once rather than again at each reader.
fn once_per_row(value: SqlDomainExpr) -> SqlDomainExpr {
    if already_windowed(&value) {
        return value;
    }
    SqlDomainExpr::WindowFunction {
        name: "first_value".to_string(),
        args: vec![value],
        distinct: false,
        partition_by: Vec::new(),
        order_by: Vec::new(),
        frame: Some(SqlWindowFrame {
            mode: SqlFrameMode::Rows,
            start: SqlFrameBound::CurrentRow,
            end: SqlFrameBound::CurrentRow,
        }),
    }
}

/// Does a window stand at THIS scalar level?
///
/// The question is about the value being published, and a subquery's interior
/// is not that value: it is a relation with its own levels, and a window
/// inside it buffers rows the outer expression never sees. An outer select
/// item beside one is an ordinary expression, flattenable and substitutable at
/// each reader, so it still needs the fence. The boundary is the same one
/// `map_columns` keeps for the same reason.
fn already_windowed(value: &SqlDomainExpr) -> bool {
    use SqlDomainExpr as E;
    let any = |es: &[E]| es.iter().any(already_windowed);
    match value {
        E::WindowFunction { .. } => true,
        // Relational boundaries. What buffers inside one is the inner
        // relation's, not this value's.
        E::Subquery(_) | E::Exists { .. } => false,
        E::Column(_)
        | E::Literal(_)
        | E::PublishedNameLiteral(_)
        | E::PublishedJsonPathLiteral(_)
        | E::JsonPathLiteral(_)
        | E::ScopeNameLiteral(_)
        | E::Star => false,
        E::Cast { expr, .. } | E::Unary { expr, .. } | E::Observation { expr, .. } => {
            already_windowed(expr)
        }
        E::Parens(expr) => already_windowed(expr),
        E::Binary { left, right, .. } => already_windowed(left) || already_windowed(right),
        E::Function { args, .. } | E::PredicateRewrite { args, .. } => any(args),
        E::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_deref().is_some_and(already_windowed)
                || when_clauses
                    .iter()
                    .any(|arm| already_windowed(arm.when()) || already_windowed(arm.then()))
                || else_clause.as_deref().is_some_and(already_windowed)
        }
    }
}

/// One value the row was asked to publish: what to compute, the column that
/// names it, and how many publications must stand under it first.
struct Published {
    anchor: ast_refined::DomainExpression,
    column: ColId,
    depth: usize,
}

struct Publish<'a> {
    identities: &'a Rc<Registry>,
    at: ScopeId,
    published: Vec<Published>,
}

impl Publish<'_> {
    /// Publish every supplied input this call will name more than once, and
    /// leave the call reading the columns instead of the expressions.
    fn publishing_mode_inputs(
        &mut self,
        mut select: crate::pipeline::asts::core::FieldSelect<Refined>,
    ) -> Result<crate::pipeline::asts::core::FieldSelect<Refined>> {
        let named = scalar::mode_input_occurrences(&select);
        for (position, argument) in scalar::mode_arguments_mut(&mut select).enumerate() {
            if named.get(position).copied().unwrap_or(0) < 2 || repeats_harmlessly(argument) {
                continue;
            }
            let column = self.identities.mint_column(
                self.at,
                crate::names::ColumnOrigin::Minted {
                    by: crate::names::MintReason::AnchoredCase,
                },
                None,
                crate::names::Addressing::Hygienic,
                crate::names::ValueFacts::default(),
            );
            let depth = self.depth_of(argument);
            let published = std::mem::replace(
                argument,
                ast_refined::DomainExpression::Reference(Reference::named(ColumnOccurrence {
                    column,
                    explicit_qualifier: false,
                })),
            );
            self.published.push(Published {
                anchor: published,
                column,
                depth,
            });
        }
        Ok(select)
    }

    /// How many publications must stand under this anchor: one past the
    /// deepest already-published column it names, or none if it names none.
    fn depth_of(&self, anchor: &ast_refined::DomainExpression) -> usize {
        use crate::pipeline::ast_visit::{walk_visit_domain, AstVisit, Descent};

        struct Reads<'a>(&'a [Published], usize);
        impl AstVisit<Refined> for Reads<'_> {
            fn enter_domain(&mut self, e: &ast_refined::DomainExpression) -> Result<Descent> {
                if let ast_refined::DomainExpression::Reference(Reference::Named(named)) = e {
                    let column = named.column().column;
                    if let Some(entry) = self.0.iter().find(|entry| entry.column == column) {
                        self.1 = self.1.max(entry.depth + 1);
                    }
                }
                Ok(Descent::Continue)
            }
        }

        let mut reads = Reads(&self.published, 0);
        match walk_visit_domain(&mut reads, anchor) {
            Ok(_) => reads.1,
            // A walk that cannot finish tells us nothing about what this reads,
            // so it stands above everything already published rather than
            // beside something it might name.
            Err(_) => self.published.len(),
        }
    }
}

/// A value that may be named more than once without being asked more than
/// once. ONE classification, for every form that names a value repeatedly —
/// the anchored case's anchor and the declared mode's supplied inputs are the
/// same question about the same kind of value.
fn repeats_harmlessly(value: &ast_refined::DomainExpression) -> bool {
    matches!(
        value,
        ast_refined::DomainExpression::Reference(_)
            | ast_refined::DomainExpression::Application(FunctionApplication::Ground(_))
    )
}

impl AstTransform<Refined, Refined> for Publish<'_> {
    same_phase_payload_folds!(Refined);

    fn transform_domain(
        &mut self,
        e: ast_refined::DomainExpression,
    ) -> Result<ast_refined::DomainExpression> {
        // Children first, so an anchor holding a case of its own is published
        // before the case that holds it asks for a column.
        let e = walk_transform_domain(self, e)?;
        // A MODE-COMPRESSED CALL SUPPLIES ONE VALUE PER DECLARED INPUT, and
        // the spelling names each of them more than once: once per arm where
        // the match row is a conjunction or asks about null, and again
        // wherever an output cell reads it back. Same question as the
        // anchor's, same answer — the row publishes it and the call reads
        // the column.
        let e = match e {
            ast_refined::DomainExpression::Application(FunctionApplication::FieldSelect(
                select,
            )) => ast_refined::DomainExpression::Application(FunctionApplication::FieldSelect(
                self.publishing_mode_inputs(select)?,
            )),
            other => other,
        };
        let ast_refined::DomainExpression::Application(FunctionApplication::Case(
            ast_refined::CaseExpression::Anchored {
                anchor,
                arms,
                default,
            },
        )) = e
        else {
            return Ok(e);
        };
        let asks_about_null = arms
            .iter()
            .any(|arm| matches!(arm.term, LiteralValue::Null));
        let anchor = if asks_about_null && !repeats_harmlessly(&anchor) {
            let column = self.identities.mint_column(
                self.at,
                crate::names::ColumnOrigin::Minted {
                    by: crate::names::MintReason::AnchoredCase,
                },
                None,
                crate::names::Addressing::Hygienic,
                crate::names::ValueFacts::default(),
            );
            let depth = self.depth_of(&anchor);
            self.published.push(Published {
                anchor: *anchor,
                column,
                depth,
            });
            Box::new(ast_refined::DomainExpression::Reference(Reference::named(
                ColumnOccurrence {
                    column,
                    explicit_qualifier: false,
                },
            )))
        } else {
            anchor
        };
        Ok(ast_refined::DomainExpression::Application(
            FunctionApplication::Case(ast_refined::CaseExpression::Anchored {
                anchor,
                arms,
                default,
            }),
        ))
    }
}
