// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Relational lowering: `r_lower_*` handlers.
//!
//! Each function lowers one AST node kind into builder operations.
//! `r_lower_*` functions take and return builders — they are the relational
//! algebra level of the transformation. Every function in this module starts
//! with `r_lower_` — no other prefixes, no exceptions.
//!
//! # Top-level handlers (called from `descend()`)
//!
//! - `r_lower_relation` — leaf: table, anonymous, TVF, inner relation
//! - `r_lower_filter` — WHERE predicate; `r_lower_bound` — LIMIT/OFFSET
//! - `r_lower_join` — JOIN two builders
//! - `r_lower_pipe` — left-fold pipe segments over a base builder
//! - `r_lower_set_op` — UNION / INTERSECT / EXCEPT
//!
//! # Pipe-segment handlers (called from `r_lower_pipe`)
//!
//! - `r_lower_projection` — SELECT list (`|> (cols)`)
//! - `r_lower_group_by` — GROUP BY + aggregates (`|> %(keys ~> aggs)`)
//! - `r_lower_order_by` — ORDER BY (`|> #(cols)`)
//! - `r_lower_limit` — LIMIT (`# < N`)
//! - `r_lower_distinct` — DISTINCT (`|> %(*)`)
//! - `r_lower_map_cover` — `|> $(fn:())(cols)`
//! - `r_lower_project_out` — `|> -(cols)`
//! - `r_lower_rename_cover` — `|> *(old as new)`
//! - `r_lower_transform` — `|> $$(expr as col)`
//! - `r_lower_embed_map` — `|> +$(fn:())(cols)`
//! - `r_lower_meta_ize` — `|> ^` / `|> ^^`
//! - `r_lower_witness` — `|> exists(*)` / `|> notexists(*)`
//! - `r_lower_drill_down` — `|> .col(*)`
//! - `r_lower_dml_terminal` — `|> update!()(*)`

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Refined;
use crate::pipeline::asts::refined as ast_refined;
/// A single pipe segment: the operator applied and the schema it produces.
pub(super) struct PipeSegment<P: crate::pipeline::asts::core::Phase> {
    pub step: PipeStep<P>,
    pub result: <P as crate::pipeline::asts::core::Phase>::Scope,
}

/// One step of the trailing run: an operator, or a dimension access.
///
/// Every access is a no-op in SQL — qualification and USING semantics are
/// settled in the refiner's metadata, never materialized here — so the two
/// stand side by side in the run rather than in two runs.
pub(super) enum PipeStep<P: crate::pipeline::asts::core::Phase> {
    Operator(crate::pipeline::asts::core::PipeOp<P>),
    Access,
    /// A structural step of the run, lowered by its own kind — the exact
    /// typed family, never the broad continuation enum.
    Structural(crate::pipeline::asts::core::StructuralStep<P>),
}
use crate::pipeline::sql_ast::TableExpression;

use super::anchors;
use super::builder::{
    wrap_origin, Builder, NameGenerator, Projected, Qualify, ScopeName, SqlLayout, Unprojected,
};
use super::scalar;
use super::tree_group;
use super::TransformCtx;
use crate::pipeline::asts::core::operators::{EmbedMapCover, MapCover};
use crate::pipeline::asts::core::ColumnMetadata;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{NamedReference, Reference};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn lower_join_type(join_type: Option<ast_refined::JoinType>) -> crate::pipeline::sql_ast::JoinType {
    use crate::pipeline::sql_ast::JoinType as SqlJoinType;

    match join_type {
        None | Some(ast_refined::JoinType::Inner) => SqlJoinType::Inner,
        Some(ast_refined::JoinType::LeftOuter) => SqlJoinType::Left,
        Some(ast_refined::JoinType::RightOuter) => SqlJoinType::Right,
        Some(ast_refined::JoinType::FullOuter) => SqlJoinType::Full,
    }
}

/// The semantic result the outermost node of a chain publishes.
#[stacksafe::stacksafe]
pub(crate) fn extract_relation(expr: &ast_refined::Chain) -> &crate::relation::SemanticRelation {
    match expr.continuations().last() {
        Some(step) => step.result(),
        None => expr.head().result(),
    }
}

/// The registry occurrence that relation is stored under.
///
/// LOWERING'S ONE ROAD from the semantic result to a physical question.
/// It projects; it does not invert — nothing here can turn a scope back
/// into a relation, so no lowering can mint one.
/// One branch's physical output list, as the transformer laid it out.
///
/// Both halves come off the [`SetArm`] that made it, and there is no
/// constructor: the struct is written only inside that value's two layout
/// methods, which read the relation from themselves. Nothing anywhere
/// takes a relation beside a column list.
pub(crate) struct BranchLayout {
    arm: crate::relation::SemanticRelation,
    columns: Vec<crate::names::ColId>,
}

impl BranchLayout {
    /// A layout stated outright, for the witnesses that must be able to
    /// make one DISAGREE with its evidence. Production has no such road:
    /// the two halves come off one `SetArm`.
    #[cfg(test)]
    pub(crate) fn for_test(
        arm: crate::relation::SemanticRelation,
        columns: Vec<crate::names::ColId>,
    ) -> Self {
        BranchLayout { arm, columns }
    }

    pub(crate) fn arm(&self) -> &crate::relation::SemanticRelation {
        &self.arm
    }

    pub(crate) fn columns(&self) -> &[crate::names::ColId] {
        &self.columns
    }
}

/// ONE LOWERED SET ARM: the statement, and the relation the chain it was
/// lowered from carries.
///
/// Made from a CHAIN and nothing else, so the two halves have no separate
/// existence to be paired wrongly. There is no entrance anywhere that takes
/// a relation beside an output list — the layout an arm hands the physical
/// binding comes out of this value, which knows both because it lowered
/// both from one expression.
pub(super) struct SetArm {
    builder: Builder<Projected>,
    relation: crate::relation::SemanticRelation,
}

impl SetArm {
    /// THE ONE PRODUCER.
    pub(super) fn lower(
        expr: ast_refined::Chain,
        names: &NameGenerator,
        ctx: &TransformCtx,
    ) -> Result<Self> {
        let relation = *extract_relation(&expr);
        Ok(SetArm {
            builder: super::descend::descend_as_query(expr, names, ctx)?,
            relation,
        })
    }

    /// AS IT STANDS: the positional stack adds no boundary, so what a
    /// branch emits is what its own builder already publishes.
    pub(super) fn as_it_stands(&self) -> BranchLayout {
        BranchLayout {
            arm: self.relation,
            columns: self
                .builder
                .columns()
                .iter()
                .map(ColumnMetadata::identity)
                .collect(),
        }
    }

    /// Republished under the boundary the operation gives it. Minting that
    /// boundary, rewriting the statement to it, and recording what the
    /// branch now emits are ONE act.
    pub(super) fn lay_out(
        self,
        index: usize,
        ctx: &TransformCtx,
        names: &NameGenerator,
    ) -> Result<(
        crate::pipeline::sql_ast::QueryExpression,
        crate::names::ScopeId,
        Vec<ColumnMetadata>,
        Vec<ColumnMetadata>,
        BranchLayout,
        crate::sql_binding::SqlSiteId,
    )> {
        let metadata = self.builder.columns().to_vec();
        let scope = ColumnMetadata::common_identity_scope(&metadata, &ctx.identities)
            .map_or_else(
                || names.anonymous(),
                |input| names.set_arm(input, index as u16),
            )
            .identity();
        let mut query = self.builder.to_sql()?;
        let republished =
            super::builder::republish_under(&mut query, scope, &metadata, &ctx.identities)?;
        let layout = BranchLayout {
            arm: self.relation,
            columns: republished.iter().map(ColumnMetadata::identity).collect(),
        };
        // THE BRANCH REPUBLISHES THE ARM, position for position: each
        // republication stands where the arm's own port stands, stated one
        // at a time rather than zipped against a list.
        let mut row = ctx
            .identities
            .bindings()
            .emitting(&ctx.relations, &self.relation)?;
        for (slot, port) in layout
            .columns
            .iter()
            .copied()
            .zip(ctx.relations.interface(&self.relation)?.ports().to_vec())
        {
            row.publishes(slot, port)?;
        }
        let site = row.close(&ctx.relations)?;
        Ok((query, scope, metadata, republished, layout, site))
    }

    pub(super) fn into_builder(self) -> Builder<Projected> {
        self.builder
    }
}

/// Build a SQL column expression from a `ColumnMetadata`, properly qualified.
///
/// This is the universal "pass-through column" pattern — every projection
/// operator needs to turn a scope column into a `DomainExpression`.
fn qualified_col_expr(col: &ColumnMetadata) -> crate::pipeline::sql_ast::DomainExpression {
    crate::pipeline::sql_ast::DomainExpression::Column(col.identity())
}

/// Build a pass-through `SelectItem` from a `ColumnMetadata`.
fn passthrough_item(col: &ColumnMetadata) -> crate::pipeline::sql_ast::SelectItem {
    use crate::pipeline::sql_ast::SelectItem;

    SelectItem::Publishing {
        expr: qualified_col_expr(col),
        slot: col.identity(),
        printed: true,
    }
}

/// Project the exact source port recorded for every output position.
///
/// These edit forms only carry positions: none computes a value or merges
/// several inputs. The authority's source record is therefore total and
/// single-valued here. SQL lowering binds that semantic source at the current
/// physical site and never searches values, names, provenance, or columns.
fn select_carried_items(
    builder: &Builder<Unprojected>,
    result: &crate::relation::SemanticRelation,
    relations: &crate::relation::Relations,
) -> Result<Vec<crate::pipeline::sql_ast::SelectItem>> {
    relations
        .carried_sources(result)?
        .into_iter()
        .map(|(output, sources)| {
            let [source] = sources.as_slice() else {
                return Err(DelightQLError::parse_error(format!(
                    "a carried output position must have exactly one construction-recorded source; {:?} has {}",
                    output,
                    sources.len()
                )));
            };
            Ok(crate::pipeline::sql_ast::SelectItem::Publishing { expr: crate::pipeline::sql_ast::DomainExpression::Column(
                    builder.rebind_port(*source)?,
                ), slot: output.column(), printed: true })
        })
        .collect()
}

/// Build a `json_each(source.column) AS alias` table-valued function expression.
///
/// Used by both `r_lower_melt_join` and `build_json_each_query` — the shared
/// pattern for expanding a JSON array column into rows. The column is always
/// an array the transformer built (a melt packet), so the TVF carries the
/// array-provenance internal name — spelled `json_each` canonically, but
/// respellable per-dialect where each-over-array needs a different form.
fn json_each_tvf(
    column: crate::names::ColId,
    tvf_scope: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> TableExpression {
    use crate::pipeline::sql_ast::TvfArgument;
    let function = identities.mint_intrinsic(crate::names::Intrinsic::JsonEachArray);
    TableExpression::TVF {
        function,
        arguments: vec![TvfArgument::Column(column)],
        alias: tvf_scope,
    }
}

/// Lower a TVF (Table-Valued Function) like `json_each(...)` or `pragma_table_info(...)`.
///
/// Converts each `HoArgument::Scalar` to a structured `TvfArgument`, preserving
/// literals, identifiers, and qualified references without stringifying.
fn r_lower_tvf(
    function: crate::names::FnId,
    ho_arguments: crate::pipeline::asts::core::operators::CallArguments<Refined>,
    result: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::table::TvfArgument;

    let arguments: Vec<TvfArgument> = ho_arguments
        .ho_members()
        .filter_map(|arg| match arg {
            crate::pipeline::asts::core::operators::HoArgument::Value(value) => {
                Some(lower_tvf_argument(value.value.clone()))
            }
            crate::pipeline::asts::core::operators::HoArgument::Relation(_)
            | crate::pipeline::asts::core::operators::HoArgument::Rule(_)
            | crate::pipeline::asts::core::operators::HoArgument::Landed(_) => None,
            crate::pipeline::asts::core::operators::HoArgument::Skip => None,
            crate::pipeline::asts::core::operators::HoArgument::Landing(landing) => {
                match *landing {}
            }
        })
        .collect::<Result<_>>()?;

    let scope = result.scope();

    let table_expr = TableExpression::TVF {
        function,
        arguments,
        alias: scope,
    };

    let columns = columns_from_relation(&result, ctx)?;

    Builder::from_table(
        table_expr,
        ScopeName::Resolved(scope),
        columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
    )
}

/// Convert a scalar domain expression to a structured TVF argument.
fn lower_tvf_argument(
    expr: ast_refined::DomainExpression,
) -> Result<crate::pipeline::sql_ast::table::TvfArgument> {
    use crate::pipeline::sql_ast::table::TvfArgument;

    match expr {
        ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Ground(
            value,
        )) => Ok(TvfArgument::Literal(value)),
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Ok(TvfArgument::Column(column.column())),
        ast_refined::DomainExpression::Reference(Reference::Physical(column)) => {
            Ok(TvfArgument::Column(column))
        }
        _ => Err(DelightQLError::ParseError {
            message: "TVF arguments must be resolved columns or literals".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

/// Lower an anonymous relation (`_(1, 2, 3)`) into a `Builder<Unprojected>`.
///
/// Builds one `SELECT` per row (no FROM), folds with UNION ALL.
/// Lower an anonymous table head.
pub(super) fn r_lower_anon_table(
    anon: ast_refined::AnonRelation,
    result: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    r_lower_anonymous(anon.table.body.rows, result, names, ctx)
}

fn r_lower_anonymous(
    rows: crate::pipeline::asts::vocabulary::Vec1<
        crate::pipeline::asts::core::TabularRow<crate::pipeline::asts::core::Datum<Refined>>,
    >,
    result: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{
        query::SetOperator, QueryExpression, SelectBuilder, SelectItem,
    };

    let scope = result.scope();
    let output_columns = relation_output_columns(&result, ctx)?;

    let dummy = DummyQualify(&ctx.identities);
    // The publication belongs to the union's result, not to any one row: the
    // first row publishes it and every row after aligns with it.
    let published = SqlLayout::new(
        scope,
        output_columns
            .iter()
            .copied()
            .map(ColumnMetadata::new)
            .collect(),
        &ctx.identities,
    );

    let mut row_queries: Vec<QueryExpression> = Vec::new();
    for (row_idx, row) in rows.into_vec().into_iter().enumerate() {
        let mut sb = SelectBuilder::new();
        // A LITERAL ROW IS STILL A ROW. A cell whose case must name its
        // anchor more than once gets that anchor published beneath this
        // branch, exactly as an operator's would — the branch is the row that
        // owns it.
        let (published_anchors, row) = anchors::publishing_in_row(row, ctx)?;
        for (col_idx, datum) in row.into_vec().into_iter().enumerate() {
            let val = datum.into_value();
            let sql_expr = scalar::s_lower_expression(val, &dummy, ctx)?;
            let alias = output_columns.get(col_idx).copied();
            // Only first row gets aliases (SQL UNION ALL infers from first branch)
            if row_idx == 0 {
                sb = sb.select(match alias {
                    Some(alias) => SelectItem::expression_with_alias(sql_expr, alias),
                    None => {
                        SelectItem::scaffolding_value(sql_expr, ctx.identities.scaffolding_slot())
                    }
                });
            } else {
                sb = sb.select(SelectItem::Scaffolding {
                    expr: sql_expr,
                    slot: ctx.identities.scaffolding_slot(),
                });
            }
        }
        // Only the first row spells the aliases, because SQL takes a UNION's
        // output names from its first branch. The rows after it fill the same
        // slots and name nothing of their own — a fold's row reads an OUTER
        // column, so what its item stands for is not this scope's to publish.
        let sb = anchors::standing_on(sb, published_anchors, &dummy, ctx)?;
        let stmt = if row_idx == 0 {
            published.publish(sb)?
        } else {
            sb.standing_at(scope).map_err(DelightQLError::parse_error)?
        };
        row_queries.push(QueryExpression::Select(Box::new(stmt)));
    }

    let query = row_queries
        .into_iter()
        .reduce(|left, right| QueryExpression::SetOperation {
            op: SetOperator::UnionAll,
            left: Box::new(left),
            right: Box::new(right),
        })
        .ok_or_else(|| DelightQLError::ParseError {
            message: "r_lower_anonymous: empty rows".to_string(),
            source: None,
            subcategory: None,
        })?;

    let columns = columns_from_relation(&result, ctx)?;

    // A LITERAL RELATION BINDS LIKE ANY OTHER. The join road reaches this
    // one directly, so the site is recorded here rather than by the descent
    // that would otherwise have bound it — a sibling's reference to one of
    // these positions has nowhere else to ask.
    Builder::from_frozen(
        query,
        ScopeName::Resolved(scope),
        columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
    )?
    .bind_relation(result, &ctx.relations)
}

/// Lower an inner relation (interior subquery).
///
/// All patterns (UDT, CDT-SJ, CDT-GJ, CDT-WJ) share the same core:
/// recursively descend into the subquery, finalize to a QueryExpression,
/// and wrap as a Frozen builder with the inner relation's scope.
///
/// The subquery is a full `Chain` — pipes, filters, joins,
/// even nested inner relations — processed by the same `descend()` path
/// as any exterior query. Induction handles depth.
fn r_lower_inner_relation(
    pattern: ast_refined::InnerRelationPattern,
    result: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use super::descend;

    let carries_correlation = matches!(
        pattern,
        ast_refined::InnerRelationPattern::CorrelatedScalarJoin { .. }
            | ast_refined::InnerRelationPattern::CorrelatedGroupJoin { .. }
    );
    let subquery = match pattern {
        ast_refined::InnerRelationPattern::Indeterminate { .. } => {
            return Err(DelightQLError::ParseError {
                message:
                    "r_lower_inner_relation: Indeterminate pattern should be classified by refiner"
                        .to_string(),
                source: None,
                subcategory: None,
            });
        }
        ast_refined::InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. }
        | ast_refined::InnerRelationPattern::CorrelatedScalarJoin { subquery, .. }
        | ast_refined::InnerRelationPattern::CorrelatedGroupJoin { subquery, .. } => subquery,
    };

    let body = subquery.semantic_relation();
    // A PHYSICAL WRAP STANDS AT A RANGE OF ITS OWN. Where the relation
    // inside the subquery is the relation outside it, nothing was published
    // across the boundary and the semantic scope still belongs to the level
    // beneath — reusing it for the derived table would name two different
    // FROM entries the same, and a base-table scope reused this way makes a
    // wrap's own column indistinguishable from a catalog column.
    let scope = if body == result {
        names
            .wrap(result.scope(), crate::names::WrapReason::Limit)
            .identity()
    } else {
        result.scope()
    };

    // Recursive descent into the subquery — same path as any exterior query.
    let inner_names = names.fork();
    let inner_builder = if carries_correlation {
        descend::descend_as_query_carrying_hygiene(*subquery, &inner_names, ctx)?
    } else {
        descend::descend_as_query(*subquery, &inner_names, ctx)?
    };
    // Compare inner output names with the published heading. If they differ
    // (e.g., the heading says "fn" but inner outputs "first_name"), inject a
    // rename projection so the finalized SQL outputs the published names.
    //
    // A PHYSICAL WRAP PUBLISHES NOTHING SEMANTICALLY, AND STILL EMITS. The
    // refiner stands a bounded body inside a subquery so a later step
    // observes the bounded rows; the relation on both sides is one relation,
    // so no boundary is republished. But a derived table is a RANGE, and a
    // column it offers must belong to the range it is aliased by — a body
    // column carried through unchanged qualifies by the FROM entry the
    // enclosing statement does not have.
    let (query, cpr_columns) = if body == result {
        requalify_physical_wrap(inner_builder, scope, ctx)?
    } else {
        publish_relation_body(inner_builder, &result, ctx)?
    };

    // Hygienic columns (__dql_corr_0 etc.) are in the subquery output for
    // JOIN ON but NOT in the published scope. The Qualify fallback uses the
    // scope's own name as qualifier, so join conditions still resolve correctly.

    // Return as Table with subquery — not Frozen. This way, the join
    // handler's into_table_expr() passes the TableExpression through
    // directly instead of wrapping it again with a generated alias.
    let table_expr = TableExpression::subquery(query, scope);
    // THE DERIVED TABLE IS THE RELATION IT PUBLISHES. Binding it here is what
    // tells the join above which of its offered columns are the boundary's
    // positions and which are the support it still owes.
    Builder::from_table(
        table_expr,
        ScopeName::Resolved(scope),
        cpr_columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
    )?
    .bind_relation(result, &ctx.relations)
}

/// Lower a ConsultedView: view body inlined as a subquery, reconciled
/// against the boundary the resolver published.
fn r_lower_consulted_view(
    body: ast_refined::Query,
    scoped: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let scope = scoped.scope();
    let cpr_columns = ctx
        .relations
        .interface(&scoped)?
        .ports()
        .iter()
        .map(|port| ColumnMetadata::new(port.column()))
        .collect();

    let body_sql = {
        let ast_refined::Query { locals, body } = body;
        let ctes = locals.into_ctes();
        let sql_ctes: Vec<crate::pipeline::sql_ast::Cte> = ctes
            .into_iter()
            .map(|binding| lower_cte_binding(binding, names, ctx))
            .collect::<Result<_>>()?;

        let inner_builder = super::descend::descend_as_final(body, names, ctx)?;
        let (main_query, _) = publish_relation_body(inner_builder, &scoped, ctx)?;

        if sql_ctes.is_empty() {
            main_query
        } else {
            // Merge CTEs if the body already has a WITH clause
            match main_query {
                crate::pipeline::sql_ast::QueryExpression::WithCte {
                    ctes: inner_ctes,
                    query: inner_query,
                } => {
                    let mut merged = sql_ctes;
                    merged.extend(inner_ctes);
                    crate::pipeline::sql_ast::QueryExpression::WithCte {
                        ctes: merged,
                        query: inner_query,
                    }
                }
                other => crate::pipeline::sql_ast::QueryExpression::WithCte {
                    ctes: sql_ctes,
                    query: Box::new(other),
                },
            }
        }
    };

    let table_expr = TableExpression::subquery(body_sql, scope);
    Builder::from_table(
        table_expr,
        ScopeName::Resolved(scope),
        cpr_columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
    )
}

/// Lower a ground relation with positional (argumentative) access.
///
/// Emits `SELECT original AS alias, ... FROM table` — a rename projection
/// that drops underscored positions and renames columns per the user's
/// positional binding. The result is wrapped as a Frozen subquery so that
/// downstream consumers (joins, pipes) see the renamed columns.
///
/// Hygienic columns (literal grounding positions) are included in the
/// subquery SELECT but will be stripped by a wrapping layer when the
/// resolver-lifted Filter node is processed.
fn r_lower_positional_relation(
    scope: crate::names::ScopeId,
    result: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectBuilder, SelectItem,
    };
    let mut columns = columns_from_relation(&result, ctx)?;
    let carried = ctx.relations.carried_sources(&result)?;
    let Some(input) = ctx.relations.read_source(&result)? else {
        return Err(DelightQLError::parse_error(
            "a positional relation has no construction-recorded read to stand on",
        ));
    };
    let input_ports = ctx.relations.interface(&input)?;
    let input_site = ctx
        .identities
        .bindings()
        .bind_interface(&ctx.relations, &input)?;
    let from = ground_table_expression(&input, ctx)?;
    let mut select_items = Vec::new();
    for (col, (port, sources)) in columns.iter().zip(carried) {
        let output = col.identity();
        if port.column() != output {
            return Err(DelightQLError::parse_error(
                "a positional output disagrees with its construction record",
            ));
        }
        if sources.len() != 1 {
            return Err(DelightQLError::parse_error(
                "a positional output does not carry exactly one source",
            ));
        }
        // WHICH POSITION OF THE READ THIS OUTPUT NAMES, from the record and
        // nothing else. Two kinds of edge answer: the position the
        // construction recorded this output as CARRYING, and the position a
        // refinement recorded it as REPLACING. Both are things the authority
        // wrote down; neither is preferred over the other, because a
        // precedence between two records is a choice this road has no
        // grounds to make. They must agree on ONE position of the read, and
        // where they do not this refuses rather than taking the first.
        let mut named: Vec<crate::relation::PortId> = Vec::new();
        for candidate in ctx
            .relations
            .ancestors_into(&result, port)?
            .into_iter()
            .chain(sources.iter().copied())
        {
            if input_ports.ports().contains(&candidate) && !named.contains(&candidate) {
                named.push(candidate);
            }
        }
        let source = match named.as_slice() {
            [source] => *source,
            [] => {
                return Err(DelightQLError::parse_error(
                    "a positional output has no construction-recorded source port",
                ))
            }
            _ => {
                return Err(DelightQLError::parse_error(
                    "a positional output carries several source ports",
                ))
            }
        };
        let source = ctx.identities.bindings().at(input_site, source)?;
        select_items.push(SelectItem::Publishing {
            expr: SqlDomainExpr::Column(source),
            slot: output,
            printed: true,
        });
    }
    for dependency in ctx.relations.dependencies(&result)? {
        let source = ctx.identities.bindings().at(input_site, dependency)?;
        let support = ctx
            .identities
            .sql_column(scope, None, crate::names::Addressing::Hygienic);
        select_items.push(SelectItem::Publishing {
            expr: SqlDomainExpr::Column(source),
            slot: support,
            printed: true,
        });
        columns.push(ColumnMetadata::new(support));
    }

    // THE CROSSED SLOT UNIFIES, NULL-SAFELY. The slot carries the column and
    // the truth read as a VALUE, and the comparison between them is spelled
    // HERE — where the source column is already in scope — rather than at
    // resolution, where the value operand would have to be able to hold a
    // truth. Plain SQL equality is the wrong operator: unification answers
    // yes when a null meets a null, and `=` answers unknown.
    //
    // Both operands are the READ's positions: the slot names one of them and
    // the truth's interior names others. Qualifying against this access
    // instead would answer with the support alias it emits for the same
    // value — a name the level's own WHERE cannot rely on surviving.

    // THE ROW IS THE INTERFACE AND THEN THE SUPPORT IT EMITTED. The
    // hygienic slots a crossed truth needed are the compiler's own: they
    // realize no occurrence and nothing above addresses them, and saying so
    // one position at a time is what keeps the interface from being carved
    // out of the row by width.
    let mut row = ctx
        .identities
        .bindings()
        .emitting(&ctx.relations, &result)?;
    let mut emitted = columns.iter().map(ColumnMetadata::identity);
    for port in ctx.relations.interface(&result)?.ports().to_vec() {
        let slot = emitted.next().ok_or_else(|| {
            DelightQLError::parse_error(
                "a positional relation emits fewer positions than it publishes",
            )
        })?;
        row.publishes(slot, port)?;
    }
    for slot in emitted {
        row.scaffolds(slot);
    }
    let site = row.close(&ctx.relations)?;

    let select = SelectBuilder::new()
        .select_all(select_items)
        .from_tables(vec![from]);
    let stmt = select
        .standing_at(scope)
        .map_err(crate::error::DelightQLError::parse_error)?;

    let query = QueryExpression::Select(Box::new(stmt));
    Builder::from_frozen_at_site(
        query,
        ScopeName::Resolved(scope),
        columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
        site,
    )
}

// ---------------------------------------------------------------------------
// Top-level handlers (called from descend())
// ---------------------------------------------------------------------------

/// Lower a base `Relation` (table, anonymous, TVF, inner relation, etc.)
/// Name a ground relation in the FROM clause.
///
/// A scope standing for a base table is emitted by its own name only because
/// that name is the table's. Give the table a user alias and the two part
/// company: the scope answers to the alias, which names no table the engine
/// has. So an aliased base table is emitted as the entity it reads, aliased —
/// the alias is what every reference to it is qualified by.
///
/// Only one step of origin is consulted, never the chain: a CTE read, a pipe
/// stage and a wrapper all descend from a base table too, and each is a
/// derived relation that must keep being emitted under its own name.
/// `(SELECT <heading> FROM <cte>) AS <occurrence>` — the occurrence's heading
/// read off the CTE's, each occurrence column paired with the CTE column its
/// own chain carries. Position cannot be the correspondence: the occurrence
/// heading may be a SUBSET of the CTE's — a hygienic carrier stays in the
/// CTE's registered heading but stops at the access — and a subset zipped by
/// index misbinds. Position stands in only where no chain evidence disputes
/// it: same width, and every target the chains DO identify already sits at
/// its own index. A target two sources claim, or identified evidence that
/// contradicts position, is ambiguity rather than absence — the occurrence
/// keeps the bare scope, which fails loudly downstream instead of selecting
/// AN UNACCESSED INCHOATE OCCURRENCE YIELDS ZERO ROWS under its opaque
/// displayed heading. The read stays the relation it names — the subquery
/// exposes the physical heading so every reference qualifies as usual —
/// and the impossible predicate is what makes the annihilation the
/// occurrence's own: an outer join against it keeps its other arm.
fn annihilated_read(
    entity: crate::names::EntityId,
    scope: crate::names::ScopeId,
) -> TableExpression {
    use crate::pipeline::ast_refined::LiteralValue as SqlLit;
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlExpr, QueryExpression, SelectItem, SelectStatement,
    };
    let empty = SelectStatement::builder()
        .select(SelectItem::star_over_nothing())
        .from_tables(vec![TableExpression::Entity {
            entity,
            alias: None,
        }])
        .where_clause(SqlExpr::Binary {
            left: Box::new(SqlExpr::Literal(SqlLit::Number("0".to_string()))),
            op: crate::pipeline::sql_ast::BinaryOperator::Equal,
            right: Box::new(SqlExpr::Literal(SqlLit::Number("1".to_string()))),
        })
        .standing_at(scope)
        .expect("the annihilating read is a complete select");
    TableExpression::subquery(QueryExpression::Select(Box::new(empty)), scope)
}

/// values by table order.
fn cte_occurrence(
    occurrence: &crate::relation::SemanticRelation,
    cte: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<TableExpression> {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, SelectStatement,
    };
    let scope = occurrence.scope();
    let carried = ctx.relations.carried_sources(occurrence)?;
    if carried.is_empty() {
        return Ok(TableExpression::Scope(scope));
    }
    let items: Vec<SelectItem> = carried
        .into_iter()
        .map(|(target, sources)| {
            let [source] = sources.as_slice() else {
                return Err(DelightQLError::parse_error(
                    "a CTE occurrence output must carry exactly one definition port",
                ));
            };
            Ok(SelectItem::Publishing {
                expr: SqlDomainExpr::Column(source.column()),
                slot: target.column(),
                printed: true,
            })
        })
        .collect::<Result<_>>()?;
    match (SelectStatement::builder()
        .select_all(items)
        .from_tables(vec![TableExpression::Scope(cte)]))
    .standing_at(scope)
    .map_err(crate::error::DelightQLError::parse_error)
    {
        Ok(select) => Ok(TableExpression::subquery(
            QueryExpression::Select(Box::new(select)),
            scope,
        )),
        Err(error) => Err(error),
    }
}

fn ground_table_expression(
    relation: &crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<TableExpression> {
    let scope = relation.scope();
    // A ground names the entity it reads, and an alias beside it so references
    // have something to qualify by. Emitting the scope alone leaves the FROM
    // entry spelled by whatever name the scope is given — and a scope competing
    // for the table's own name loses it, so the statement reads from a table
    // that does not exist. The entity is not a name to be assigned; it is the
    // one thing here that already has one.
    if let Some(entity) = ctx.relations.entity(relation)? {
        let input_annihilated = ctx
            .relations
            .inputs(relation)?
            .iter()
            .any(|input| ctx.identities.is_annihilated(input.scope()));
        return if ctx.identities.is_annihilated(scope) || input_annihilated {
            Ok(annihilated_read(entity, scope))
        } else {
            Ok(TableExpression::Entity {
                entity,
                alias: Some(scope),
            })
        };
    }

    // A CTE use is a fresh semantic occurrence over one exact definition
    // relation. The construction record says both facts; a naming-scope kind
    // and parent walk cannot become evidence for either one.
    if ctx.relations.instance_kind(relation)? == Some(crate::relation::form::DefinitionKind::Cte) {
        let inputs = ctx.relations.inputs(relation)?;
        let [definition] = inputs.as_slice() else {
            return Err(DelightQLError::parse_error(
                "a CTE occurrence does not have exactly one construction-recorded definition",
            ));
        };
        return cte_occurrence(relation, definition.scope(), ctx);
    }

    Ok(TableExpression::Scope(scope))
}

/// into a fresh `Builder<Unprojected>`.
///
/// This is the leaf case — the base of the dive-and-bubble recursion.
pub(super) fn r_lower_read(
    rel: ast_refined::Relation,
    access: Option<ast_refined::Access>,
    result: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    match rel {
        ast_refined::Relation::Ground { .. } => {
            let access = access.unwrap_or(ast_refined::Access::All);
            let scope = result;

            // A caller pattern: emit SELECT original AS alias for each column
            if matches!(access, ast_refined::Access::Slots(_)) {
                return r_lower_positional_relation(scope.scope(), result, names, ctx);
            }

            // Glob/bare: all columns, no rename
            let table_expr = ground_table_expression(&scope, ctx)?;
            let columns = columns_from_relation(&result, ctx)?;

            Builder::from_table(
                table_expr,
                ScopeName::Resolved(scope.scope()),
                columns,
                names.fork(),
                std::rc::Rc::clone(&ctx.identities),
            )
        }

        ast_refined::Relation::InnerRelation { pattern, .. } => {
            r_lower_inner_relation(pattern, result, names, ctx)
        }

        ast_refined::Relation::ConsultedView { body, .. } => {
            r_lower_consulted_view(*body, result, names, ctx)
        }

        ast_refined::Relation::FunctorCall { call, alias: () } => {
            // A mutation call heading a chain is consumed WHOLE by dml.rs;
            // reaching this relation lowering means further terms stand on
            // it — the multi-step DML shape (comma=dataflow,
            // semicolon=sequential), documented scripting but not
            // implemented. Refuse cleanly instead of lowering the call as a
            // read.
            if super::is_mutation_call(&call, ctx) {
                return Err(DelightQLError::validation_error_categorized(
                    "dml/shape/multi_terminal",
                    "a DML terminal (insert!/update!/delete!) must be the \
                     final operation of a statement; multi-step DML via `,` \
                     (dataflow) or `;` (sequential) is not yet supported",
                    "run each mutation as a separate statement",
                ));
            }
            let call = call.into_inner();
            let function = call.callee;
            r_lower_tvf(function, call.arguments, result, names, ctx)
        }
    }
}

/// Reconcile an inner builder's output columns with the published heading.
///
/// If the inner builder outputs different column names from what the heading
/// expects (e.g., inner has `first_name` but the heading says `fn`), inject a
/// rename projection before finalizing. This ensures the SQL output matches
/// the scope names — the alias-scope invariant.
///
/// For columns beyond the heading's width (hygienic columns like `__dql_corr_0`),
/// they are passed through unchanged.
/// Extract column metadata from a CTE binding's expression.
/// Used by the CTE lowering in mod.rs to reconcile CTE body output columns.
///
/// Returns empty if the heading has duplicate column names (e.g., from a join
/// before disambiguation), since reconciling would create duplicate SQL aliases.
/// Argumentative binding on the recursive
/// self-reference (`c(m)` inside c's own definition) does not bind today:
/// the rename mis-merges into a NULL-padded two-column union and returns
/// SILENTLY WRONG results. Hard-refuse until the rename-hoist legalization
/// (`WITH c(m) AS (…)` — needs the Cte column list) lands.
///
/// WHETHER a binding is recursive is not re-decided here: the authority's
/// answer travels on the binding and gates this walk. What the walk asks is
/// the SHAPE of the self-reference the decision already found — a question
/// the decision does not answer, and the reason there is a walk at all.
fn check_recursive_argumentative_binding(
    binding: &ast_refined::CteBinding,
    identities: &crate::names::Registry,
) -> Result<()> {
    if !binding.body().is_fixpoint() {
        return Ok(());
    }
    let binding_scope = *binding.subject();
    if binding
        .parts()
        .into_iter()
        .any(|part| expr_has_positional_self_ref(part, binding_scope.scope(), identities))
    {
        return Err(DelightQLError::ValidationError {
            message: "a recursive CTE reference uses argumentative binding; renames and \
                 constraints on the self-reference do not bind inside a recursive \
                 definition yet. Use glob binding and rename or filter in a pipe stage."
                .to_string(),
            context: "transformer::lower_cte_binding".to_string(),
            subcategory: Some(crate::uri_registry::subcat::RECURSION_ARGUMENTATIVE_BINDING),
        });
    }
    Ok(())
}

#[stacksafe::stacksafe]
fn expr_has_positional_self_ref(
    expr: &ast_refined::Chain,
    binding: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> bool {
    if let ast_refined::GroundForm::Reference(rel) = expr.head().form() {
        if read_is_positional_self_ref(
            rel,
            expr.head().result(),
            expr.head_access(),
            binding,
            identities,
        ) {
            return true;
        }
    }
    expr.forms().any(|continuation| match continuation {
        ast_refined::Continuation::Member { rhs, .. } => {
            expr_has_positional_self_ref(rhs, binding, identities)
        }
        ast_refined::Continuation::BagOp { arm, .. } => {
            expr_has_positional_self_ref(arm, binding, identities)
        }
        ast_refined::Continuation::Access { .. }
        | ast_refined::Continuation::Restrict { .. }
        | ast_refined::Continuation::Correlate { .. }
        | ast_refined::Continuation::Bound { .. }
        | ast_refined::Continuation::Destructure { .. }
        | ast_refined::Continuation::Pipe { .. }
        | ast_refined::Continuation::Structural(_)
        | ast_refined::Continuation::ErJoin(_) => false,
    })
}

fn read_is_positional_self_ref(
    rel: &ast_refined::Relation,
    read: &crate::relation::SemanticRelation,
    access: Option<&ast_refined::Access>,
    binding: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> bool {
    match rel {
        ast_refined::Relation::Ground { .. } => {
            matches!(access, Some(ast_refined::Access::Slots(_)))
                && crate::relation::contains_scope(identities, read, binding).unwrap_or(false)
        }
        ast_refined::Relation::InnerRelation { pattern, .. } => {
            use ast_refined::InnerRelationPattern as P;
            match pattern {
                P::Indeterminate { subquery, .. }
                | P::UncorrelatedDerivedTable { subquery, .. }
                | P::CorrelatedScalarJoin { subquery, .. }
                | P::CorrelatedGroupJoin { subquery, .. } => {
                    expr_has_positional_self_ref(subquery, binding, identities)
                }
            }
        }
        _ => false,
    }
}

/// Lower a single CTE binding to a SQL CTE, reconciling body columns with
/// the heading the binding publishes.
///
/// THE ONE TRANSITION from a decided binding to a SQL binding. What arrives
/// is one value the authority built — the body IS the decision — and what
/// leaves is one `Cte` carrying the same variant. There is nothing here to
/// split, nothing to pair, and no flavor to guess: this function knows only
/// how a chain becomes SQL.
pub(in crate::pipeline) fn lower_cte_binding(
    binding: ast_refined::CteBinding,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<crate::pipeline::sql_ast::Cte> {
    check_recursive_argumentative_binding(&binding, &ctx.identities)?;
    let cte_scope = *binding.subject();
    let materialized_once = ctx.relations.is_materialized_once(&cte_scope)?;
    let carries_hygiene = ctx
        .relations
        .interface(&cte_scope)?
        .ports()
        .iter()
        .any(|port| ctx.identities.addressing(port.column()) == crate::names::Addressing::Hygienic);
    // What a CTE publishes is its binding's heading — that is what every
    // reference through the name was addressed against. The body's own schema
    // answers only where the binding has none: reconciling to the body instead
    // leaves the CTE emitting occurrences its own name does not carry.
    crate::probe::probe!(
        recursion,
        "binding {cte_scope:?} {:?}",
        crate::probe::scope_chain(&ctx.identities, cte_scope.scope())
    );
    // ONE VALUE IN, ONE VALUE OUT. The binding says what it is and what it
    // stands on; this lowering says how a chain becomes SQL and nothing
    // else. It does not learn the variant, choose the anchor, hold an
    // accumulation, or name the scope the result binds — there is no
    // argument here for any of them.
    let cte = binding.into_sql(
        |anchor| {
            let builder = if carries_hygiene {
                super::descend::descend_as_query_carrying_hygiene(anchor, names, ctx)?
            } else {
                super::descend::descend_as_final(anchor, names, ctx)?
            };
            publish_relation_body(builder, &cte_scope, ctx)
        },
        |clause, published| {
            let builder = if carries_hygiene {
                super::descend::descend_as_query_carrying_hygiene(clause, names, ctx)?
            } else {
                super::descend::descend_as_final(clause, names, ctx)?
            };
            publish_fixpoint_member(builder, &cte_scope, published, ctx)
        },
    )?;
    Ok(if materialized_once {
        cte.requiring_materialization()
    } else {
        cte
    })
}

/// Re-alias one clause of a fixpoint onto the occurrences the binding
/// publishes.
///
/// EVERY PART OF A BODY EMITS THE BINDING'S OWN HEADING — that is what every
/// reader through the name addresses, and a part still emitting its own
/// leaves the binding claiming columns the statement under it does not
/// output. Pairing is POSITIONAL and is not a judgment: the recursion
/// authority already judged that the clauses publish one heading, and a
/// fixpoint accumulates by ordinal.
fn publish_fixpoint_member(
    builder: Builder<Projected>,
    cte: &crate::relation::SemanticRelation,
    published: &[ColumnMetadata],
    ctx: &TransformCtx,
) -> Result<crate::pipeline::sql_ast::QueryExpression> {
    if builder.columns().len() != published.len() {
        return Err(DelightQLError::parse_error(format!(
            "a fixpoint clause publishing {} positions cannot accumulate under a \
             binding publishing {}",
            builder.columns().len(),
            published.len()
        )));
    }
    let pairs: Vec<_> = builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .zip(published.iter().map(ColumnMetadata::identity))
        .collect();
    let mut query = builder.to_sql()?;
    super::builder::state::rewrite_output_aliases(
        &mut query,
        cte.scope(),
        &pairs,
        &ctx.identities,
    )?;
    Ok(query)
}

/// Publish a CTE body under the heading its name carries.
///
/// A CTE is the one boundary that costs no SQL: `WITH c AS (body)` outputs
/// exactly what the body outputs, so when the two headings line up slot for
/// slot the whole act is re-aliasing the body's outputs in place. Projecting
/// instead would be merely wasteful for most bodies and is fatal for one: a
/// projection over a UNION has to wrap it, and a wrapped union buries the
/// recursive member's self-reference where no engine will resolve it.
///
/// Anything that does not line up — a discarding caller pattern, a permuted
/// heading — is a real projection and goes the ordinary way.
/// Re-alias a physical wrap's body into the range the wrap is named by.
///
/// The wrap adds no semantic boundary: the relation inside it is the relation
/// outside it, and this act mints no port and consults no interface. What it
/// answers is a RANGE question — a derived table offers the columns of the
/// range it is aliased by, and the body's own occurrences belong to the level
/// beneath. Emitting them unchanged would leave every reader above qualifying
/// by a FROM entry the enclosing statement does not have.
fn requalify_physical_wrap(
    inner_builder: Builder<Projected>,
    scope: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<(
    crate::pipeline::sql_ast::QueryExpression,
    Vec<ColumnMetadata>,
)> {
    let columns = inner_builder.columns().to_vec();
    let mut query = inner_builder.to_sql()?;
    let republished =
        super::builder::republish_under(&mut query, scope, &columns, &ctx.identities)?;
    Ok((query, republished))
}

fn publish_relation_body(
    inner_builder: Builder<Projected>,
    cte: &crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<(
    crate::pipeline::sql_ast::QueryExpression,
    Vec<ColumnMetadata>,
)> {
    use crate::pipeline::sql_ast::{DomainExpression, SelectItem};

    let ports = ctx.relations.interface(cte)?.ports().to_vec();
    if ports.is_empty() {
        let columns = inner_builder.columns().to_vec();
        return Ok((inner_builder.to_sql()?, columns));
    }
    let carried = ctx.relations.carried_sources(cte)?;
    let mut sources = Vec::with_capacity(carried.len());
    for (_, input) in &carried {
        let [source] = input.as_slice() else {
            return Err(DelightQLError::parse_error(
                "a CTE output must carry exactly one body position",
            ));
        };
        sources.push(inner_builder.rebind_port(*source)?);
    }
    // WHAT THE BOUNDARY OWES CROSSES IT. A correlation carrier is a
    // dependency of this relation, so the body emits it beside the published
    // heading and the enclosing statement can still read it.
    let dependencies = ctx.relations.dependencies(cte)?;
    let mut support = Vec::with_capacity(dependencies.len());
    for dependency in &dependencies {
        support.push((
            inner_builder.rebind_port(*dependency)?,
            ctx.identities
                .sql_column(cte.scope(), None, crate::names::Addressing::Hygienic),
        ));
    }
    let outputs: Vec<_> = inner_builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .collect();
    if outputs != sources || !support.is_empty() {
        let mut items: Vec<_> = sources
            .into_iter()
            .zip(&ports)
            .map(|(source, output)| {
                SelectItem::expression_with_alias(DomainExpression::Column(source), output.column())
            })
            .collect();
        let mut columns: Vec<_> = ports
            .iter()
            .map(|port| ColumnMetadata::new(port.column()))
            .collect();
        for (source, alias) in support {
            items.push(SelectItem::expression_with_alias(
                DomainExpression::Column(source),
                alias,
            ));
            columns.push(ColumnMetadata::new(alias));
        }
        let published = inner_builder.add_projection_publishing(items, cte.scope(), columns)?;
        let columns = published.columns().to_vec();
        return Ok((published.to_sql()?, columns));
    }
    let pairs: Vec<_> = outputs
        .iter()
        .copied()
        .zip(ports.iter().map(|port| port.column()))
        .collect();
    let mut query = inner_builder.to_sql()?;
    super::builder::state::rewrite_output_aliases(
        &mut query,
        cte.scope(),
        &pairs,
        &ctx.identities,
    )?;
    let columns = ports
        .iter()
        .map(|port| ColumnMetadata::new(port.column()))
        .collect();
    Ok((query, columns))
}

/// Extract `Vec<ColumnMetadata>` from a scope, pushing a scope transition
/// onto each column's identity stack so the qualifier reflects the given scope.
///
/// This is the translation boundary: the scope the refiner bound flows in, and
/// the builder gets `Vec<ColumnMetadata>` with the identity stack updated to
/// reflect the current SQL scope. No information is discarded.
pub(super) fn columns_from_relation(
    relation: &crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Vec<ColumnMetadata>> {
    Ok(ctx
        .relations
        .interface(relation)?
        .ports()
        .iter()
        .map(|port| port.column())
        .into_iter()
        .map(ColumnMetadata::new)
        .collect())
}

fn relation_output_columns(
    relation: &crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Vec<crate::names::ColId>> {
    Ok(ctx
        .relations
        .interface(relation)?
        .ports()
        .iter()
        .map(|port| port.column())
        .collect())
}

/// The values a publication list computes. A spread computes none — it
/// enumerates — so it contributes nothing to a road that reads expressions.
pub(super) fn published_values(
    items: Vec<ast_refined::OutItem>,
) -> Vec<ast_refined::DomainExpression> {
    items.into_iter().filter_map(into_published_value).collect()
}

/// What a reduction publishes, for a lowering that owns its own output
/// schema: a computed value, or a metadata level.
///
/// The two are not one carrier because a metadata level is not an
/// expression — a lowering that cannot represent one says so instead of
/// receiving something it would have to classify.
pub(super) enum ReductionPayload {
    Value(ast_refined::DomainExpression),
    Metadata(ast_refined::MetadataGroup),
}

pub(super) fn published_reductions(
    items: Vec<ast_refined::ReductionItem>,
) -> Vec<ReductionPayload> {
    items
        .into_iter()
        .filter_map(|item| match item {
            ast_refined::ReductionItem::Out(item) => {
                into_published_value(item).map(ReductionPayload::Value)
            }
            ast_refined::ReductionItem::Metadata(metadata) => {
                Some(ReductionPayload::Metadata(metadata.group))
            }
            // A delegate lowers at the group's own delegate roads.
            ast_refined::ReductionItem::Delegate(_) => None,
            // A group holding a pivot is routed to the pivot lowering by
            // `r_lower_group`; reaching here is that routing failing.
            ast_refined::ReductionItem::Pivot(_) => {
                unreachable!("a pivot reduction reached the ordinary reduction road")
            }
        })
        .collect()
}

/// The values a reduction list publishes, where the position admits no
/// metadata level. A metadata group reaching one is a lowering that was
/// handed a shape it has no rendering for.
pub(super) fn published_reduction_values(
    items: Vec<ast_refined::ReductionItem>,
) -> Result<Vec<ast_refined::DomainExpression>> {
    published_reductions(items)
        .into_iter()
        .map(|payload| match payload {
            ReductionPayload::Value(value) => Ok(value),
            ReductionPayload::Metadata(_) => Err(DelightQLError::ParseError {
                message: "a metadata group stands in this reduction, which lowers values only"
                    .to_string(),
                source: None,
                subcategory: None,
            }),
        })
        .collect()
}

pub(super) fn into_published_value(
    item: ast_refined::OutItem,
) -> Option<ast_refined::DomainExpression> {
    match item {
        ast_refined::OutItem::One(one) => Some(one.expr),
        ast_refined::OutItem::Many(_) | ast_refined::OutItem::Whole => None,
    }
}

pub(super) fn alias_unaliased(
    item: &mut crate::pipeline::sql_ast::SelectItem,
    column: crate::names::ColId,
) {
    if item.printed_alias().is_none() {
        if let Some(named) = item.realizing(column) {
            *item = named;
        }
    }
}

/// Lower a restriction: add WHERE to the child builder.
///
/// Transparent — it passes through the child's scope. The `origin` records
/// where the filter came from (comma vs interior).
pub(super) fn r_lower_filter(
    child: Builder<Unprojected>,
    condition: ast_refined::TruthExpression,
    #[expect(
        unused_variables,
        reason = "all filter origins currently share one lowering"
    )]
    origin: ast_refined::FilterOrigin,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let (child, condition) = anchors::publishing_in_condition(child, condition, ctx)?;
    // The level the predicate lands on is settled BEFORE it is lowered: a
    // bound or a grouping under it gives the filter a level of its own, and
    // the references must name what that level emits.
    let child = child.ready_for_filter()?;
    // A HOISTED PREDICATE STILL NAMES WHAT IT NAMED. Refinement moves a
    // condition onto the relation it constrains, and the position it reads
    // may be an operand's or one this relation replaced. The
    // construction-recorded ancestry translates it; nothing searches.
    let predicate = {
        let ancestral = super::builder::AncestralQualify::over(&result, &ctx.relations, &child)?;
        scalar::s_lower_boolean(condition, &ancestral, ctx)?
    };
    child.add_where(predicate)
}

/// Lower the ARBITRARY row bound: `#<n` is LIMIT n, `#>n` its OFFSET.
/// THE OPERATOR SAYS WHICH BOUND IT IS.
///
/// `#<n` caps the rows; `#>n` says where the count starts and selects no
/// maximum. Both are bounds and both denote a relation, so each stands on
/// its own level unless the refiner has already composed a skip into the
/// cap that follows it. The bound an ordering consumed never arrives here:
/// it is the ordering's own node and `r_lower_order_by` emits it in the
/// ordering's scope.
pub(super) fn r_lower_bound(
    child: Builder<Unprojected>,
    bound: crate::pipeline::asts::core::TupleOrdinalClause,
) -> Result<Builder<Unprojected>> {
    row_clause(child, bound)
}

/// The row clause a bound spells, added to the level it bounds.
fn row_clause(
    level: Builder<Unprojected>,
    bound: crate::pipeline::asts::core::TupleOrdinalClause,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::asts::core::TupleOrdinalOperator;

    match bound.operator {
        TupleOrdinalOperator::LessThan => level.add_limit(bound.value, bound.offset),
        TupleOrdinalOperator::GreaterThan => level.add_offset(bound.value),
        // `#=` has no authored spelling: `row_bound` derives `#<` and `#>`
        // and nothing builds this arm.
        TupleOrdinalOperator::Exactly => Err(DelightQLError::transformation_error(
            "an exact row bound has no authored spelling",
            "row_bound",
        )),
    }
}

/// Lower a `Join` node: combine two builders into a single joined builder.
///
/// Prepares both sides as join operands FIRST (which may wrap complex states
/// as subqueries with generated aliases), then lowers the join condition
/// against the post-wrap scopes. This ensures the condition's qualifiers
/// match the SQL aliases that actually appear in the output.
pub(super) fn r_lower_join(
    left: Builder<Unprojected>,
    right: Builder<Unprojected>,
    correlation: ast_refined::MemberCorrelation,
    join_type: Option<ast_refined::JoinType>,
    result: crate::relation::SemanticRelation,
    emitted_swapped: bool,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{JoinCondition as SqlJoinCondition, JoinType as SqlJoinType};

    let sql_join_type = lower_join_type(join_type);

    // Prepare both sides — this may wrap Segment/Select/Frozen states as
    // subqueries, requalifying scope columns to the wrapper alias.
    let left_op = left.into_join_operand()?;
    let mut right_op = right.into_join_operand()?;

    // A TVF argument is a lateral reference into the left side, addressed
    // before the wrap that gave the left its FROM alias.
    right_op.resolve_tvf_args(&left_op)?;

    let combined = super::builder::ChainedQualify {
        inner: &left_op,
        outer: &right_op,
    };
    let operation = super::builder::AncestralQualify::over(&result, &ctx.relations, &combined)?;

    // Lower the join condition against the POST-WRAP scopes.
    // ChainedQualify lives in the builder module — the qualify logic stays
    // in one place instead of being reimplemented here.
    let condition = match correlation {
        ast_refined::MemberCorrelation::Correspond(correspondence) => {
            // THE PAIR NAMES THE OPERANDS THE RESOLVER SAW. Refinement
            // rebuilds a join, and the rebuilt operand publishes its own
            // positions; the ancestry the construction recorded is what
            // carries the recorded pair onto them. Each side asks over its
            // OWN operand, because a merged output has an ancestor in both
            // and asking over the pair would reach two columns for one.
            let left_ancestry =
                super::builder::AncestralQualify::over(&result, &ctx.relations, &left_op)?;
            let right_ancestry =
                super::builder::AncestralQualify::over(&result, &ctx.relations, &right_op)?;
            let mut merged = Vec::new();
            for pair in correspondence.pairs {
                // A merged pair names the EMITTED sides. A right outer join
                // is emitted with its operands exchanged, and the pair still
                // names the semantic left and right, so the exchange is
                // applied here rather than left for the physical map to
                // discover as one column standing at two positions.
                let (near, far) = if emitted_swapped {
                    (pair.right, pair.left)
                } else {
                    (pair.left, pair.right)
                };
                let left = left_ancestry.rebind_port(near)?;
                let right = right_ancestry.rebind_port(far)?;
                merged.push(crate::pipeline::sql_ast::MergedSlots { left, right });
            }
            if sql_join_type == SqlJoinType::Full {
                // Full outer must project merged columns as COALESCE —
                // either side's orphan rows carry the key alone.
                return Builder::from_join_full_outer_merge(left_op, right_op, merged)?.demote();
            }
            SqlJoinCondition::Merge(merged)
        }
        ast_refined::MemberCorrelation::Condition(bool_expr) => {
            let pred = scalar::s_lower_boolean(bool_expr, &operation, ctx)?;
            SqlJoinCondition::On(pred.into_expr())
        }
        // The semantic step carries the deliberate cross itself; nothing
        // here may manufacture one from an absence.
        ast_refined::MemberCorrelation::Cartesian(()) => SqlJoinCondition::Cartesian,
    };

    Builder::from_join(left_op, right_op, sql_join_type, condition, emitted_swapped)
}

/// Lower a join where the right side is an anonymous table.
///
/// When the anonymous table's row data contains column references (e.g.,
/// `u.first_name`), those references are correlated — they refer to the
/// left-side scope. A plain UNION ALL subquery can't reference outer scope
/// in SQL (no LATERAL support in SQLite).
///
/// Strategy:
/// - No column refs → fall through to normal `r_lower_anonymous` + `r_lower_join`
/// - Has column refs → JSON melt: pack row values into a `json_array()`
///   expression evaluated in the left scope, push as CTE, expand with
///   `json_each`, extract columns with `json_extract`
pub(super) fn r_lower_join_anonymous(
    left: Builder<Unprojected>,
    anon: ast_refined::AnonRelation,
    anon_relation: crate::relation::SemanticRelation,
    correlation: ast_refined::MemberCorrelation,
    join_type: Option<ast_refined::JoinType>,
    result: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let ast_refined::AnonRelation { table, alias, .. } = anon;
    let rows = table.body.rows;
    let anonymous_relation = anon_relation;

    // Check if any row value contains a column reference.
    let has_column_refs = rows
        .iter()
        .any(|row| row.iter().any(|v| contains_column_reference(&v.value())));

    if !has_column_refs {
        // No correlated refs — use normal UNION ALL path.
        let right = r_lower_anonymous(rows, anonymous_relation, names, ctx)?;
        return r_lower_join(left, right, correlation, join_type, result, false, ctx);
    }

    // --- JSON melt path ---
    r_lower_melt_join(
        left,
        rows,
        alias,
        anonymous_relation,
        correlation,
        join_type,
        result,
        names,
        ctx,
    )
}

/// Build a JSON melt: pack correlated anonymous-table rows into a json_array
/// on the left side, then expand with json_each + json_extract.
fn r_lower_melt_join(
    left: Builder<Unprojected>,
    rows: crate::pipeline::asts::vocabulary::Vec1<
        crate::pipeline::asts::core::TabularRow<crate::pipeline::asts::core::Datum<Refined>>,
    >,
    _alias: Option<delightql_types::SqlIdentifier>,
    anonymous_relation: crate::relation::SemanticRelation,
    correlation: ast_refined::MemberCorrelation,
    join_type: Option<ast_refined::JoinType>,
    result: crate::relation::SemanticRelation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, JoinCondition as SqlJoinCondition, QueryExpression,
        SelectItem, SelectStatement,
    };

    let source_metadata = left.columns().to_vec();
    let input_scope = ColumnMetadata::common_identity_scope(&source_metadata, &ctx.identities)
        .unwrap_or_else(|| ctx.identities.anonymous_scope(None));
    // The packet is an output of this projection, not an extra output of the
    // relation being read. Owning it by the input mutates that input's
    // heading; a second melt over the same occurrence then mistakes the first
    // packet for caller data and emits a reference no input table publishes.
    let packet_scope = ctx
        .identities
        .wrap_scope(input_scope, crate::names::WrapReason::Pivot);
    let packet = ctx
        .identities
        .sql_column(packet_scope, None, crate::names::Addressing::Hygienic);
    let row_exprs = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|datum| scalar::s_lower_expression(datum.value(), &left, ctx))
                .collect::<Result<Vec<_>>>()
                .map(|values| SqlDomainExpr::function("json_array", values))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut source_items: Vec<_> = source_metadata.iter().map(passthrough_item).collect();
    source_items.push(SelectItem::Publishing {
        expr: SqlDomainExpr::function("json_array", row_exprs),
        slot: packet,
        printed: true,
    });
    let projected = left.add_projection(source_items)?;
    let projected_columns = projected.columns().to_vec();
    let projected_packet = projected_columns
        .last()
        .map(ColumnMetadata::identity)
        .ok_or_else(|| DelightQLError::ParseError {
            message: "melt packet projection produced no column".to_string(),
            source: None,
            subcategory: None,
        })?;
    let source_origin = wrap_origin(
        &projected_columns,
        &ctx.identities,
        crate::names::WrapReason::Pivot,
    );
    let source_scope = names.fresh(source_origin).identity();
    let mut source_query = projected.to_sql()?;
    let wrapped_columns: Vec<_> = super::builder::republish_under(
        &mut source_query,
        source_scope,
        &projected_columns,
        &ctx.identities,
    )?
    .into_iter()
    .map(|column| column.identity())
    .collect();
    let wrapped_packet = *wrapped_columns.last().expect("packet column exists");

    let tvf_scope = names.interior_emission(projected_packet).identity();
    let value_spelling = ctx.identities.intern("value", false);
    let value_column = ctx.identities.sql_column(
        tvf_scope,
        Some(value_spelling),
        crate::names::Addressing::Published,
    );

    let output_ids = relation_output_columns(&result, ctx)?;
    let melt_ids = relation_output_columns(&anonymous_relation, ctx)?;
    let melt_metadata: Vec<_> = melt_ids.iter().copied().map(ColumnMetadata::new).collect();
    // The predicate is addressed against the logical join heading. The SQL
    // FROM below exposes neither half under those identities: the left half
    // has crossed a wrapper, and the right half exists only as extraction
    // expressions. Lower first against the complete logical heading, then
    // replace every logical occurrence with the expression its FROM exposes.
    let mut condition_columns = source_metadata.clone();
    condition_columns.extend(melt_metadata);
    // The logical heading IS the join's interface, position for position, so
    // the predicate's references are answered by the join's own binding
    // rather than by a search over loose columns. A zero-width anonymous
    // table keeps its cells OUT of that interface; the condition still
    // reads them, so they ride the site as support — realized below as
    // extraction expressions, never published.
    let logical_site = {
        let mut row = ctx
            .identities
            .bindings()
            .emitting(&ctx.relations, &result)?;
        let interface: Vec<_> = ctx.relations.interface(&result)?.ports().to_vec();
        for port in &interface {
            row.publishes(port.column(), *port)?;
        }
        for port in crate::relation::published_ports(&ctx.identities, &anonymous_relation)? {
            if !interface.contains(&port) {
                row.supports(port.column(), port)?;
            }
        }
        row.close(&ctx.relations)?
    };
    let condition_qualify = MeltJoinQualify {
        columns: condition_columns,
        site: logical_site,
        identities: &ctx.identities,
    };

    let mut lowered_condition = match correlation {
        ast_refined::MemberCorrelation::Correspond(_) => {
            return Err(DelightQLError::validation_error_categorized(
                "transform/melt-join/using",
                "a correlated anonymous join cannot lower an implicit USING condition",
                "write an explicit predicate between the left and anonymous columns",
            ));
        }
        ast_refined::MemberCorrelation::Condition(condition) => {
            scalar::s_lower_boolean(condition, &condition_qualify, ctx)?.into_expr()
        }
        ast_refined::MemberCorrelation::Cartesian(()) => {
            SqlDomainExpr::literal(crate::pipeline::asts::core::LiteralValue::Boolean(true))
        }
    };

    let mut replacements = std::collections::HashMap::new();
    // `projected` is constructed as every source column followed by exactly
    // one packet, and `republish_under` preserves that order. Taking the first
    // source-width entries therefore enumerates the complete left heading.
    // The predicate names the join's OWN positions, so the map that carries
    // it onto the FROM below is keyed on those.
    for (position, wrapped) in wrapped_columns
        .iter()
        .copied()
        .take(source_metadata.len())
        .enumerate()
    {
        if let Some(output) = output_ids.get(position) {
            replacements.insert(*output, SqlDomainExpr::Column(wrapped));
        }
    }
    let mut select_items = Vec::new();
    for (position, column) in wrapped_columns
        .iter()
        .take(source_metadata.len())
        .enumerate()
    {
        select_items.push(match output_ids.get(position).copied() {
            Some(output) => {
                SelectItem::expression_with_alias(SqlDomainExpr::Column(*column), output)
            }
            None => SelectItem::bare_column(*column),
        });
    }
    for (position, column) in melt_ids.iter().enumerate() {
        let extracted = SqlDomainExpr::function(
            "json_extract",
            vec![
                SqlDomainExpr::Column(value_column),
                SqlDomainExpr::literal(crate::pipeline::asts::core::LiteralValue::String(format!(
                    "$[{position}]"
                ))),
            ],
        );
        // A zero-width anonymous table gives this cell no slot in the join
        // heading: the extraction is spent entirely inside the condition,
        // and selecting it would emit a column the wrap above has no
        // target for.
        match output_ids.get(source_metadata.len() + position).copied() {
            Some(output) => {
                replacements.insert(output, extracted.clone());
                select_items.push(SelectItem::expression_with_alias(extracted, output));
            }
            None => {
                replacements.insert(*column, extracted);
            }
        }
    }
    replace_melt_join_columns(&mut lowered_condition, &replacements);
    let output_scope = result.scope();
    let columns = columns_from_relation(&result, ctx)?;
    let select = (SelectStatement::builder()
        .set_select(select_items)
        .from_tables(vec![TableExpression::Join {
            left: Box::new(TableExpression::subquery(source_query, source_scope)),
            right: Box::new(json_each_tvf(wrapped_packet, tvf_scope, &ctx.identities)),
            join_type: lower_join_type(join_type),
            join_condition: SqlJoinCondition::On(lowered_condition),
        }]))
    .standing_at(output_scope)
    .map_err(crate::error::DelightQLError::parse_error)?;
    Builder::from_query(
        QueryExpression::Select(Box::new(select)),
        ScopeName::Resolved(output_scope),
        columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
    )?
    .demote()
}

struct MeltJoinQualify<'a> {
    columns: Vec<ColumnMetadata>,
    site: crate::sql_binding::SqlSiteId,
    identities: &'a crate::names::Registry,
}

impl Qualify for MeltJoinQualify<'_> {
    fn identities(&self) -> &crate::names::Registry {
        self.identities
    }

    crate::pipeline::transformer::builder::qualifies_by_emitting!();

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.columns.clone()
    }
}

impl crate::pipeline::transformer::builder::Emitting for MeltJoinQualify<'_> {
    fn site(&self) -> crate::sql_binding::SqlSiteId {
        self.site
    }
}

fn replace_melt_join_columns(
    expression: &mut crate::pipeline::sql_ast::DomainExpression,
    replacements: &std::collections::HashMap<
        crate::names::ColId,
        crate::pipeline::sql_ast::DomainExpression,
    >,
) {
    struct ReplaceColumns<'a>(
        &'a std::collections::HashMap<
            crate::names::ColId,
            crate::pipeline::sql_ast::DomainExpression,
        >,
    );

    impl crate::pipeline::sql_ast::walk::SqlVisitorMut for ReplaceColumns<'_> {
        fn expr(&mut self, expression: &mut crate::pipeline::sql_ast::DomainExpression) {
            let crate::pipeline::sql_ast::DomainExpression::Column(column) = expression else {
                return;
            };
            if let Some(replacement) = self.0.get(column) {
                *expression = replacement.clone();
            }
        }
    }

    crate::pipeline::sql_ast::walk::visit_expression_mut(
        expression,
        &mut ReplaceColumns(replacements),
    );
}

/// Check if a domain expression contains column references.
///
/// Any Lvar counts — unqualified Lvars in melt rows are correlated
/// references to the left-side scope (e.g., `json` in `json:{.path}`).
/// False positives are harmless: the melt/json_each path is functionally
/// correct for non-correlated rows too, just slightly less optimal SQL.
///
/// A relation standing beneath the value is its own scope and may read the
/// left-side scope from inside; the walk's scope judgment answers for every
/// value alike before any shape is read.
fn contains_column_reference(expr: &ast_refined::DomainExpression) -> bool {
    if expr.nests_relation() {
        return true;
    }
    match expr {
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { .. },
        ))) => true,
        ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Ground(_)) => {
            false
        }
        ast_refined::DomainExpression::Application(func) => {
            match func {
                crate::pipeline::asts::core::FunctionApplication::Standard(application) => {
                    application
                        .call()
                        .arguments
                        .scalar_members()
                        .iter()
                        .any(|member| {
                            member
                                .scalar_domain()
                                .is_some_and(contains_column_reference)
                        })
                }
                crate::pipeline::asts::core::FunctionApplication::Infix(infix) => {
                    contains_column_reference(&infix.left)
                        || contains_column_reference(&infix.right)
                }
                crate::pipeline::asts::core::FunctionApplication::JsonAccess(access) => {
                    contains_column_reference(&access.source)
                }
                crate::pipeline::asts::core::FunctionApplication::Enclyph(enclyph) => {
                    use crate::pipeline::asts::core::{Enclyph, RecordMember};
                    match enclyph {
                        Enclyph::Record(record) => {
                            record.members.iter().any(|member| match member {
                                RecordMember::SelfKeyed(_) | RecordMember::Metadata { .. } => true,
                                RecordMember::Keyed { value, .. } => {
                                    contains_column_reference(value)
                                }
                                // An induced level reads its own source, and a
                                // spread is spent before this phase.
                                RecordMember::Induced { .. } | RecordMember::Spread(_) => false,
                            })
                        }
                        Enclyph::EmptyRecord(_) => false,
                        Enclyph::Tuple(tuple) => tuple
                            .elements
                            .iter()
                            .any(|element| contains_column_reference(element.value())),
                    }
                }
                // A crossed truth reads what its truth reads.
                crate::pipeline::asts::core::FunctionApplication::Crossed(crossing) => crossing
                    .truth()
                    .scalar_operands()
                    .into_iter()
                    .any(contains_column_reference),
                _ => false,
            }
        }
        _ => false,
    }
}

/// Qualify implementation for contexts with no scope (anonymous table rows).
///
/// All columns come back unqualified — anonymous rows contain only literals
/// and expressions that don't reference any table columns.
struct DummyQualify<'a>(&'a crate::names::Registry);

impl Qualify for DummyQualify<'_> {
    fn identities(&self) -> &crate::names::Registry {
        self.0
    }

    // AN ANONYMOUS ROW EMITS NOTHING TO NAME. Its cells are literals and
    // computed values; there is no site under them and no column a
    // reference could mean. It says so here, in its own words, rather than
    // answering `None` to a question every scope used to be asked.
    fn rebind_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId> {
        Err(no_emission(port))
    }

    fn slot_of_port(&self, port: crate::relation::PortId) -> Result<usize> {
        Err(no_emission(port))
    }

    fn slot_of_physical(&self, column: crate::names::ColId) -> Result<usize> {
        Err(DelightQLError::parse_error(format!(
            "physical column {column:?} was looked for in an anonymous row, which emits none"
        )))
    }
}

fn no_emission(port: crate::relation::PortId) -> DelightQLError {
    DelightQLError::parse_error(format!(
        "semantic port {port:?} was looked for in an anonymous row, which emits no column"
    ))
}

/// The two exact physical sites named by one set correlation.
///
/// Correlation is the only scalar context here: its semantic carrier names
/// both arms, and each lowered arm was bound to its emitted site in the same
/// act that laid it out. A port must therefore occur at exactly one of those
/// sites; there is no column-level replacement or heading search to fall back
/// to.
struct ArmPairQualify<'a> {
    identities: &'a crate::names::Registry,
    sites: [crate::sql_binding::SqlSiteId; 2],
}

impl Qualify for ArmPairQualify<'_> {
    fn identities(&self) -> &crate::names::Registry {
        self.identities
    }

    fn rebind_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId> {
        let matches = self
            .sites
            .iter()
            .filter_map(|site| self.identities.bindings().at(*site, port).ok())
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [column] => Ok(*column),
            [] => Err(DelightQLError::parse_error(
                "a set correlation names a port neither exact arm emits",
            )),
            _ => Err(DelightQLError::parse_error(
                "a set correlation names a port both exact arms emit",
            )),
        }
    }

    fn sql_sites(&self) -> Vec<crate::sql_binding::SqlSiteId> {
        self.sites.to_vec()
    }

    // A CORRELATION NAMES ONE OF TWO ARMS. Which one is the answer
    // `rebind_port` gives; a POSITION is not, because the two arms lay
    // their columns out independently and there is no one ordinal a
    // reference could mean.
    fn slot_of_port(&self, port: crate::relation::PortId) -> Result<usize> {
        Err(DelightQLError::parse_error(format!(
            "semantic port {port:?} was asked for a position across a set-arm pair, \
             which lays out two"
        )))
    }

    fn slot_of_physical(&self, column: crate::names::ColId) -> Result<usize> {
        Err(DelightQLError::parse_error(format!(
            "physical column {column:?} was asked for a position across a set-arm pair, \
             which lays out two"
        )))
    }
}

/// Lower a pipe chain: left-fold segments over a base builder.
///
/// The fold starts with `Builder<Unprojected>` (the base) and produces
/// `Builder<Projected>` (the last segment must set a SELECT list).
/// Emit a caller pattern's own ordered interface.
///
/// The published ports come first, each reading the operand position the
/// authority recorded it as carrying. The dependencies follow as physical
/// support — a constrained position is still read by the predicate standing
/// behind the access — under fresh hygienic aliases, so the wrap above the
/// predicate prunes them and no discarded position reaches the output. A
/// position the pattern neither publishes nor constrains is neither.
fn r_lower_access(
    builder: Builder<Unprojected>,
    relation: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let ports = ctx.relations.interface(&relation)?.ports().to_vec();
    let dependencies = ctx.relations.dependencies(&relation)?;
    let mut items = Vec::with_capacity(ports.len() + dependencies.len());
    let mut columns = Vec::with_capacity(ports.len() + dependencies.len());
    {
        // The pattern's own construction stands between the chain's relation
        // and this one — an answering-name export, the access itself — and
        // none of those intermediate occurrences is emitted. The recorded
        // ancestry is what translates a published port onto the operand
        // position the site actually binds.
        let ancestral =
            super::builder::AncestralQualify::over(&relation, &ctx.relations, &builder)?;
        for port in ports {
            items.push(crate::pipeline::sql_ast::SelectItem::Publishing {
                expr: crate::pipeline::sql_ast::DomainExpression::Column(
                    ancestral.rebind_port(port)?,
                ),
                slot: port.column(),
                printed: true,
            });
            columns.push(ColumnMetadata::new(port.column()));
        }
        for port in dependencies {
            let support = ctx.identities.sql_column(
                relation.scope(),
                None,
                crate::names::Addressing::Hygienic,
            );
            items.push(crate::pipeline::sql_ast::SelectItem::Publishing {
                expr: crate::pipeline::sql_ast::DomainExpression::Column(
                    ancestral.rebind_port(port)?,
                ),
                slot: support,
                printed: true,
            });
            columns.push(ColumnMetadata::new(support));
        }
    }
    if items.is_empty() {
        items.push(crate::pipeline::sql_ast::SelectItem::star_over_nothing());
    }
    builder
        .add_projection_publishing(items, relation.scope(), columns)?
        .bind_relation(relation, &ctx.relations)
}

pub(super) fn r_lower_pipe(
    base: Builder<Unprojected>,
    segments: Vec<PipeSegment<Refined>>,
    #[expect(
        unused_variables,
        reason = "pipe-segment lowerings receive naming through the transform context"
    )]
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::asts::core::operators::PipeOp;

    // If no segments, just project_all (SELECT *)
    if segments.is_empty() {
        return base.project_all();
    }

    // Left-fold: each segment transforms Unprojected → Projected.
    // Between segments, demote Projected → Unprojected for the next one.
    let mut current: Builder<Unprojected> = base;

    let last_idx = segments.len() - 1;
    for (i, segment) in segments.into_iter().enumerate() {
        let PipeSegment { step, result } = segment;
        let operation_relation = result;
        let operator = match step {
            PipeStep::Operator(operator) => operator,
            // AN ACCESS PUBLISHES ITS OWN INTERFACE. A caller pattern names
            // the positions it binds, discards the rest, and keeps the ones
            // a constraint still reads as physical support — so the emitted
            // list is the authority's ports followed by its dependencies,
            // never the operand's heading passed through.
            PipeStep::Access => {
                let result = r_lower_access(current, operation_relation, ctx)?;
                if i == last_idx {
                    return Ok(result);
                }
                current = result.demote()?;
                continue;
            }
            PipeStep::Structural(step) => {
                // A value a structural step must name more than once — an
                // anchored case's computed anchor in an ordering — is
                // published by the level below it, exactly as an operator's.
                let (published, step) = anchors::publishing_in_structural_step(current, step, ctx)?;
                current = published;
                let result: Builder<Projected> = match step.form {
                    ast_refined::StructuralForm::Ordering { specs, bound } => {
                        r_lower_order_by(current, specs, bound, result.clone(), ctx)?
                    }
                    ast_refined::StructuralForm::Reposition { .. } => {
                        r_lower_reposition(current, result.clone(), ctx)?
                    }
                    ast_refined::StructuralForm::Meta => r_lower_meta_ize(current, result, ctx)?,
                    ast_refined::StructuralForm::Witness { polarity } => {
                        r_lower_witness(current, polarity, result, ctx)?
                    }
                    ast_refined::StructuralForm::SignedWitness => {
                        r_lower_signed_witness(current, result, ctx)?
                    }
                    ast_refined::StructuralForm::Drill { drill } => r_lower_interior_drill_down(
                        current,
                        drill.column,
                        drill.columns,
                        drill.groundings,
                        result,
                        ctx,
                    )?,
                    ast_refined::StructuralForm::Narrow {
                        nest,
                        pattern,
                        schema,
                    } => {
                        r_lower_narrowing_destructure(current, nest, pattern, &schema, result, ctx)?
                    }
                }
                .bind_relation(operation_relation, &ctx.relations)?;
                if i == last_idx {
                    return Ok(result);
                }
                current = result.demote()?;
                continue;
            }
        };
        // A value this operator must name more than once is published by the
        // level below it, and the operator reads a column instead.
        let (published, operator) = anchors::publishing_in_operator(current, operator, ctx)?;
        current = published;
        let result: Builder<Projected> = match operator {
            PipeOp::Project(items) => {
                r_lower_projection(current, items, Some(result.clone()), ctx)?
            }

            // Extension IS projection at this level: the resolved items
            // already carry the operand's expanded heading in front of the
            // added columns, so the two lower through one road.
            PipeOp::Embed(items) => r_lower_projection(current, items, Some(result.clone()), ctx)?,

            PipeOp::ProjectOut(selector) => {
                r_lower_project_out(current, selector, result.clone(), ctx)?
            }

            PipeOp::Rename(specs) => r_lower_rename_cover(current, specs, result.clone(), ctx)?,

            PipeOp::Group(spec) => r_lower_group(current, spec, result.clone(), ctx)?,

            PipeOp::Transform {
                items: transformations,
                guard: conditioned_on,
                ..
            } => r_lower_transform(
                current,
                transformations,
                conditioned_on,
                result.clone(),
                ctx,
            )?,

            PipeOp::MapCover(MapCover { guard, cells, .. }) => {
                r_lower_map_cover(current, cells, guard, result.clone(), ctx)?
            }

            PipeOp::EmbedMapCover(EmbedMapCover { cells, .. }) => {
                r_lower_embed_map(current, cells, result.clone(), ctx)?
            }
        }
        .bind_relation(operation_relation, &ctx.relations)?;

        // The stage's resolver-stamped schema knows tree-typedness the
        // structural re-derivation above cannot; adopt it so a later
        // stage embedding this column can re-splice it.
        let result = result;

        if i == last_idx {
            return Ok(result);
        }
        // Demote for next segment
        current = result.demote()?;
    }

    unreachable!("segments is non-empty")
}

/// Lower a bag operation with no correlation on any arm.
pub(super) fn r_lower_set_op(
    operands: Vec<SetArm>,
    operator: ast_refined::SetOperator,
    steps: &[crate::relation::SemanticRelation],
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    match operator {
        // The name-aligned operators shape each arm to the output heading:
        // corresponding pads what an arm lacks, smart reorders. Both read
        // the binding the branches were laid out under rather than matching
        // names again.
        ast_refined::SetOperator::UnionCorresponding | ast_refined::SetOperator::SmartUnionAll => {
            r_lower_correlated_set_op(operands, operator, Vec::new(), steps, ctx)
        }
        // It still BINDS: a reference to a positional result's own position
        // has to be told which emitted column stands under it, and stacking
        // the branches is exactly the act that decides.
        // POSITIONAL EMITS NO ITEM LIST TO DERIVE: arms stand in ordinal
        // order and the branches stack as they are.
        ast_refined::SetOperator::UnionAllPositional => {
            let accumulation = crate::pipeline::sql_ast::SetOperator::UnionAll;
            let layouts: Vec<_> = operands.iter().map(SetArm::as_it_stands).collect();
            ctx.identities
                .bindings()
                .bind_run(&ctx.identities, steps, &layouts)?;
            let mut iter = operands.into_iter().map(SetArm::into_builder);
            let mut accumulated = iter.next().ok_or_else(|| empty_run())?;
            // Each stack publishes ITS OWN step's positions: `a || b || c` is
            // two steps, and the inner one is a result in its own right.
            for (step, next) in steps.iter().zip(iter) {
                let outputs = relation_output_columns(step, ctx)?;
                accumulated =
                    accumulated.stack_at(next, accumulation.clone(), step.scope(), &outputs)?;
            }
            Ok(accumulated)
        }
        // Minus reaches lowering with its correlation filled in — a bare
        // minus IS the whole-tuple anti-semijoin, and that is where the
        // predicate is written. There is no set-difference capability to
        // fall back to.
        ast_refined::SetOperator::MinusCorresponding => Err(DelightQLError::parse_error(
            "minus reached lowering without its anti-semijoin correlation",
        )),
    }
}

/// A bag run has one step per operator and one operand per arm.
fn empty_run() -> DelightQLError {
    DelightQLError::parse_error("a bag run reached lowering with no step to lower")
}

/// Shape one branch to the operation's output heading, from the binding
/// the branches were laid out under.
///
/// NOT a judgment of any kind. The binding already says, position by
/// position, what this branch emits there: a physical column of its own, or
/// the typed null a padding stands for. Deciding it again here — by
/// matching names, by walking lineage, or by taking the one candidate that
/// happens to be left — would be a second authority over one fact.
fn align_arm_items(
    operator: ast_refined::SetOperator,
    binding: crate::sql_binding::RunBinding,
    arm: usize,
    ctx: &TransformCtx,
) -> Result<Vec<crate::pipeline::sql_ast::SelectItem>> {
    use crate::pipeline::asts::core::LiteralValue;
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};
    use crate::sql_binding::SqlOutput;

    // THE BINDING IS TOTAL OVER WHAT THIS SET EMITS. The positions it
    // covers must be the positions being emitted, in that order — a select
    // list longer, shorter or otherwise ordered is not the one this binding
    // describes, and emitting it anyway would alias a branch's columns onto
    // positions nobody bound.
    ctx.identities
        .bindings()
        .branch(binding, arm)?
        .iter()
        .map(|(port, cell)| {
            let output = &port.column();
            match cell {
                SqlOutput::Slot(slot) => Ok(SelectItem::expression_with_alias(
                    SqlDomainExpr::Column(slot.column()),
                    *output,
                )),
                // A typed NULL pad — `cast(NULL, t)`, not a bare NULL.
                // Postgres resolves union types pairwise, so two untyped pad
                // branches collapse the column to text before a typed branch
                // arrives.
                SqlOutput::Pad(_) => {
                    debug_assert!(
                        matches!(operator, ast_refined::SetOperator::UnionCorresponding),
                        "only a corresponding set pads; the exact modes refuse before construction"
                    );
                    let null = SqlDomainExpr::literal(LiteralValue::Null);
                    let pad = match ctx.identities.facts(*output).declared_type {
                        Some(type_name) => SqlDomainExpr::cast(null, type_name),
                        None => null,
                    };
                    Ok(SelectItem::Publishing {
                        expr: pad,
                        slot: *output,
                        printed: true,
                    })
                }
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Pipe-segment handlers (called from r_lower_pipe)
// ---------------------------------------------------------------------------

/// Lower a projection: `|> (col1, col2)`.
///
/// Sets the SELECT list, transitioning Unprojected → Projected.
///
/// When `result` is provided, uses it to fill in aliases for select items
/// that don't have one (e.g., JSON path expressions where the AST node carries
/// no alias but the refiner has computed one).
pub(super) fn r_lower_projection(
    builder: Builder<Unprojected>,
    publication: crate::pipeline::asts::vocabulary::Vec1<ast_refined::OutItem>,
    result: Option<crate::relation::SemanticRelation>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    // THE WHOLE OPERAND EXPANDS HERE, into one item per operand position.
    // A star is a hole in the emitted list: the site binds ports to the
    // columns a statement selects, and a star selects a list nobody stated.
    // The expansion carries the operand's hygienic carriers too — an
    // internal column may be CARRIED, it may not be NAMED by an author, and
    // the guard below is the author's.
    let mut items = Vec::with_capacity(publication.len());
    for item in publication.into_vec() {
        if matches!(item, ast_refined::OutItem::Whole) {
            for column in builder.columns() {
                // SUPPORT IS NOT PART OF THE WHOLE. A position the operand
                // emits to pay a debt is not one of its dimensions, so a
                // star over the operand does not enumerate it.
                {
                    let site = builder.publication().site();
                    if ctx
                        .identities
                        .bindings()
                        .is_support(site, column.identity())?
                    {
                        continue;
                    }
                }
                items.push(crate::pipeline::sql_ast::SelectItem::Scaffolding {
                    slot: ctx.identities.scaffolding_slot(),
                    expr: crate::pipeline::sql_ast::DomainExpression::Column(column.identity()),
                });
            }
            continue;
        }
        let lowered = scalar::s_lower_out_item(item, &builder, ctx)?;
        // A resolved reference to a hygienic position is construction-owned:
        // authored resolution cannot name one. It may feed a temporary
        // compiler position or continue as hidden physical row support.
        items.push(lowered);
    }

    project_publishing_resolved(builder, items, result, ctx)
}

/// Set a pipe segment's SELECT list, publishing the scope the resolver bound
/// the segment to whenever the lowered list lines up with it slot for slot.
///
/// Every reference downstream of a segment was addressed against the
/// occurrences the resolver published for it; a freshly minted set answers to
/// none of them. Lining up is the whole condition — a list that dropped a slot
/// it could not place, or that carries a glob or a hygiene column, is not the
/// resolver's heading and mints as before.
fn project_publishing_resolved(
    builder: Builder<Unprojected>,
    mut items: Vec<crate::pipeline::sql_ast::SelectItem>,
    result: Option<crate::relation::SemanticRelation>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    if let Some(relation) = result {
        let ports = ctx.relations.interface(&relation)?.ports().to_vec();
        if ports.len() != items.len() {
            return Err(DelightQLError::parse_error(
                "a lowered semantic operation emitted a different number of positions than its interface",
            ));
        }
        for (item, port) in items.iter_mut().zip(&ports) {
            let Some(realized) = item.realizing(port.column()) else {
                return Err(DelightQLError::parse_error(
                    "a semantic output position reached lowering as an unexpanded star",
                ));
            };
            *item = realized;
        }
        let mut columns: Vec<_> = ports
            .iter()
            .map(|port| ColumnMetadata::new(port.column()))
            .collect();
        // WHAT THE OPERATION STILL OWES IS EMITTED, NOT PUBLISHED. A
        // dependency is a position of the OPERAND a later operation reads;
        // it rides beside the heading as physical support under a name
        // nothing addresses.
        for dependency in ctx.relations.dependencies(&relation)? {
            let support = ctx.identities.sql_column(
                relation.scope(),
                None,
                crate::names::Addressing::Hygienic,
            );
            items.push(crate::pipeline::sql_ast::SelectItem::Publishing {
                expr: crate::pipeline::sql_ast::DomainExpression::Column(
                    builder.rebind_port(dependency)?,
                ),
                slot: support,
                printed: true,
            });
            columns.push(ColumnMetadata::new(support));
        }
        return builder.add_projection_publishing(items, relation.scope(), columns);
    }

    builder.add_projection(items)
}

/// The same rule for a reducing segment, whose select list is keys then
/// aggregates and whose GROUP BY clause is read off the keys.
fn group_by_publishing_resolved(
    builder: Builder<Unprojected>,
    mut spec: super::builder::GroupBySpec,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let key_count = spec.keys.len();
    let mut items = spec.keys;
    items.append(&mut spec.aggregates);
    let ports = ctx.relations.interface(&result)?.ports().to_vec();
    if ports.len() != items.len() {
        return Err(DelightQLError::parse_error(
            "a grouped operation emitted a different number of positions than its interface",
        ));
    }
    for (item, port) in items.iter_mut().zip(&ports) {
        let Some(realized) = item.realizing(port.column()) else {
            return Err(DelightQLError::parse_error(
                "a grouped semantic position reached lowering as an unexpanded star",
            ));
        };
        *item = realized;
    }
    let aggregates = items.split_off(key_count);
    let columns = ports
        .into_iter()
        .map(|port| ColumnMetadata::new(port.column()))
        .collect();
    builder.add_group_by_publishing(
        super::builder::GroupBySpec {
            keys: items,
            aggregates,
        },
        result.scope(),
        columns,
    )
}

/// Lower ORDER BY: `|> #(col1, col2 descending)` — and, when the ordering
/// carries the bound that consumed it, `#(col), #<n` as ONE query scope.
///
/// Adds the ORDER BY terms and then the bound's row clause to the SAME
/// level, then projects all at the scope the resolver bound to the segment.
/// That one scope is the membership act: `ORDER BY … LIMIT n` in one block
/// is what the standard promises, where an ordering carried through a
/// derived table is an engine courtesy — and a later presentation ordering
/// then stands over the chosen members and cannot replace them.
///
/// Leaving the heading unchanged is not the same as standing at the input's
/// scope: the segment has one of its own, and every reference downstream of it
/// was addressed against that scope's occurrences.
pub(super) fn r_lower_order_by(
    builder: Builder<Unprojected>,
    specs: Vec<ast_refined::OrderingSpec>,
    bound: Option<crate::pipeline::asts::core::TupleOrdinalClause>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{OrderDirection as SqlDir, OrderTerm};

    // Expressions must be lowered against the post-wrap scope: a frozen body
    // wraps when the terms are added, and terms named against the pre-wrap
    // level would name aliases the statement no longer emits.
    let builder = builder.ensure_not_frozen()?;

    let terms: Vec<OrderTerm> = specs
        .into_iter()
        .map(|spec| {
            let expr = scalar::s_lower_expression(spec.column, &builder, ctx)?;
            let dir = spec.direction.map(|d| match d {
                ast_refined::OrderDirection::Ascending => SqlDir::Asc,
                ast_refined::OrderDirection::Descending => SqlDir::Desc,
            });
            Ok(OrderTerm::new(expr, dir))
        })
        .collect::<Result<_>>()?;

    let ordered = builder.add_order_by(terms)?;
    let bounded = match bound {
        Some(bound) => row_clause(ordered, bound)?,
        None => ordered,
    };
    let (builder, items) = bounded.projectable_star_items()?;
    project_publishing_resolved(builder, items, Some(result), ctx)
}

/// Lower the Group operator: DISTINCT (`GroupSpec::Distinct`) or GROUP BY (`GroupSpec::Reduce`).
fn r_lower_group(
    builder: Builder<Unprojected>,
    spec: ast_refined::GroupSpec,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    match spec {
        ast_refined::GroupSpec::Distinct { keys } => {
            // |> %(cols) → SELECT DISTINCT cols
            let items: Vec<_> = keys
                .into_vec()
                .into_iter()
                .map(|item| scalar::s_lower_out_item(item, &builder, ctx))
                .collect::<Result<_>>()?;
            let projected = project_publishing_resolved(builder, items, Some(result), ctx)?;
            projected.add_distinct()
        }

        ast_refined::GroupSpec::Reduce {
            keys,
            reductions,
            plan,
        } => {
            // THE PARTITION IS POSITIONAL NO LONGER: a delegate is a
            // reduction member, split out here where the two lowering
            // shapes part.
            let (delegates, reductions): (Vec<_>, Vec<_>) = reductions
                .into_vec()
                .into_iter()
                .partition(|item| matches!(item, ast_refined::ReductionItem::Delegate(_)));
            let delegates: Vec<ast_refined::DelegateSpec> = delegates
                .into_iter()
                .map(|item| match item {
                    ast_refined::ReductionItem::Delegate(delegate) => delegate,
                    _ => unreachable!("the partition selected delegates"),
                })
                .collect();
            let any_ordered = delegates.iter().any(|d| !d.order.is_empty());

            // All-arbitrary (empty-order) delegates lower as bare columns,
            // exactly as the `~?` arbitrary does.
            if !any_ordered {
                // Arbitrary path lowers payloads as bare columns via the group-by
                // spec. The payload items thread through with their stamps
                // intact, so each aliases from its own delegate stamp — no
                // positional re-threading.
                let arbitrary = delegates.into_iter().flat_map(|d| d.payload).collect();
                return r_lower_group_by_spec(
                    builder,
                    keys,
                    reductions,
                    plan,
                    arbitrary,
                    Some(result),
                    ctx,
                );
            }

            // A single ordered delegate with no aggregates is the 1-arity
            // degenerate of the N-way join: one `row_number()=1` relation, no
            // join to make.
            if reductions.is_empty() && delegates.len() == 1 {
                let delegate = delegates.into_iter().next().unwrap();
                return r_lower_single_ordered_delegate(
                    builder,
                    keys,
                    delegate,
                    result.scope(),
                    ctx,
                );
            }

            // General case: an aggregate relation (when there are aggregates)
            // plus one `row_number()=1` relation per delegate, joined on the
            // group key.
            r_lower_n_way_delegate_join(builder, keys, reductions, plan, delegates, ctx)
        }
    }
}

/// Build one delegate relation — the `row_number()=1` filtered rows for a single
/// delegate — and return it (pre-projection) as a `Builder<Unprojected>`:
///
/// ```sql
/// SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY <keys> ORDER BY <order>)
///                           AS __dql_delegate_rn
///                 FROM <source> )
/// WHERE __dql_delegate_rn = 1
/// ```
///
/// This is the shared primitive: the single-delegate lowering projects one of
/// these; the N-way join builds one per delegate and joins them on the group
/// key. Partition/order use bare column names (they resolve against the wrapped
/// subquery). An empty `order` (arbitrary delegate) yields a window with no
/// ORDER BY — one arbitrary row per group.
fn build_delegate_relation(
    builder: Builder<Unprojected>,
    keys: &[ast_refined::OutItem],
    order: &[ast_refined::OrderingSpec],
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::asts::core::literals::LiteralValue;
    use crate::pipeline::sql_ast::{
        ordering::OrderDirection, BinaryOperator, DomainExpression as SqlDomainExpr, SqlPredicate,
    };

    let bare = |expr: ast_refined::DomainExpression, q: &dyn Qualify| -> Result<SqlDomainExpr> {
        scalar::s_lower_expression(expr, q, ctx)
    };

    // PARTITION BY reads each key's value; naming and output stamps belong to
    // projection. A spread key publishes several and partitions by none.
    let partition: Vec<SqlDomainExpr> = keys
        .iter()
        .filter_map(ast_refined::OutItem::value)
        .map(|expr| bare(expr.clone(), &builder))
        .collect::<Result<_>>()?;
    let sql_order: Vec<(SqlDomainExpr, OrderDirection)> = order
        .iter()
        .map(|spec| {
            let col = bare(spec.column.clone(), &builder)?;
            let dir = match spec.direction {
                Some(ast_refined::OrderDirection::Descending) => OrderDirection::Desc,
                _ => OrderDirection::Asc,
            };
            Ok((col, dir))
        })
        .collect::<Result<_>>()?;

    // Tag each row with row_number, wrap as a subquery, filter to the first.
    let owner = ColumnMetadata::common_identity_scope(builder.columns(), &ctx.identities)
        .unwrap_or_else(|| ctx.identities.anonymous_scope(None));
    let row_number = ctx
        .identities
        .sql_column(owner, None, crate::names::Addressing::Hygienic);
    let tagged = builder.project_all()?.add_window_column(
        "ROW_NUMBER",
        vec![],
        partition,
        sql_order,
        row_number,
    )?;
    let emitted_row_number = tagged
        .columns()
        .last()
        .map(ColumnMetadata::identity)
        .ok_or_else(|| DelightQLError::ParseError {
            message: "delegate row-number projection produced no column".to_string(),
            source: None,
            subcategory: None,
        })?;
    // The demote wraps the tagged select one more time, so the filter must
    // reference the occurrence the demoted layer publishes, not the window
    // layer's — the pre-demote occurrence renders under an alias no FROM
    // entry of the filtered select carries.
    let demoted = tagged.demote()?;
    let row_number_here = demoted.rebind_physical(emitted_row_number)?;
    demoted.add_where(SqlPredicate::new(SqlDomainExpr::Binary {
        left: Box::new(SqlDomainExpr::Column(row_number_here)),
        op: BinaryOperator::Equal,
        right: Box::new(SqlDomainExpr::literal(LiteralValue::Number(
            "1".to_string(),
        ))),
    }))
}

/// Lower a single ordered delegate selection (no aggregates): the 1-arity
/// degenerate of the N-way join — build one delegate relation, project it
/// directly (no join). Output items are projected against the post-wrap builder,
/// whose scope carries `prior_identities` so qualification resolves
/// automatically.
fn r_lower_single_ordered_delegate(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::OutItem>,
    delegate: ast_refined::DelegateSpec,
    _relation: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let filtered = build_delegate_relation(builder, &keys, &delegate.order, ctx)?;

    // Output = group keys + delegate payload, lowered against the POST-WRAP
    // builder so identities resolve through the wrap chain (trust the builder).
    // Each group key carries its own output stamp. Keys are always lowered;
    // a present stamp supplies the projection occurrence.
    let mut output_items: Vec<crate::pipeline::sql_ast::SelectItem> = Vec::new();
    for item in keys {
        output_items.push(scalar::s_lower_out_item(item, &filtered, ctx)?);
    }
    // Each payload expression carries its own output stamp: `None` = the
    // resolver decided it yields no output column (a `(*)` payload that
    // duplicates a group key, already emitted in group position), `Some(col)`
    // = emit, aliased from the stamp. Deduplication is the resolver's.
    for item in delegate.payload {
        let ast_refined::OutItem::One(_) = &item else {
            continue;
        };
        output_items.push(scalar::s_lower_out_item(item, &filtered, ctx)?);
    }

    filtered.add_projection(output_items)
}

/// Lower the general N-way delegate join: a GROUP BY relation (when there are
/// aggregates) plus one `row_number()=1` relation per ordered delegate, all
/// joined on the group key. This is the canonical decomposition; the single
/// ordered delegate with no aggregates is its 1-arity degenerate (handled by
/// `r_lower_single_ordered_delegate` — no join to make with one relation).
///
/// ```sql
/// SELECT agg.k, agg.<aggs>, d0.<payload0>, d1.<payload1>
/// FROM   (SELECT k, <aggs> FROM src GROUP BY k)                         AS agg
/// JOIN   (SELECT * FROM (.. ROW_NUMBER() OVER (PARTITION BY k ORDER BY o0)) WHERE rn=1) AS d0
///          ON agg.k IS NOT DISTINCT FROM d0.k
/// JOIN   (.. ORDER BY o1 .. WHERE rn=1)                                 AS d1
///          ON agg.k IS NOT DISTINCT FROM d1.k
/// ```
///
/// Each relation is built from a frozen copy of the source. The relations share
/// the source column names, so the join tree is kept flat (no intermediate
/// subquery wrap, via `Builder::from_joins`) and every output column is
/// explicitly qualified to the operand that owns it.
fn r_lower_n_way_delegate_join(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::OutItem>,
    reductions: Vec<ast_refined::ReductionItem>,
    plan: ast_refined::ReductionPlan,
    delegates: Vec<ast_refined::DelegateSpec>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{
        BinaryOperator, DomainExpression as SqlDomainExpr, JoinCondition, JoinType, SelectItem,
    };

    // Group-key column names. Each operand wraps the source as a subquery, so a
    // key must survive as a named column to be joined on. Expression keys
    // (e.g. `lower(name)`) combined with ordered delegates are a later slice.
    let key_columns: Vec<crate::names::ColId> = keys
        .iter()
        .filter_map(ast_refined::OutItem::value)
        .map(
            |expr| match scalar::s_lower_expression(expr.clone(), &builder, ctx)? {
                SqlDomainExpr::Column(column) => Ok(column),
                _ => Err(DelightQLError::ParseError {
                    message: "N-way delegate join requires plain column group keys \
                              (expression keys with ordered delegates are not yet supported)"
                        .to_string(),
                    source: None,
                    subcategory: None,
                }),
            },
        )
        .collect::<Result<_>>()?;

    // Freeze the source once and rebuild a fresh frozen Builder per relation.
    // (Duplicating the source subquery is correct; CTE-hoisting it is a future
    // perf peephole, not a correctness concern.)
    let cols = builder.columns().to_vec();
    let names = builder.names().clone();
    let identities = std::rc::Rc::clone(builder.identities());
    let projected_source = builder.project_all()?;
    let source_site = projected_source.publication().site();
    let src = projected_source.to_sql()?;
    let fresh_source = || {
        Builder::from_frozen_at_site(
            src.clone(),
            ScopeName::Fresh(names.fresh(wrap_origin(
                &cols,
                &identities,
                crate::names::WrapReason::Projection,
            ))),
            cols.clone(),
            names.clone(),
            std::rc::Rc::clone(&identities),
            source_site,
        )
    };

    let has_agg = !reductions.is_empty();

    // Operands in output order: [aggregate relation?] then one per delegate.
    let mut operands: Vec<super::builder::JoinOperand> = Vec::new();

    if has_agg {
        let agg = r_lower_group_by_spec(
            fresh_source()?,
            keys.clone(),
            reductions,
            plan,
            vec![],
            None,
            ctx,
        )?;
        operands.push(agg.demote()?.into_join_operand()?);
    }

    // Each delegate → one `row_number()=1` relation. Remember its operand index
    // and payload so output columns can be mapped back to it.
    let mut delegate_slots: Vec<(usize, Vec<ast_refined::OutItem>)> = Vec::new();
    for d in delegates {
        let rel = build_delegate_relation(fresh_source()?, &keys, &d.order, ctx)?;
        delegate_slots.push((operands.len(), d.payload));
        operands.push(rel.into_join_operand()?);
    }

    // Join conditions: anchor.key IS NOT DISTINCT FROM op_i.key (NULL-safe), one
    // per non-anchor operand.
    let conditions: Vec<(JoinType, JoinCondition)> = operands
        .iter()
        .enumerate()
        .skip(1)
        .map(|(operand_index, operand)| {
            let conds: Vec<SqlDomainExpr> = key_columns
                .iter()
                .enumerate()
                .map(|(position, key)| {
                    let anchor = if has_agg {
                        operands[0]
                            .columns()
                            .get(position)
                            .map(ColumnMetadata::identity)
                            .ok_or_else(|| {
                                DelightQLError::parse_error(
                                    "an aggregate operand omitted a group key",
                                )
                            })?
                    } else {
                        exact_republication(*key, &cols, operands[0].columns())?
                    };
                    let other = if has_agg && operand_index == 0 {
                        operand
                            .columns()
                            .get(position)
                            .map(ColumnMetadata::identity)
                            .ok_or_else(|| {
                                DelightQLError::parse_error(
                                    "a delegate operand omitted a group key",
                                )
                            })?
                    } else {
                        exact_republication(*key, &cols, operand.columns())?
                    };
                    Ok(SqlDomainExpr::Binary {
                        left: Box::new(SqlDomainExpr::Column(anchor)),
                        op: BinaryOperator::IsNotDistinctFrom,
                        right: Box::new(SqlDomainExpr::Column(other)),
                    })
                })
                .collect::<Result<_>>()?;
            Ok((
                JoinType::Inner,
                JoinCondition::On(SqlDomainExpr::and(conds)),
            ))
        })
        .collect::<Result<_>>()?;

    // Output projection in cpr order: keys, aggregates, then per-delegate
    // payloads — each explicitly qualified to the operand that owns it, so the
    // qualifier-aware `find_input_column` attaches correct provenance even
    // though all operands share the source column names.
    let mut output_items: Vec<SelectItem> = Vec::new();

    // (a) group keys — from the anchor operand, each aliased from its own
    // output stamp. The n-way path admits only plain-column keys.
    for (position, item) in keys.iter().enumerate() {
        let anchor = if has_agg {
            operands[0]
                .columns()
                .get(position)
                .ok_or_else(|| {
                    DelightQLError::parse_error("an aggregate operand omitted a group key")
                })?
                .identity()
        } else {
            exact_republication(key_columns[position], &cols, operands[0].columns())?
        };
        let mut select = SelectItem::Scaffolding {
            slot: ctx.identities.scaffolding_slot(),
            expr: SqlDomainExpr::Column(anchor),
        };
        if let Some(col) = item.output().map(crate::relation::PortId::column) {
            alias_unaliased(&mut select, col);
        }
        output_items.push(select);
    }

    // (b) aggregates — from the aggregate operand (operands[0] when present).
    // Its columns are keys + aggregate outputs; the aggregates are the columns
    // whose names are not group keys, in order. Each aggregate column already
    // carries the resolver's chosen name (the agg subquery aliased it from its
    // own reductions stamp), so it self-aliases by its column name — again
    // byte-identical to the retired positional thread.
    if has_agg {
        // The group law publishes keys first, then reductions.
        for col in operands[0].columns().iter().skip(key_columns.len()) {
            let mut item = SelectItem::Scaffolding {
                slot: ctx.identities.scaffolding_slot(),
                expr: SqlDomainExpr::Column(col.identity()),
            };
            alias_unaliased(&mut item, col.identity());
            output_items.push(item);
        }
    }

    // (c) delegate payloads — each from its own operand. Each payload
    // expression carries its own output stamp: `None` = the resolver decided
    // it yields no output column (duplicates a group key already emitted in
    // group position), `Some(col)` = emit, aliased from the stamp.
    // Deduplication is the resolver's; the stamp IS its decision.
    for (op_idx, payload) in &delegate_slots {
        for entry in payload {
            let (Some(col), Some(expr)) = (
                entry.output().map(crate::relation::PortId::column),
                entry.value(),
            ) else {
                continue; // resolver stamped None — no output column
            };
            let mut item = SelectItem::Scaffolding {
                slot: ctx.identities.scaffolding_slot(),
                expr: scalar::s_lower_expression(expr.clone(), &operands[*op_idx], ctx)?,
            };
            alias_unaliased(&mut item, col);
            output_items.push(item);
        }
    }

    // Keys, aggregates, and payloads carry their projection occurrences.

    // Assemble the flat join and project.
    let joined = Builder::from_joins(operands, conditions)?;
    joined.add_projection(output_items)
}

/// Lower GROUP BY with keys and aggregate reductions.
///
/// Handles three cases:
/// 1. Simple aggregates (`count:(*), sum:(x)`) — straight GROUP BY
/// 2. Tree group in reductions without CTE (`{first_name, last_name}`) — GROUP BY + aggregate wrapper
/// 3. Tree group in reductions with CTE (nested `~>`) — CTE chain via push_cte
fn r_lower_group_by_spec(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::OutItem>,
    reductions: Vec<ast_refined::ReductionItem>,
    plan: ast_refined::ReductionPlan,
    arbitrary: Vec<ast_refined::OutItem>,
    result: Option<crate::relation::SemanticRelation>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use super::builder::GroupBySpec;

    // A PIVOT IS 1:N. It stands in reduction position and publishes one
    // column per value its key's membership predicate named, so its group
    // takes the pivot road whole rather than the value road one item at a
    // time.
    let has_pivot = reductions
        .iter()
        .any(|item| matches!(item, ast_refined::ReductionItem::Pivot(_)));

    if has_pivot {
        let result = result.ok_or_else(|| {
            DelightQLError::parse_error("an internal delegate aggregate cannot contain a pivot")
        })?;
        let keys = published_values(keys);
        return r_lower_pivot(builder, keys, reductions, result, ctx);
    }

    // Check if any keys expression is a tree group (a record or a
    // metadata level with nested reductions). This pattern:
    // `|> %( {key, "nested": ~> {...}} as tg ~> count:(*) )`
    let by_needs_cte = keys
        .iter()
        .enumerate()
        .any(|(index, _)| plan.needs_cte(ast_refined::TreeGroupLocation::InKeys, index));

    if by_needs_cte {
        let result = result.ok_or_else(|| {
            DelightQLError::parse_error("an internal delegate aggregate cannot contain a tree key")
        })?;
        // Tree-group-in-keys lowering owns its output schema; unwrap the
        // stamps at the boundary.
        let reductions = published_reduction_values(reductions)?;
        let keys = published_values(keys);
        return tree_group::r_lower_tree_group_in_keys(builder, keys, reductions, result, ctx);
    }

    // Check if any reductions expression is a record or metadata level
    // needing CTEs.
    let needs_cte = reductions
        .iter()
        .enumerate()
        .any(|(index, item)| tree_group::reduction_item_needs_cte(item, index, &plan));

    if needs_cte {
        let result = result.ok_or_else(|| {
            DelightQLError::parse_error(
                "an internal delegate aggregate cannot contain a tree reduction",
            )
        })?;
        // A single pure tree reduction takes the CTE chain directly; a
        // MIX of CTE-needing trees with other reductions builds one arm
        // per tree joined to a straight arm on the keys.
        if reductions.len() == 1 && arbitrary.is_empty() {
            // Tree-group CTE lowering owns its output schema; unwrap the stamps.
            let reductions = published_reductions(reductions);
            let keys = published_values(keys);
            return tree_group::r_lower_tree_group_cte(builder, keys, reductions, result, ctx);
        }
        return tree_group::r_lower_tree_group_mixed(
            builder, keys, reductions, plan, arbitrary, result, ctx,
        );
    }

    // Lower GROUP BY keys → SelectItems, aliasing each from its OWN output
    // stamp. A computed key's stamp is the ONLY road out of the grouping
    // stage: the key's occurrence is minted at this stage, so no chain or
    // value tier can re-anchor a reference to it through a heading that
    // dropped the stamp — the adopt road below has to see it.
    let keys: Vec<_> = keys
        .into_iter()
        .map(|item| scalar::s_lower_out_item(item, &builder, ctx))
        .collect::<Result<_>>()?;

    // Lower aggregate reductions → SelectItems, aliasing each from its OWN output
    // stamp. The resolver assigns names like "count", "count_2" to aggregate
    // expressions; the stamp carries that decision on the expression, so no
    // positional cpr threading is needed. Record constructions get the aggregate
    // wrapper; others use normal lowering.
    let mut aggregates: Vec<crate::pipeline::sql_ast::SelectItem> = Vec::new();
    for entry in reductions {
        // Delegates were split off at the group dispatch; their payloads
        // lower through the delegate roads, never as aggregates.
        if matches!(entry, ast_refined::ReductionItem::Delegate(_)) {
            continue;
        }
        let output = *entry.output();
        let payload = match entry {
            ast_refined::ReductionItem::Out(item) => match into_published_value(item) {
                Some(value) => ReductionPayload::Value(value),
                None => continue,
            },
            ast_refined::ReductionItem::Metadata(metadata) => {
                ReductionPayload::Metadata(metadata.group)
            }
            // A group holding a pivot took the pivot road above.
            ast_refined::ReductionItem::Pivot(_) => {
                unreachable!("a pivot reduction reached the ordinary reduction road")
            }
            ast_refined::ReductionItem::Delegate(_) => {
                unreachable!("a delegate was skipped before the payload match")
            }
        };
        let mut item = tree_group::s_lower_reduction_item(payload, &builder, ctx)?;
        alias_unaliased(&mut item, output.column());
        aggregates.push(item);
    }

    // Lower arbitrary delegate columns (bare `<~`) and stamp
    // each with the arbitrary-witness form (`__dql_arbitrary`). This is the
    // only site that knows the user wrote bare `<~`, so the FORM is chosen
    // here; the SPELLING is per-dialect — canonical/sqlite unwraps to the
    // bare column (relaxed GROUP BY), strict targets render `any_value(...)`.
    // Ordered delegates (`<~ #(order)`) lower via the N-way join, not here.
    // Each arb item aliases from its own delegate stamp; a `None` stamp = the
    // resolver decided this payload yields no column (dup-of-key already emitted
    // in group position), so it is skipped rather than positionally threaded.
    for entry in arbitrary {
        use crate::pipeline::sql_ast::DomainExpression as SqlDomainExpr;
        let output = entry.output().map(crate::relation::PortId::column);
        let Some(expr) = into_published_value(entry) else {
            continue;
        };
        let Some(col) = output else {
            continue; // resolver stamped None — no output column
        };
        let lowered = scalar::s_lower_select_item(expr, &builder, ctx)?;
        let mut item = match lowered.expr() {
            Some(expr) => lowered.with_expr(SqlDomainExpr::intrinsic(
                crate::names::Intrinsic::Arbitrary,
                vec![expr.clone()],
            )),
            None => lowered,
        };
        alias_unaliased(&mut item, col);
        aggregates.push(item);
    }

    let spec = GroupBySpec { keys, aggregates };
    match result {
        Some(result) => group_by_publishing_resolved(builder, spec, result, ctx),
        None => builder.add_group_by(spec),
    }
}

/// Lower pivot: `|> %(keys ~> value_col of pivot_key)`.
///
/// Generates a JSON-based CTE pattern:
///   1. Optional _preagg CTE (when value columns contain aggregates)
///   2. _prepivot CTE with json_group_object
///   3. Outer SELECT with json_extract for each pivot value
#[derive(Clone)]
struct StructuralPivotGroup {
    key: ast_refined::DomainExpression,
    values: Vec<ast_refined::DomainExpression>,
}

#[derive(Clone)]
enum StructuralPivotTerm {
    Pivot {
        group: usize,
        member: usize,
        values: Vec<String>,
    },
    Regular(ast_refined::DomainExpression),
}

fn pivot_internal_column(scope: crate::names::ScopeId, ctx: &TransformCtx) -> crate::names::ColId {
    ctx.identities
        .sql_column(scope, None, crate::names::Addressing::Hygienic)
}

/// Re-anchor one pivot term onto the site the CTE input emits.
///
/// The site is a VALUE here, not a question: a CTE input's columns are the
/// realization of one exact site, and the term is rebound at it.
fn rebind_pivot_expression(
    mut expr: crate::pipeline::sql_ast::DomainExpression,
    site: crate::sql_binding::SqlSiteId,
    identities: &crate::names::Registry,
) -> Result<crate::pipeline::sql_ast::DomainExpression> {
    struct Rebind<'a> {
        site: crate::sql_binding::SqlSiteId,
        identities: &'a crate::names::Registry,
        error: Option<DelightQLError>,
    }
    impl crate::pipeline::sql_ast::walk::SqlVisitorMut for Rebind<'_> {
        fn expr(&mut self, expr: &mut crate::pipeline::sql_ast::DomainExpression) {
            let crate::pipeline::sql_ast::DomainExpression::Column(source) = expr else {
                return;
            };
            let site = self.site;
            match self.identities.bindings().physical_at(site, *source) {
                Ok(Some(column)) => {
                    *expr = crate::pipeline::sql_ast::DomainExpression::Column(column);
                }
                Ok(None) => {}
                Err(error) => self.error = Some(error),
            }
        }
    }

    let mut rebind = Rebind {
        site,
        identities,
        error: None,
    };
    crate::pipeline::sql_ast::walk::visit_expression_mut(&mut expr, &mut rebind);
    rebind.error.map_or(Ok(expr), Err)
}

fn r_lower_pivot(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::DomainExpression>,
    reductions: Vec<ast_refined::ReductionItem>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use super::builder::CteBody;
    use crate::pipeline::asts::core::literals::LiteralValue;
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectBuilder, SelectItem,
    };

    let mut groups: Vec<StructuralPivotGroup> = Vec::new();
    let mut terms = Vec::new();
    for item in &reductions {
        match item {
            ast_refined::ReductionItem::Pivot(pivot) => {
                let group = groups
                    .iter()
                    .position(|candidate| candidate.key == *pivot.pivot_key)
                    .unwrap_or_else(|| {
                        groups.push(StructuralPivotGroup {
                            key: pivot.pivot_key.as_ref().clone(),
                            values: Vec::new(),
                        });
                        groups.len() - 1
                    });
                let member = groups[group].values.len();
                groups[group]
                    .values
                    .push(pivot.value_column.as_ref().clone());
                terms.push(StructuralPivotTerm::Pivot {
                    group,
                    member,
                    values: pivot.values.clone(),
                });
            }
            ast_refined::ReductionItem::Out(item) => match into_published_value(item.clone()) {
                Some(value) => terms.push(StructuralPivotTerm::Regular(value)),
                None => continue,
            },
            ast_refined::ReductionItem::Metadata(_) => {
                return Err(DelightQLError::ParseError {
                    message: "a metadata group stands beside a pivot, which lowers values only"
                        .to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            // Delegates were split off at the group dispatch; the pivot
            // road lowers values only.
            ast_refined::ReductionItem::Delegate(_) => continue,
        }
    }

    let output_columns = relation_output_columns(&result, ctx)?;
    let expected_outputs = keys.len()
        + terms
            .iter()
            .map(|term| match term {
                StructuralPivotTerm::Pivot { values, .. } => values.len(),
                StructuralPivotTerm::Regular(_) => 1,
            })
            .sum::<usize>();
    if output_columns.len() != expected_outputs {
        return Err(DelightQLError::ParseError {
            message: "pivot output heading does not match its reductions".to_string(),
            source: None,
            subcategory: None,
        });
    }
    let group_outputs = output_columns[..keys.len()].to_vec();
    let mut output_cursor = keys.len();
    let term_outputs = terms
        .iter()
        .map(|term| {
            let count = match term {
                StructuralPivotTerm::Pivot { values, .. } => values.len(),
                StructuralPivotTerm::Regular(_) => 1,
            };
            let result = output_columns[output_cursor..output_cursor + count].to_vec();
            output_cursor += count;
            result
        })
        .collect::<Vec<_>>();

    let group_sql = keys
        .iter()
        .cloned()
        .map(|expr| scalar::s_lower_expression(expr, &builder, ctx))
        .collect::<Result<Vec<_>>>()?;
    let key_sql = groups
        .iter()
        .map(|group| scalar::s_lower_expression(group.key.clone(), &builder, ctx))
        .collect::<Result<Vec<_>>>()?;
    let value_sql = groups
        .iter()
        .map(|group| {
            group
                .values
                .iter()
                .cloned()
                .map(|expr| scalar::s_lower_expression(expr, &builder, ctx))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let regular_sql = terms
        .iter()
        .filter_map(|term| match term {
            StructuralPivotTerm::Regular(expr) => {
                Some(scalar::s_lower_expression(expr.clone(), &builder, ctx))
            }
            StructuralPivotTerm::Pivot { .. } => None,
        })
        .collect::<Result<Vec<_>>>()?;
    let needs_preagg = groups.iter().any(|group| {
        group
            .values
            .iter()
            .any(|expr| matches!(expr, ast_refined::DomainExpression::Application(_)))
    });
    if needs_preagg && !regular_sql.is_empty() {
        return Err(DelightQLError::ParseError {
            message: "pivot cannot combine pre-aggregated values with regular reductions"
                .to_string(),
            source: None,
            subcategory: None,
        });
    }

    let internal_scope = ctx.identities.anonymous_scope(None);
    let key_aliases = groups
        .iter()
        .map(|_| pivot_internal_column(internal_scope, ctx))
        .collect::<Vec<_>>();
    let value_aliases = groups
        .iter()
        .map(|group| {
            group
                .values
                .iter()
                .map(|_| pivot_internal_column(internal_scope, ctx))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let packet_aliases = groups
        .iter()
        .map(|_| pivot_internal_column(internal_scope, ctx))
        .collect::<Vec<_>>();

    let mut projected = builder.project_all()?;
    if needs_preagg {
        let group_sql = group_sql.clone();
        let key_sql = key_sql.clone();
        let value_sql = value_sql.clone();
        let group_outputs = group_outputs.clone();
        let key_aliases = key_aliases.clone();
        let value_aliases = value_aliases.clone();
        let identities = std::rc::Rc::clone(&ctx.identities);
        projected = projected.push_cte(move |input| {
            let rebound_groups = group_sql
                .iter()
                .cloned()
                .map(|expr| {
                    rebind_pivot_expression(
                        expr,
                        crate::pipeline::transformer::builder::Emitting::site(input),
                        &identities,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let rebound_keys = key_sql
                .iter()
                .cloned()
                .map(|expr| {
                    rebind_pivot_expression(
                        expr,
                        crate::pipeline::transformer::builder::Emitting::site(input),
                        &identities,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let rebound_values = value_sql
                .iter()
                .map(|group| {
                    group
                        .iter()
                        .cloned()
                        .map(|expr| {
                            rebind_pivot_expression(
                                expr,
                                crate::pipeline::transformer::builder::Emitting::site(input),
                                &identities,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            let mut items = rebound_groups
                .iter()
                .cloned()
                .zip(group_outputs.iter().copied())
                .map(|(expr, alias)| SelectItem::expression_with_alias(expr, alias))
                .collect::<Vec<_>>();
            items.extend(
                rebound_keys
                    .iter()
                    .cloned()
                    .zip(key_aliases.iter().copied())
                    .map(|(expr, alias)| SelectItem::expression_with_alias(expr, alias)),
            );
            for (expressions, aliases) in rebound_values.iter().zip(value_aliases.iter()) {
                items.extend(
                    expressions
                        .iter()
                        .cloned()
                        .zip(aliases.iter().copied())
                        .map(|(expr, alias)| SelectItem::expression_with_alias(expr, alias)),
                );
            }
            let (at, outputs, physical_aliases) = super::builder::stand_cte_body_at(
                &mut items,
                input.scope(),
                crate::names::WrapReason::Pivot,
                &identities,
            )?;
            let query = (SelectBuilder::new()
                .set_select(items)
                .from_tables(vec![TableExpression::Scope(input.scope())])
                .group_by(rebound_groups.into_iter().chain(rebound_keys).collect()))
            .standing_at(at)
            .map_err(crate::error::DelightQLError::parse_error)?;
            Ok(CteBody {
                query: QueryExpression::Select(Box::new(query)),
                input_slots: vec![None; outputs.len()],
                physical_aliases,
                output_columns: outputs,
            })
        })?;
    }

    let group_sql_for_prepivot = group_sql.clone();
    let key_sql_for_prepivot = key_sql.clone();
    let value_sql_for_prepivot = value_sql.clone();
    let group_outputs_for_prepivot = group_outputs.clone();
    let packet_aliases_for_prepivot = packet_aliases.clone();
    let regular_sql_for_prepivot = regular_sql.clone();
    let regular_outputs = terms
        .iter()
        .zip(term_outputs.iter())
        .filter_map(|(term, outputs)| match term {
            StructuralPivotTerm::Regular(_) => Some(outputs[0]),
            StructuralPivotTerm::Pivot { .. } => None,
        })
        .collect::<Vec<_>>();
    let regular_outputs_for_prepivot = regular_outputs.clone();
    let identities = std::rc::Rc::clone(&ctx.identities);
    projected = projected.push_cte(move |input| {
        let input_columns = input.scope_columns();
        let (groups, keys, values) = if needs_preagg {
            let mut cursor = 0;
            let groups = input_columns[cursor..cursor + group_outputs_for_prepivot.len()]
                .iter()
                .map(ColumnMetadata::identity)
                .map(SqlDomainExpr::Column)
                .collect::<Vec<_>>();
            cursor += group_outputs_for_prepivot.len();
            let keys = key_aliases
                .iter()
                .map(|_| {
                    let column = input_columns[cursor].identity();
                    cursor += 1;
                    SqlDomainExpr::Column(column)
                })
                .collect::<Vec<_>>();
            let values = value_aliases
                .iter()
                .map(|aliases| {
                    aliases
                        .iter()
                        .map(|_| {
                            let column = input_columns[cursor].identity();
                            cursor += 1;
                            SqlDomainExpr::Column(column)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            (groups, keys, values)
        } else {
            let groups = group_sql_for_prepivot
                .iter()
                .cloned()
                .map(|expr| {
                    rebind_pivot_expression(
                        expr,
                        crate::pipeline::transformer::builder::Emitting::site(input),
                        &identities,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let keys = key_sql_for_prepivot
                .iter()
                .cloned()
                .map(|expr| {
                    rebind_pivot_expression(
                        expr,
                        crate::pipeline::transformer::builder::Emitting::site(input),
                        &identities,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let values = value_sql_for_prepivot
                .iter()
                .map(|group| {
                    group
                        .iter()
                        .cloned()
                        .map(|expr| {
                            rebind_pivot_expression(
                                expr,
                                crate::pipeline::transformer::builder::Emitting::site(input),
                                &identities,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            (groups, keys, values)
        };
        let regular = regular_sql_for_prepivot
            .iter()
            .cloned()
            .map(|expr| {
                rebind_pivot_expression(
                    expr,
                    crate::pipeline::transformer::builder::Emitting::site(input),
                    &identities,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let mut items = groups
            .iter()
            .cloned()
            .zip(group_outputs_for_prepivot.iter().copied())
            .map(|(expr, alias)| SelectItem::expression_with_alias(expr, alias))
            .collect::<Vec<_>>();
        items.extend(
            regular
                .into_iter()
                .zip(regular_outputs_for_prepivot.iter().copied())
                .map(|(expr, alias)| SelectItem::expression_with_alias(expr, alias)),
        );
        for (group_index, (key, group_values)) in keys.iter().zip(values.iter()).enumerate() {
            let mut object_args = Vec::with_capacity(group_values.len() * 2);
            for (member, value) in group_values.iter().enumerate() {
                object_args.push(SqlDomainExpr::literal(LiteralValue::String(
                    member.to_string(),
                )));
                object_args.push(value.clone());
            }
            let object = SqlDomainExpr::function("json_object", object_args);
            let packet = SqlDomainExpr::function("json_group_object", vec![key.clone(), object]);
            items.push(SelectItem::expression_with_alias(
                packet,
                packet_aliases_for_prepivot[group_index],
            ));
        }
        let (at, outputs, physical_aliases) = super::builder::stand_cte_body_at(
            &mut items,
            input.scope(),
            crate::names::WrapReason::Pivot,
            &identities,
        )?;
        let mut select = SelectBuilder::new()
            .set_select(items)
            .from_tables(vec![TableExpression::Scope(input.scope())]);
        if !groups.is_empty() {
            select = select.group_by(groups);
        }
        let query = (select)
            .standing_at(at)
            .map_err(crate::error::DelightQLError::parse_error)?;
        Ok(CteBody {
            query: QueryExpression::Select(Box::new(query)),
            input_slots: vec![None; outputs.len()],
            physical_aliases,
            output_columns: outputs,
        })
    })?;

    let prepivot_columns = projected.columns();
    let group_count = group_outputs.len();
    let regular_count = regular_outputs.len();
    let group_columns = prepivot_columns[..group_count]
        .iter()
        .map(ColumnMetadata::identity)
        .collect::<Vec<_>>();
    let regular_columns = prepivot_columns[group_count..group_count + regular_count]
        .iter()
        .map(ColumnMetadata::identity)
        .collect::<Vec<_>>();
    let packet_columns = prepivot_columns[group_count + regular_count..]
        .iter()
        .map(ColumnMetadata::identity)
        .collect::<Vec<_>>();
    let mut items = group_columns
        .into_iter()
        .zip(group_outputs.iter().copied())
        .map(|(source, output)| {
            SelectItem::expression_with_alias(SqlDomainExpr::Column(source), output)
        })
        .collect::<Vec<_>>();
    let mut regular_index = 0;
    for (term, outputs) in terms.iter().zip(term_outputs.iter()) {
        match term {
            StructuralPivotTerm::Regular(_) => {
                items.push(SelectItem::expression_with_alias(
                    SqlDomainExpr::Column(regular_columns[regular_index]),
                    outputs[0],
                ));
                regular_index += 1;
            }
            StructuralPivotTerm::Pivot {
                group,
                member,
                values,
            } => {
                for (value, output) in values.iter().zip(outputs.iter()) {
                    let path = format!("$.{}.{}", json_path_segment(value), member);
                    items.push(SelectItem::expression_with_alias(
                        SqlDomainExpr::intrinsic(
                            crate::names::Intrinsic::JsonExtractRaw,
                            vec![
                                SqlDomainExpr::Column(packet_columns[*group]),
                                SqlDomainExpr::literal(LiteralValue::String(path)),
                            ],
                        ),
                        *output,
                    ));
                }
            }
        }
    }
    projected.add_projection(items)
}

fn json_path_segment(key: &str) -> String {
    format!("\"{}\"", key.replace('\\', "\\\\").replace('"', "\\\""))
}

// ---------------------------------------------------------------------------
// Pivot helpers
// ---------------------------------------------------------------------------

/// All pre-lowered SQL expressions needed by the pivot CTE chain.

/// Parse reductions into PivotGroup structs, a key→group index map,
/// and regular (non-pivot) aggregate expressions.

/// Lower all AST expressions against the builder's scope before it is consumed
/// by `project_all()`.

/// Push the _preagg CTE: GROUP BY (keys + pivot_keys), aggregate value columns.

/// Push the _prepivot CTE: json_group_object aggregation.

/// Build the outer SELECT items: json_extract per pivot value column.
/// The seam carries typed columns; spelling is extracted at the alias
/// borders via `col_name` ("_unnamed" fallback chain).

/// Strip qualifiers from a SQL expression (for use inside CTEs where columns
/// are available unqualified from the source CTE).

/// The structural identity of a pivot key: what decides whether two
/// pivot clauses share a group. FULL structural equality — never a
/// truncated hash, which could collide and silently MERGE two distinct
/// derived keys into one group (a semantic change, not a naming one).
/// The SQL alias is allocated separately, per group index.

/// Extract the base name from an Lvar (for pivot key/value column names).

/// Lower map-cover: `|> $(fn:())(cols)`.
///
/// For each scope column: if it appears in `columns`, wrap it with `function`;
/// otherwise pass through unchanged. The curried function's existing arguments
/// are kept — the column value takes the row's final place after them.
pub(super) fn r_lower_map_cover(
    builder: Builder<Unprojected>,
    cells: Vec<ast_refined::AppliedCell>,
    guard: Option<Box<ast_refined::TruthExpression>>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem, WhenClause};

    // Lower the guard condition once (if present)
    let sql_condition: Option<SqlDomainExpr> = match guard {
        Some(cond) => Some(super::scalar::s_lower_boolean(*cond, &builder, ctx)?.into_expr()),
        None => None,
    };

    // RESOLUTION ALREADY APPLIED THE CALLABLE: each covered cell carries
    // the closed expression its application produced, so lowering reads
    // cells rather than substituting anything. The stage's published
    // heading is POSITIONAL over its input, so each item aliases the
    // resolver occurrence its slot publishes — which is what keeps two
    // repeated publications independently addressable downstream.
    let outputs = ctx.relations.interface(&result)?.ports().to_vec();
    if outputs.len() != builder.columns().len() {
        return Err(DelightQLError::parse_error(
            "a map cover and its input have different widths",
        ));
    }
    let cell_slots: std::collections::HashMap<usize, &ast_refined::AppliedCell> = cells
        .iter()
        .map(|cell| Ok((builder.slot_of_port(cell.column)?, cell)))
        .collect::<Result<_>>()?;
    let reads = ctx
        .relations
        .carried_sources(&result)?
        .into_iter()
        .map(|(_, sources)| {
            let [source] = sources.as_slice() else {
                return Err(DelightQLError::parse_error(
                    "a map cover output must carry exactly one input position",
                ));
            };
            builder.rebind_port(*source)
        })
        .collect::<Result<Vec<_>>>()?;
    let items: Vec<SelectItem> = builder
        .columns()
        .iter()
        .enumerate()
        .map(|(slot, _)| {
            let applied = cell_slots.get(&slot);
            let alias = outputs[slot].column();
            let col_expr = SqlDomainExpr::Column(reads[slot]);
            match applied {
                Some(cell) => {
                    let result = scalar::s_lower_expression(cell.expr.clone(), &builder, ctx)?;
                    // Wrap in CASE WHEN guard THEN fn(col) ELSE col END
                    let final_expr = match &sql_condition {
                        Some(cond) => SqlDomainExpr::Case {
                            expr: None,
                            when_clauses: vec![WhenClause::new(cond.clone(), result)],
                            else_clause: Some(Box::new(col_expr)),
                        },
                        None => result,
                    };
                    Ok(SelectItem::Publishing {
                        expr: final_expr,
                        slot: alias,
                        printed: true,
                    })
                }
                None => Ok(SelectItem::Publishing {
                    expr: col_expr,
                    slot: alias,
                    printed: true,
                }),
            }
        })
        .collect::<Result<_>>()?;

    project_publishing_resolved(builder, items, Some(result), ctx)
}

/// Lower project-out: `|> -(cols)`.
///
/// Trusts the published heading — the resolver already determined which
/// columns survive.
pub(super) fn r_lower_project_out(
    builder: Builder<Unprojected>,
    _selector: Vec<ast_refined::SelectorItem>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_carried_items(&builder, &result, &ctx.relations)?;
    project_publishing_resolved(builder, items, Some(result), ctx)
}

/// Lower rename-cover: `|> *(old as new)`.
///
/// Trusts the published heading — the resolver already determined the
/// output names.
pub(super) fn r_lower_rename_cover(
    builder: Builder<Unprojected>,
    _specs: crate::pipeline::asts::vocabulary::Vec1<ast_refined::RenameSpec>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_carried_items(&builder, &result, &ctx.relations)?;
    project_publishing_resolved(builder, items, Some(result), ctx)
}

/// Lower transform (basic-cover): `|> $$(expr as col)`.
///
/// Projects all scope columns, replacing those whose name matches a
/// transformation alias with the transformed expression in place.
pub(super) fn r_lower_transform(
    builder: Builder<Unprojected>,
    transformations: crate::pipeline::asts::vocabulary::Vec1<ast_refined::NamedOutItem>,
    conditioned_on: Option<Box<ast_refined::TruthExpression>>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem, WhenClause};

    // THE TARGET IS THE ITEM'S OUTPUT. Resolution addressed the written name
    // once, against the heading the transform stands on; re-addressing the
    // same characters here would answer against a later heading, and a
    // folded second answer is free to disagree with the first.
    let replacements: Vec<(crate::relation::PortId, ast_refined::DomainExpression)> =
        transformations
            .into_vec()
            .into_iter()
            .map(|item| (*item.output(), item.expr))
            .collect();

    // Lower the guard condition once (if present)
    let sql_condition: Option<SqlDomainExpr> = match conditioned_on {
        Some(cond) => Some(super::scalar::s_lower_boolean(*cond, &builder, ctx)?.into_expr()),
        None => None,
    };

    let carried: std::collections::HashMap<_, _> = ctx
        .relations
        .carried_sources(&result)?
        .into_iter()
        .collect();
    let matched_slots: std::collections::HashMap<usize, &ast_refined::DomainExpression> =
        replacements
            .iter()
            .map(|(output, expr)| {
                let sources = carried.get(output).ok_or_else(|| {
                    DelightQLError::parse_error("a transform output is absent from its relation")
                })?;
                let [source] = sources.as_slice() else {
                    return Err(DelightQLError::parse_error(
                        "a transformed output must carry exactly one input position",
                    ));
                };
                Ok((builder.slot_of_port(*source)?, expr))
            })
            .collect::<Result<_>>()?;
    let outputs = ctx.relations.interface(&result)?.ports().to_vec();
    if outputs.len() != builder.columns().len() {
        return Err(DelightQLError::parse_error(
            "a transform and its input have different widths",
        ));
    }
    let reads = ctx
        .relations
        .carried_sources(&result)?
        .into_iter()
        .map(|(_, sources)| {
            let [source] = sources.as_slice() else {
                return Err(DelightQLError::parse_error(
                    "a transform output must carry exactly one input position",
                ));
            };
            builder.rebind_port(*source)
        })
        .collect::<Result<Vec<_>>>()?;
    let items: Vec<SelectItem> = builder
        .columns()
        .iter()
        .enumerate()
        .map(|(slot, _)| {
            let col_expr = SqlDomainExpr::Column(reads[slot]);
            if let Some(replacement_expr) = matched_slots.get(&slot) {
                let sql_expr =
                    scalar::s_lower_expression((*replacement_expr).clone(), &builder, ctx)?;
                // Wrap in CASE WHEN guard THEN new_val ELSE original END
                let final_expr = match &sql_condition {
                    Some(cond) => SqlDomainExpr::Case {
                        expr: None,
                        when_clauses: vec![WhenClause::new(cond.clone(), sql_expr)],
                        else_clause: Some(Box::new(col_expr)),
                    },
                    None => sql_expr,
                };
                Ok(SelectItem::Publishing {
                    expr: final_expr,
                    slot: outputs[slot].column(),
                    printed: true,
                })
            } else {
                Ok(SelectItem::Publishing {
                    expr: col_expr,
                    slot: outputs[slot].column(),
                    printed: true,
                })
            }
        })
        .collect::<Result<_>>()?;

    project_publishing_resolved(builder, items, Some(result), ctx)
}

/// Lower embed-map-cover: `|> +$(fn:() as :"{@}_suffix")(cols)`.
///
/// Keeps all existing columns, then appends new columns by applying the
/// function to each target column with a templated alias name.
pub(super) fn r_lower_embed_map(
    builder: Builder<Unprojected>,
    cells: Vec<ast_refined::AppliedCell>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::SelectItem;

    let outputs: Vec<_> = ctx
        .relations
        .interface(&result)?
        .ports()
        .iter()
        .map(|port| port.column())
        .collect();
    let appended = outputs.get(builder.columns().len()..).unwrap_or(&[]);

    // Part 1: all existing columns pass through
    let mut items: Vec<SelectItem> = builder
        .columns()
        .iter()
        .map(|c| passthrough_item(c))
        .collect();

    // Part 2: RESOLUTION ALREADY APPLIED THE CALLABLE — each cell carries
    // its closed expression, appended under the output identity resolution
    // minted for it.
    for (position, cell) in cells.iter().enumerate() {
        let fn_expr = scalar::s_lower_expression(cell.expr.clone(), &builder, ctx)?;
        let alias = appended
            .get(position)
            .copied()
            .ok_or_else(|| DelightQLError::ParseError {
                message: "embed-map output schema is missing a generated column".to_string(),
                source: None,
                subcategory: None,
            })?;

        items.push(SelectItem::Publishing {
            expr: fn_expr,
            slot: alias,
            printed: true,
        });
    }

    project_publishing_resolved(builder, items, Some(result), ctx)
}

/// Lower meta-ize: `|> ^` — one application; `^^` arrives here as two
/// stacked pipes (composition), never as a distinct operator.
///
/// Synthesizes a VALUES relation from the source's column metadata:
/// columns are always (scope, column_name, ordinal) — meta-ize is
/// shape-only. A detailed variant with declared types is a tempting
/// regression: declaration echoes misreport derived columns.
pub(super) fn r_lower_meta_ize(
    builder: Builder<Unprojected>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, SelectStatement,
        SetOperator,
    };
    let source_columns = builder.columns().to_vec();
    let mut inputs = ctx.relations.inputs(&result)?.into_iter();
    let subject = inputs
        .next()
        .ok_or_else(|| DelightQLError::parse_error("meta-ize has no semantic subject"))?;
    if inputs.next().is_some() {
        return Err(DelightQLError::parse_error(
            "meta-ize has more than one semantic subject",
        ));
    }
    let subject_ports = ctx.relations.interface(&subject)?.ports().to_vec();
    if subject_ports.len() != source_columns.len() {
        return Err(DelightQLError::parse_error(
            "meta-ize's semantic and physical subjects have different widths",
        ));
    }
    let output_columns = relation_output_columns(&result, ctx)?;
    if source_columns.is_empty() || output_columns.len() < 3 {
        return Err(DelightQLError::ParseError {
            message: "meta-ize requires an input heading and three output columns".to_string(),
            source: None,
            subcategory: None,
        });
    }
    let scope = result.scope();
    let make_row = |position: usize, column: &ColumnMetadata| -> Result<Vec<SqlDomainExpr>> {
        let owner = ctx.relations.owner(subject_ports[position])?;
        Ok(vec![
            // The relation the column BELONGS to, not the one publishing it
            // here. A join republishes both arms into one scope so it has a
            // heading of its own; reading that scope would report every
            // column of a two-relation join as one relation's, and the
            // reader's whole question is which relation a column is from.
            SqlDomainExpr::ScopeNameLiteral(owner),
            SqlDomainExpr::PublishedNameLiteral(column.identity()),
            SqlDomainExpr::literal(ast_refined::LiteralValue::Number(
                (position + 1).to_string(),
            )),
        ])
    };
    let mut rows = source_columns.iter().enumerate();
    let (position, column) = rows.next().expect("non-empty checked");
    let published = SqlLayout::new(
        scope,
        output_columns
            .iter()
            .copied()
            .map(ColumnMetadata::new)
            .collect(),
        &ctx.identities,
    );
    let first = published.publish(
        SelectStatement::builder().select_all(
            make_row(position, column)?
                .into_iter()
                .zip(output_columns.iter())
                .map(|(expr, output)| SelectItem::expression_with_alias(expr, *output))
                .collect(),
        ),
    )?;
    let mut query = QueryExpression::Select(Box::new(first));
    for (position, column) in rows {
        // A later branch spells no aliases — SQL takes the output names from
        // the first — so it fills the same slots and names nothing.
        let row = SelectStatement::builder()
            .select_all(
                make_row(position, column)?
                    .into_iter()
                    .map(|expr| {
                        SelectItem::scaffolding_value(expr, ctx.identities.scaffolding_slot())
                    })
                    .collect(),
            )
            .standing_at(scope)
            .map_err(DelightQLError::parse_error)?;
        query = QueryExpression::SetOperation {
            op: SetOperator::UnionAll,
            left: Box::new(query),
            right: Box::new(QueryExpression::Select(Box::new(row))),
        };
    }
    Builder::from_query(
        query,
        ScopeName::Resolved(scope),
        columns_from_relation(&result, ctx)?,
        builder.names().fork(),
        std::rc::Rc::clone(&ctx.identities),
    )
}

/// Lower witness: `|> +` or `|> \+`.
///
/// Generates:
///   `+`  → `SELECT EXISTS(SELECT 1 FROM (<source>)) AS "met"`
///   `\+` → `SELECT NOT EXISTS(SELECT 1 FROM (<source>)) AS "met"`
pub(super) fn r_lower_witness(
    builder: Builder<Unprojected>,
    polarity: crate::pipeline::asts::core::Polarity,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, SelectStatement,
    };

    let names_fork = builder.names().fork();
    let source_query = builder.project_all()?.to_sql()?;
    let exists_expr = if polarity.is_positive() {
        SqlDomainExpr::exists(source_query)
    } else {
        SqlDomainExpr::not_exists(source_query)
    };
    let output_columns = relation_output_columns(&result, ctx)?;
    let output = output_columns
        .first()
        .copied()
        .ok_or_else(|| DelightQLError::ParseError {
            message: "witness requires one resolved output column".to_string(),
            source: None,
            subcategory: None,
        })?;
    let scope = result.scope();
    let select = (SelectStatement::builder()
        .select(SelectItem::expression_with_alias(exists_expr, output)))
    .standing_at(scope)
    .map_err(crate::error::DelightQLError::parse_error)?;

    let query = QueryExpression::Select(Box::new(select));

    Builder::from_query(
        query,
        ScopeName::Resolved(scope),
        columns_from_relation(&result, ctx)?,
        names_fork,
        std::rc::Rc::clone(&ctx.identities),
    )
}

/// Lower the signed witness: postfix `+-`.
///
/// The one-row-unit LEFT-JOIN wrap — source rows widened with `met = 1`,
/// or one all-NULL proxy row with `met = 0` when the source is empty:
///
///   SELECT r.c1 AS c1, ..., COALESCE(r.__p, 0) AS met
///   FROM (SELECT 1 AS __dee) AS dee
///   LEFT JOIN (SELECT 1 AS __p, a.* FROM (<source>) AS a) AS r ON 1 = 1
///
/// Mirrors the effect transformer's `witness_wrap` (the value-position
/// lowering, effect_transformer/mod.rs), including the `met_2` collision
/// convention — the same convention `resolve_signed_witness` applies, so
/// the resolver's schema and the emitted SQL agree. Pinned by the effects
/// ball's compose--70…76 (witness citizenship) and compose--74 (plain
/// ad-hoc `+-`).
pub(super) fn r_lower_signed_witness(
    builder: Builder<Unprojected>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::names::{Addressing, WrapReason};
    use crate::pipeline::asts::core::literals::LiteralValue;
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, JoinCondition, JoinType, QueryExpression, SelectItem,
        SelectStatement,
    };

    let err = |e: String| DelightQLError::ParseError {
        message: format!("SignedWitness: {}", e),
        source: None,
        subcategory: None,
    };

    let names_fork = builder.names().fork();
    let projected = builder.project_all()?;
    let source_columns = projected.columns().to_vec();
    let mut source_query = projected.to_sql()?;
    let source_scope = ColumnMetadata::common_identity_scope(&source_columns, &ctx.identities)
        .ok_or_else(|| DelightQLError::ParseError {
            message: "signed witness input has no common scope".to_string(),
            source: None,
            subcategory: None,
        })?;

    let one = || SqlDomainExpr::literal(LiteralValue::Number("1".to_string()));

    let dee_scope = ctx.identities.wrap_scope(source_scope, WrapReason::Witness);
    let dee_column = ctx
        .identities
        .sql_column(dee_scope, None, Addressing::Hygienic);
    let dee = (SelectStatement::builder()
        .select(SelectItem::expression_with_alias(one(), dee_column)))
    .standing_at(dee_scope)
    .map_err(crate::error::DelightQLError::parse_error)?;

    let source_alias_scope = ctx.identities.wrap_scope(source_scope, WrapReason::Witness);
    let source_alias_columns: Vec<_> = super::builder::republish_under(
        &mut source_query,
        source_alias_scope,
        &source_columns,
        &ctx.identities,
    )?
    .into_iter()
    .map(|column| column.identity())
    .collect();
    let sentinel_scope = ctx.identities.exact_emission_scope(
        source_alias_scope,
        WrapReason::Witness,
        ctx.identities.intern("r", false),
    );
    let sentinel_column = ctx.identities.sql_column(
        sentinel_scope,
        Some(ctx.identities.intern("__p", false)),
        Addressing::Hygienic,
    );
    let sentinel_payload = source_alias_columns
        .iter()
        .map(|column| {
            ctx.identities.rebind_sql_column(
                *column,
                sentinel_scope,
                ctx.identities.published(*column),
            )
        })
        .collect::<Vec<_>>();
    let sentinel = SelectStatement::builder()
        .select(SelectItem::expression_with_alias(one(), sentinel_column))
        .select_all(
            source_alias_columns
                .iter()
                .zip(sentinel_payload.iter())
                .map(|(source, output)| {
                    SelectItem::expression_with_alias(SqlDomainExpr::Column(*source), *output)
                })
                .collect(),
        )
        .from_tables(vec![TableExpression::subquery(
            source_query,
            source_alias_scope,
        )]);
    let sentinel = (sentinel)
        .standing_at(sentinel_scope)
        .map_err(crate::error::DelightQLError::parse_error)?;

    let join = TableExpression::Join {
        left: Box::new(TableExpression::subquery(
            QueryExpression::Select(Box::new(dee)),
            dee_scope,
        )),
        right: Box::new(TableExpression::subquery(
            QueryExpression::Select(Box::new(sentinel)),
            sentinel_scope,
        )),
        join_type: JoinType::Left,
        join_condition: JoinCondition::On(SqlDomainExpr::eq(one(), one())),
    };

    let output_columns = relation_output_columns(&result, ctx)?;
    if output_columns.len() != sentinel_payload.len() + 1 {
        return Err(DelightQLError::ParseError {
            message: "signed witness output heading does not match its input".to_string(),
            source: None,
            subcategory: None,
        });
    }
    let mut items: Vec<SelectItem> = Vec::with_capacity(output_columns.len());
    for (source, output) in sentinel_payload
        .iter()
        .zip(output_columns.iter().take(sentinel_payload.len()))
    {
        let read = SqlDomainExpr::Column(*source);
        let expr = if ctx.identities.is_tree_valued(*source) {
            SqlDomainExpr::function(
                "coalesce",
                vec![
                    read,
                    SqlDomainExpr::literal(LiteralValue::String("[]".to_string())),
                ],
            )
        } else {
            read
        };
        items.push(SelectItem::expression_with_alias(expr, *output));
    }
    items.push(SelectItem::expression_with_alias(
        SqlDomainExpr::function(
            "coalesce",
            vec![
                SqlDomainExpr::Column(sentinel_column),
                SqlDomainExpr::literal(LiteralValue::Number("0".to_string())),
            ],
        ),
        *output_columns.last().expect("length checked"),
    ));

    let scope = ctx
        .identities
        .common_scope(&output_columns)
        .ok_or_else(|| err("signed witness output has no common scope".to_string()))?;
    let select = (SelectStatement::builder()
        .select_all(items)
        .from_tables(vec![join]))
    .standing_at(scope)
    .map_err(crate::error::DelightQLError::parse_error)?;

    let query = QueryExpression::Select(Box::new(select));
    Builder::from_query(
        query,
        ScopeName::Resolved(scope),
        columns_from_relation(&result, ctx)?,
        names_fork,
        std::rc::Rc::clone(&ctx.identities),
    )
}

/// Lower reposition: `|> *[col as pos]`.
///
/// Trusts the published heading — the resolver already computed the
/// reordered column list.
pub(super) fn r_lower_reposition(
    builder: Builder<Unprojected>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_carried_items(&builder, &result, &ctx.relations)?;
    project_publishing_resolved(builder, items, Some(result), ctx)
}

/// Lower narrowing destructure: `|> .column{.field1, .field2}`.
///
/// Iterates a JSON array column via `json_each`, extracts named fields.
/// Output schema contains ONLY the extracted fields (no context carry-forward).
///
/// ```sql
/// SELECT json_extract(_narrow_0.value, '$.name') AS name,
///        json_extract(_narrow_0.value, '$.age') AS age
/// FROM (<source>) AS t_N, json_each(t_N."col") AS _narrow_0
/// ```
pub(super) fn r_lower_narrowing_destructure(
    builder: Builder<Unprojected>,
    nest: ast_refined::Reference,
    pattern: ast_refined::RecordPattern,
    schema: &[ast_refined::DestructureMapping],
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::asts::core::{NamedReference, Reference};

    let Reference::Named(NamedReference(ColumnOccurrence { column, .. })) = nest else {
        return Err(DelightQLError::transformation_error(
            "narrowing requires a semantic source port",
            "narrow/source",
        ));
    };
    let source_column = builder.rebind_port(column)?;
    let mut outputs = relation_output_columns(&result, ctx)?
        .into_iter()
        .peekable();

    // Narrowing iterates an ARRAY. Do not lax-wrap a top-level object
    // into a one-element array here (json_type = 'object' →
    // json_array(j)): the object case already has its spelling — pathing,
    // `(j:{.a})` — and the coercion would both duplicate it under the
    // wrong operator and convert an upstream shape bug (object arriving
    // where an array was promised) into a plausible one-row answer.
    // Non-array inputs are an open ruling; the standing red
    // brace_narrowing_single_object holds the door.
    //
    // THE PAYLOAD IS A PATTERN, and its members are read by the SAME item
    // builders the ordinary destructure uses — one path spelling, one
    // published name, so a numeric step and a flattened reach cannot mean
    // one thing here and another there.
    let items = narrowing_items(&pattern, schema, &mut outputs, ctx)?;
    builder.expand_with_json_each(
        source_column,
        "_narrow",
        super::builder::JsonEachKind::Array,
        |_source_columns, _source_slot| vec![],
        move |_key_column, value_column| {
            let source = crate::pipeline::sql_ast::DomainExpression::Column(value_column);
            items
                .iter()
                .map(|(path, output)| {
                    crate::pipeline::sql_ast::SelectItem::expression_with_alias(
                        crate::pipeline::sql_ast::DomainExpression::Function {
                            name: "json_extract".into(),
                            args: vec![
                                source.clone(),
                                crate::pipeline::sql_ast::DomainExpression::JsonPathLiteral(
                                    path.clone(),
                                ),
                            ],
                            distinct: false,
                        },
                        *output,
                    )
                })
                .collect()
        },
        &[],
    )
}

/// One reach per publishing member, in written order, with the occurrence
/// the resolver minted for it.
fn narrowing_items(
    pattern: &ast_refined::RecordPattern,
    schema: &[ast_refined::DestructureMapping],
    outputs: &mut std::iter::Peekable<impl Iterator<Item = crate::names::ColId>>,
    _ctx: &TransformCtx,
) -> Result<Vec<(crate::pipeline::asts::core::Path, crate::names::ColId)>> {
    use crate::pipeline::asts::core::{Path, PathStep, RecordPatternMember};

    let mut items = Vec::new();
    for member in pattern.members.iter() {
        let output = next_destructure_output(outputs)?;
        let path = match member {
            // A binder reads the like-named key, and the mapping the
            // resolver built beside it is where that key is written down.
            RecordPatternMember::Binder(binder) => schema
                .iter()
                .find(|mapping| mapping.column == *binder)
                .and_then(|mapping| {
                    Path::try_from_steps(vec![PathStep::Key(mapping.json_key.clone())])
                })
                .ok_or_else(|| DelightQLError::ParseError {
                    message: "a narrowing binder has no authored key".to_string(),
                    source: None,
                    subcategory: None,
                })?,
            RecordPatternMember::Path(binding) => binding.path.clone(),
            other => {
                return Err(DelightQLError::ParseError {
                    message: format!("a narrowing publishes fields; {other:?} publishes none"),
                    source: None,
                    subcategory: None,
                })
            }
        };
        items.push((path, output));
    }
    Ok(items)
}

/// Lower interior drill-down: `|> .column(*)` or `|> .column(field1, field2)`.
///
/// Explodes an interior relation (tree group JSON array column) into rows
/// using `json_each`, carrying context columns through. This is the inverse
/// of tree-group aggregation.
///
/// ```sql
/// SELECT t_N.country,
///        json_extract(_drill_0.value, '$.first_name') AS first_name,
///        json_extract(_drill_0.value, '$.last_name') AS last_name
/// FROM (<source>) AS t_N, json_each(t_N."people") AS _drill_0
/// ```
fn r_lower_interior_drill_down(
    builder: Builder<Unprojected>,
    column: crate::relation::PortId,
    selected: Vec<crate::relation::PortId>,
    groundings: Vec<crate::pipeline::asts::core::operators::ResolvedInteriorGrounding>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    let drilled = builder.rebind_port(column)?;
    // The drilled slot was resolved uniquely above, so the context is
    // everything that is not that OCCURRENCE. Excluding by value class
    // would take a sibling publication of the same value with it.
    let context_columns: Vec<_> = builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .filter(|candidate| *candidate != drilled)
        .collect();
    let num_context = context_columns.len();
    let output_columns = relation_output_columns(&result, ctx)?;
    if output_columns.len() < num_context + selected.len() {
        return Err(DelightQLError::ParseError {
            message: "interior drill output heading is incomplete".to_string(),
            source: None,
            subcategory: None,
        });
    }
    let interior = ctx.relations.interior(column)?.ok_or_else(|| {
        DelightQLError::transformation_error(
            "a drilled semantic port has no construction-recorded interior",
            "drill/interior",
        )
    })?;
    let interior_ports = ctx.relations.interface(&interior)?;
    let result_ports = ctx.relations.interface(&result)?;
    let json_keys = result_ports.ports()[num_context..num_context + selected.len()]
        .iter()
        .map(|output| {
            let mut sources = ctx
                .relations
                .ancestors_into(&result, *output)?
                .into_iter()
                .filter(|source| interior_ports.ports().contains(source));
            let source = sources.next().ok_or_else(|| {
                DelightQLError::transformation_error(
                    "a drill output has no construction-recorded interior source",
                    "drill/interior",
                )
            })?;
            if sources.next().is_some() {
                return Err(DelightQLError::transformation_error(
                    "a drill output carries several interior source ports",
                    "drill/interior",
                ));
            }
            Ok(source.column())
        })
        .collect::<Result<Vec<_>>>()?;

    builder.expand_with_json_each(
        drilled,
        "_drill",
        super::builder::JsonEachKind::Array,
        |source_columns, source_slot| {
            source_columns
                .iter()
                .enumerate()
                .filter(|(slot, _)| *slot != source_slot)
                .enumerate()
                .map(|(output, (source, column))| {
                    (
                        source,
                        SelectItem::expression_with_alias(
                            SqlDomainExpr::Column(*column),
                            output_columns[output],
                        ),
                    )
                })
                .collect()
        },
        |_key_column, value_column| {
            json_keys
                .iter()
                .enumerate()
                .map(|(i, interior_column)| {
                    SelectItem::expression_with_alias(
                        SqlDomainExpr::function(
                            "json_extract",
                            vec![
                                SqlDomainExpr::Column(value_column),
                                SqlDomainExpr::PublishedJsonPathLiteral(*interior_column),
                            ],
                        ),
                        output_columns[num_context + i],
                    )
                })
                .collect()
        },
        &groundings,
    )
}

/// Lower scalar destructure: `data ~= {first_name, last_name}`.
///
/// Lower a destructure pattern by walking the pattern tree inductively.
///
/// Scalar mode: extracts fields from a JSON value without row explosion.
/// Aggregate mode: first explodes the top-level array via `json_each`,
/// then walks the pattern against each element.
///
/// Nested `~>` patterns produce additional `json_each` joins at each level.
/// One recursive function handles everything: base extractions, the
/// iterating nested member, and the metadata binding (`key:~>`).
pub(super) fn r_lower_destructure(
    builder: Builder<Unprojected>,
    json_column: ast_refined::DomainExpression,
    mode: ast_refined::DestructureMode,
    pattern: &ast_refined::TreePattern,
    mappings: &[ast_refined::DestructureMapping],
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let source_expr = scalar::s_lower_expression(json_column.clone(), &builder, ctx)?;
    let carried = ctx.relations.carried_sources(&result)?;
    crate::probe::probing!(destructure, {
        for (output, sources) in &carried {
            crate::probe::probe!(
                destructure,
                "  output {} {:?}",
                if sources.is_empty() { "take" } else { "SKIP" },
                crate::probe::chain(&ctx.identities, output.column())
            );
        }
    });
    let mut outputs = carried
        .into_iter()
        .filter_map(|(output, sources)| sources.is_empty().then_some(output.column()))
        .peekable();

    let lowered = if matches!(mode, ast_refined::DestructureMode::Aggregate) {
        let json_column = match &json_column {
            ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) => builder.rebind_port(*column)?,
            _ => {
                return Err(DelightQLError::ParseError {
                    message: "aggregate destructure: expected Lvar for json column".into(),
                    source: None,
                    subcategory: None,
                });
            }
        };
        lower_with_json_each(builder, json_column, pattern, mappings, &mut outputs, ctx)
    } else {
        lower_destructure_pattern(builder, &source_expr, pattern, mappings, &mut outputs, ctx)
    }?;
    if outputs.peek().is_some() {
        return Err(DelightQLError::ParseError {
            message: "destructure did not account for its resolved output heading".to_string(),
            source: None,
            subcategory: None,
        });
    }
    Ok(lowered)
}

fn next_destructure_output(
    outputs: &mut std::iter::Peekable<impl Iterator<Item = crate::names::ColId>>,
) -> Result<crate::names::ColId> {
    outputs.next().ok_or_else(|| DelightQLError::ParseError {
        message: "destructure produced more columns than its resolved heading".to_string(),
        source: None,
        subcategory: None,
    })
}

fn destructure_temp(ctx: &TransformCtx) -> crate::names::ColId {
    let scope = ctx.identities.anonymous_scope(None);
    ctx.identities
        .sql_column(scope, None, crate::names::Addressing::Hygienic)
}

fn lower_destructure_pattern<I: Iterator<Item = crate::names::ColId>>(
    builder: Builder<Unprojected>,
    source: &crate::pipeline::sql_ast::DomainExpression,
    pattern: &ast_refined::TreePattern,
    mappings: &[ast_refined::DestructureMapping],
    outputs: &mut std::iter::Peekable<I>,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::asts::core::{RecordPatternMember, TreePattern};
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    match pattern {
        TreePattern::Record(record) => {
            let mut base_items = Vec::new();
            let mut explosions = Vec::new();
            let mut nested_navigations = Vec::new();
            let mut metadata = None;

            for member in record.members.iter() {
                match member {
                    RecordPatternMember::Binder(binder) => {
                        base_items.push(make_destructure_shorthand_item(
                            source,
                            *binder,
                            next_destructure_output(outputs)?,
                            mappings,
                        )?);
                    }
                    RecordPatternMember::Keyed { key, .. } => {
                        base_items.push(make_json_extract_item(
                            source,
                            &format!(".{}", key),
                            next_destructure_output(outputs)?,
                        ));
                    }
                    // `"k": ~> {…}` iterates the array under the key;
                    // `"k": {…}` navigates into the object under it.
                    RecordPatternMember::Nested {
                        key,
                        iteration,
                        pattern,
                    } => {
                        let landing =
                            (key.clone(), pattern.as_ref().clone(), destructure_temp(ctx));
                        if *iteration {
                            explosions.push(landing);
                        } else {
                            nested_navigations.push(landing);
                        }
                    }
                    RecordPatternMember::Path(binding) => {
                        base_items.push(make_json_extract_item(
                            source,
                            &binding.path.suffix(),
                            next_destructure_output(outputs)?,
                        ));
                    }
                    // The object's KEYS become one column's values, and the
                    // target binds what stands under them.
                    RecordPatternMember::Metadata { target, .. } => {
                        metadata = Some(target);
                    }
                    // The anaphor iterates and binds nothing.
                    RecordPatternMember::Disregarded => {}
                }
            }

            // A metadata binding stands alone at its level — the grammar
            // gives it no sibling — so reading it is reading the level.
            if let Some(target) = metadata {
                return lower_metadata_level(builder, source, target, mappings, outputs, ctx);
            }

            let existing_len = builder.columns().len();
            let mut proj: Vec<SelectItem> = builder
                .columns()
                .iter()
                .map(|c| passthrough_item(c))
                .collect();
            proj.extend(base_items);
            let base_count = proj.len() - existing_len;
            for (key, _, alias) in explosions.iter().chain(nested_navigations.iter()) {
                proj.push(make_json_extract_raw_item(
                    source,
                    &format!(".{}", key),
                    *alias,
                ));
            }
            let projected = builder.add_support_projection(proj)?;
            let mut cursor = existing_len + base_count;
            let mut explosion_columns = Vec::with_capacity(explosions.len());
            for (_key, pattern, _) in explosions {
                explosion_columns.push((projected.columns()[cursor].identity(), pattern));
                cursor += 1;
            }
            let mut navigation_columns = Vec::with_capacity(nested_navigations.len());
            for (_key, pattern, _) in nested_navigations {
                navigation_columns.push((projected.columns()[cursor].identity(), pattern));
                cursor += 1;
            }
            let mut builder = projected.demote()?;

            for (temp, nested_pattern) in explosion_columns {
                builder =
                    lower_with_json_each(builder, temp, &nested_pattern, mappings, outputs, ctx)?;
                builder = remove_column(builder, temp, ctx)?;
            }

            for (temp, nested_pattern) in navigation_columns {
                let current = builder.rebind_physical(temp)?;
                let nav_source = SqlDomainExpr::Column(current);
                builder = lower_destructure_pattern(
                    builder,
                    &nav_source,
                    &nested_pattern,
                    mappings,
                    outputs,
                    ctx,
                )?;
                builder = remove_column(builder, current, ctx)?;
            }

            Ok(builder)
        }

        TreePattern::Array(array) => {
            let mut items: Vec<SelectItem> = builder
                .columns()
                .iter()
                .map(|c| passthrough_item(c))
                .collect();
            for member in array.members.iter() {
                items.push(make_json_extract_item(
                    source,
                    &member.path.suffix(),
                    next_destructure_output(outputs)?,
                ));
            }
            builder.add_support_projection(items)?.demote()
        }
    }
}

/// Explode an object into its KEYS and the values under them: the key column
/// publishes the keys, and the target binds inside each value.
fn lower_metadata_level<I: Iterator<Item = crate::names::ColId>>(
    builder: Builder<Unprojected>,
    source: &crate::pipeline::sql_ast::DomainExpression,
    target: &crate::pipeline::asts::core::PatternTarget<crate::pipeline::asts::core::Refined>,
    mappings: &[ast_refined::DestructureMapping],
    outputs: &mut std::iter::Peekable<I>,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::asts::core::PatternTarget;
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    let temp_alias = destructure_temp(ctx);
    let context_len = builder.columns().len();
    let mut proj: Vec<SelectItem> = builder
        .columns()
        .iter()
        .map(|c| passthrough_item(c))
        .collect();
    proj.push(SelectItem::expression_with_alias(
        source.clone(),
        temp_alias,
    ));
    let projected = builder.add_support_projection(proj)?;
    let temp_slot = context_len;
    let temp_column = projected.columns()[temp_slot].identity();
    let builder = projected.demote()?;
    let key_output = next_destructure_output(outputs)?;
    let value_alias = destructure_temp(ctx);
    let context_len = builder.columns().len();
    let expanded = builder.expand_with_json_each(
        temp_column,
        "_je",
        super::builder::JsonEachKind::Object,
        |source_columns, _source_slot| {
            source_columns
                .iter()
                .enumerate()
                .map(|(slot, column)| {
                    (
                        slot,
                        SelectItem::expression_with_alias(SqlDomainExpr::Column(*column), *column),
                    )
                })
                .collect()
        },
        |key_column, value_column| {
            vec![
                SelectItem::expression_with_alias(SqlDomainExpr::Column(key_column), key_output),
                // .value → pass through for recursion
                SelectItem::expression_with_alias(SqlDomainExpr::Column(value_column), value_alias),
            ]
        },
        &[],
    )?;
    // Read the exploded value after the demote, and again after the
    // removal: each of them republishes the heading, and an occurrence
    // taken before one names a scope the next statement's FROM does not
    // offer. The slot is the same either side; the occurrence is not.
    let demoted = expanded.demote()?;
    let builder = remove_physical_slot(demoted, temp_slot, ctx)?;
    let value_slot = if temp_slot < context_len + 1 {
        context_len
    } else {
        context_len + 1
    };
    let value_column = builder.columns()[value_slot].identity();
    let builder = match target {
        PatternTarget::Pattern(inner) => {
            lower_with_json_each(builder, value_column, inner, mappings, outputs, ctx)?
        }
        // `g:~> _` binds the keys and disregards the contents: one row per
        // key, and nothing under it to reach.
        PatternTarget::Disregarded => builder,
    };
    // THE SLOT IS THE SAME EITHER SIDE; THE OCCURRENCE IS NOT. The nested
    // expansion republishes the heading, so searching for the occurrence
    // taken before it finds nothing — the position it stood at is what
    // carries across, and the nested level only appends after it.
    if builder.columns().len() <= value_slot {
        return Err(DelightQLError::parse_error(
            "a metadata level lost the position its value stood at",
        ));
    }
    remove_physical_slot(builder, value_slot, ctx)
}

/// Eat a `~>`: wrap a column in json_each, then recurse into the nested
/// pattern.
///
/// This is the bridge between levels. Each call produces exactly one
/// json_each. A metadata level owns its own json_each — over an OBJECT rather
/// than an array — and is reached through `lower_metadata_level` instead.
fn lower_with_json_each(
    builder: Builder<Unprojected>,
    column: crate::names::ColId,
    pattern: &ast_refined::TreePattern,
    mappings: &[ast_refined::DestructureMapping],
    outputs: &mut std::iter::Peekable<impl Iterator<Item = crate::names::ColId>>,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    let value_alias = destructure_temp(ctx);
    let context_len = builder.columns().len();
    let expanded = builder.expand_with_json_each(
        column,
        "_destr",
        super::builder::JsonEachKind::Array,
        |source_columns, _source_slot| {
            source_columns
                .iter()
                .enumerate()
                .map(|(slot, source)| {
                    (
                        slot,
                        SelectItem::expression_with_alias(SqlDomainExpr::Column(*source), *source),
                    )
                })
                .collect()
        },
        |_key_column, value_column| {
            vec![SelectItem::expression_with_alias(
                SqlDomainExpr::Column(value_column),
                value_alias,
            )]
        },
        &[],
    )?;
    // Read the exploded value AFTER the demote, not before it. `demote` wraps
    // the expansion as a subquery and republishes its heading, so an
    // expression built over the pre-wrap occurrence names the join scope
    // underneath the wrapper — a table the new statement's FROM does not
    // offer. The slot is the same either side; the occurrence is not.
    let demoted = expanded.demote()?;
    let value_column = demoted.columns()[context_len].identity();
    let value_source = SqlDomainExpr::Column(value_column);
    let builder =
        lower_destructure_pattern(demoted, &value_source, pattern, mappings, outputs, ctx)?;
    remove_physical_slot(builder, context_len, ctx)
}

fn remove_physical_slot(
    builder: Builder<Unprojected>,
    slot: usize,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let column = builder
        .columns()
        .get(slot)
        .ok_or_else(|| DelightQLError::parse_error("physical support slot is absent"))?
        .identity();
    remove_current_column(builder, column, ctx)
}

fn remove_column(
    builder: Builder<Unprojected>,
    column: crate::names::ColId,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let column = builder.rebind_physical(column)?;
    remove_current_column(builder, column, ctx)
}

fn remove_current_column(
    builder: Builder<Unprojected>,
    column: crate::names::ColId,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    // Removal is slot-exact. Excluding by value class drops every sibling
    // publication of one value, where the caller named ONE of them.
    let retained: Vec<_> = builder
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(slot, candidate)| (candidate.identity() != column).then_some(slot))
        .collect();
    let keep: Vec<_> = retained
        .iter()
        .map(|slot| builder.columns()[*slot].identity())
        .collect();
    crate::probe::probing!(destructure, {
        crate::probe::probe!(
            destructure,
            "remove {:?}",
            crate::probe::chain(&ctx.identities, column)
        );
        for candidate in builder.columns() {
            crate::probe::probe!(
                destructure,
                "  {} {:?}",
                if keep.contains(&candidate.identity()) {
                    "keep"
                } else {
                    "DROP"
                },
                crate::probe::chain(&ctx.identities, candidate.identity())
            );
        }
    });

    if keep.is_empty() {
        return Ok(builder);
    }

    let items: Vec<SelectItem> = keep
        .iter()
        .map(|column| SelectItem::expression_with_alias(SqlDomainExpr::Column(*column), *column))
        .collect();
    builder
        .select_physical_projection(items, &retained)?
        .demote()
}

fn make_json_extract_item(
    source: &crate::pipeline::sql_ast::DomainExpression,
    json_path: &str,
    alias: crate::names::ColId,
) -> crate::pipeline::sql_ast::SelectItem {
    make_json_extract_item_named(source, json_path, alias, "json_extract".into())
}

fn make_destructure_shorthand_item(
    source: &crate::pipeline::sql_ast::DomainExpression,
    member: crate::relation::PortId,
    alias: crate::names::ColId,
    mappings: &[ast_refined::DestructureMapping],
) -> Result<crate::pipeline::sql_ast::SelectItem> {
    let keys: Vec<_> = mappings
        .iter()
        .filter(|mapping| mapping.column == member)
        .map(|mapping| mapping.json_key.as_str())
        .collect();
    let [key] = keys.as_slice() else {
        return Err(DelightQLError::parse_error(
            "resolved destructure shorthand does not have exactly one authored JSON key",
        ));
    };
    Ok(make_json_extract_item(source, &format!(".{key}"), alias))
}

/// Like [`make_json_extract_item`] but the extraction must stay NATIVE json
/// (never a per-dialect *_string respell): the temp column is fed straight
/// into `json_each`/recursive navigation, which breaks on a stringified
/// subtree.
fn make_json_extract_raw_item(
    source: &crate::pipeline::sql_ast::DomainExpression,
    json_path: &str,
    alias: crate::names::ColId,
) -> crate::pipeline::sql_ast::SelectItem {
    make_json_extract_item_named(
        source,
        json_path,
        alias,
        crate::pipeline::sql_ast::FunctionName::Intrinsic(crate::names::Intrinsic::JsonExtractRaw),
    )
}

fn make_json_extract_item_named(
    source: &crate::pipeline::sql_ast::DomainExpression,
    json_path: &str,
    alias: crate::names::ColId,
    fn_name: crate::pipeline::sql_ast::FunctionName,
) -> crate::pipeline::sql_ast::SelectItem {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    let full_path = if json_path.starts_with('[') || json_path.starts_with('.') {
        format!("${}", json_path)
    } else {
        format!("$.{}", json_path)
    };

    SelectItem::expression_with_alias(
        SqlDomainExpr::Function {
            name: fn_name,
            args: vec![
                source.clone(),
                SqlDomainExpr::literal(ast_refined::LiteralValue::String(full_path)),
            ],
            distinct: false,
        },
        alias,
    )
}

/// One correlation, and the exact pair of arms it constrains.
/// Expand a whole-heading correlation into the per-column comparisons it
/// abbreviates.
///
/// HERE, at the lowering, and not in the carrier: the correlation travels as
/// the two arms and the MODE it aligns by, so nothing between the comma
/// position and this point can read it as an ordinary restriction over one
/// row.
///
/// `x.* = y.*` matches on every name both operands publish; `x|*| = y|*|`
/// matches by ordinal. Columns only one side has are not matched on — under
/// `;` the headings may differ, and the intersection of names is the only
/// natural reading.
pub(super) fn expand_whole_heading(
    whole: &ast_refined::WholeHeading,
    identities: &crate::names::Registry,
) -> Result<ast_refined::TruthExpression> {
    let by_name = whole.by_name();
    let (left, right) = whole.arms();
    let (left, right) = (*left, *right);

    let left_columns = crate::relation::published_ports(identities, &left)?;
    let right_columns = crate::relation::published_ports(identities, &right)?;
    let mut pairs: Vec<(crate::relation::PortId, crate::relation::PortId)> = Vec::new();
    if by_name {
        for right_column in right_columns {
            let Some(name) = identities.published_sym(right_column.column()) else {
                continue;
            };
            let matches: Vec<crate::relation::PortId> = left_columns
                .iter()
                .copied()
                .filter(|left| identities.published_sym(left.column()) == Some(name))
                .collect();
            match matches.as_slice() {
                [] => {}
                [left] => pairs.push((*left, right_column)),
                _ => {
                    return Err(crate::error::DelightQLError::validation_error_categorized(
                        "setop/correlation/ambiguous",
                        "whole-heading correlation found a duplicate name in one operand",
                        "project or rename the operand to a unique heading",
                    ))
                }
            }
        }
        if pairs.is_empty() {
            return Err(crate::error::DelightQLError::validation_error(
                "Whole-heading correlation has no shared columns",
                "The two operands have no column names in common",
            ));
        }
    } else {
        let width = left_columns.len().min(right_columns.len());
        if width == 0 {
            return Err(crate::error::DelightQLError::validation_error(
                "Positional whole-heading correlation has no columns to compare",
                "At least one operand has no columns",
            ));
        }
        pairs.extend((0..width).map(|slot| (left_columns[slot], right_columns[slot])));
    }

    let comparisons = pairs.into_iter().map(|(left, right)| {
        ast_refined::TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
            left: Box::new(ast_refined::DomainExpression::Reference(
                crate::pipeline::asts::core::Reference::Named(
                    crate::pipeline::asts::core::NamedReference(
                        ColumnOccurrence::engine_qualified(left),
                    ),
                ),
            )),
            right: Box::new(ast_refined::DomainExpression::Reference(
                crate::pipeline::asts::core::Reference::Named(
                    crate::pipeline::asts::core::NamedReference(
                        ColumnOccurrence::engine_qualified(right),
                    ),
                ),
            )),
        })
    });
    ast_refined::TruthExpression::all(comparisons.collect()).ok_or_else(|| {
        crate::error::DelightQLError::validation_error_categorized(
            "setop/correlation/ambiguous",
            "whole-heading correlation found no columns to pair",
            "project or rename the operands to a shared heading",
        )
    })
}

pub(super) struct ArmCorrelation {
    pub left: usize,
    pub right: usize,
    pub predicate: ast_refined::TruthExpression,
    pub min_multiplicity: bool,
}

/// Lower a correlated bag operation into SQL.
///
/// A correlation is PAIR-SCOPED: it filters exactly the two arms it names
/// and leaves the rest of the run alone. Union-flavored operators keep each
/// named arm's matching rows and union all the arms; minus keeps only the
/// left arm's rows that have no match, which is what makes it
/// bag-preserving where SQL `EXCEPT` is not.
pub(super) fn r_lower_correlated_set_op(
    operands: Vec<SetArm>,
    operator: ast_refined::SetOperator,
    correlations: Vec<ArmCorrelation>,
    steps: &[crate::relation::SemanticRelation],
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::DomainExpression as SqlDomainExpr;
    use crate::pipeline::sql_ast::SqlPredicate;

    let result = *steps.last().ok_or_else(|| empty_run())?;
    if operands.len() < 2 {
        return Err(DelightQLError::ParseError {
            message: "a correlated set operation requires at least 2 operands".to_string(),
            source: None,
            subcategory: None,
        });
    }
    for correlation in &correlations {
        if correlation.left >= operands.len() || correlation.right >= operands.len() {
            return Err(DelightQLError::ParseError {
                message: "a set-operation correlation names an arm this run does not have"
                    .to_string(),
                source: None,
                subcategory: None,
            });
        }
    }
    let output_columns = relation_output_columns(&result, ctx)?;
    let names = ctx.names.clone();
    let mut source_columns = Vec::with_capacity(operands.len());
    let mut queries = Vec::with_capacity(operands.len());
    let mut scopes = Vec::with_capacity(operands.len());
    let mut active_columns = Vec::with_capacity(operands.len());
    let mut layouts = Vec::with_capacity(operands.len());
    let mut sites = Vec::with_capacity(operands.len());
    for (index, operand) in operands.into_iter().enumerate() {
        let (query, scope, before, republished, layout, site) =
            operand.lay_out(index, ctx, &names)?;
        source_columns.push(before);
        queries.push(query);
        scopes.push(scope);
        layouts.push(layout);
        sites.push(site);
        active_columns.push(republished);
    }

    let is_minus = matches!(operator, ast_refined::SetOperator::MinusCorresponding);
    // A MINUS EXPORTS ITS LEFT OPERAND and probes its right, so it binds
    // ONE branch and the authority's exact-heading map is its evidence.
    // Every other operator merges, and its contribution table is. Two
    // evidences, ONE binding road: nothing here pairs ports with columns.
    let binding = if is_minus {
        ctx.identities
            .bindings()
            .bind_export(&ctx.identities, &result, &layouts[0])?
    } else {
        ctx.identities
            .bindings()
            .bind_run(&ctx.identities, steps, &layouts)?
    };
    if !is_minus && queries.len() == 2 && correlations.iter().any(|c| c.min_multiplicity) {
        let correlation = &correlations[0];
        return r_lower_intersect_min_multiplicity(
            &correlation.predicate,
            operator,
            queries,
            &source_columns,
            &active_columns,
            &scopes,
            binding,
            &names,
            ctx,
        );
    }

    // A minus never contributes its arm's rows; every other operator does.
    let emitting_arms = if is_minus { 1 } else { queries.len() };
    let mut halves = Vec::with_capacity(emitting_arms);
    for i in 0..emitting_arms {
        let mut probes = Vec::new();
        for correlation in &correlations {
            // Pair-scoped: only a correlation NAMING this arm constrains it.
            let counterpart = match (correlation.left, correlation.right) {
                (left, right) if left == i => right,
                (left, right) if right == i => left,
                _ => continue,
            };
            let condition = scalar::s_lower_boolean(
                correlation.predicate.clone(),
                &ArmPairQualify {
                    identities: &ctx.identities,
                    sites: [sites[i], sites[counterpart]],
                },
                ctx,
            )?
            .into_expr();
            let inner = Builder::from_frozen(
                queries[counterpart].clone(),
                ScopeName::Resolved(scopes[counterpart]),
                active_columns[counterpart].clone(),
                names.clone(),
                std::rc::Rc::clone(&ctx.identities),
            )?
            .add_where(SqlPredicate::new(condition))?
            .project_all()?
            .to_sql()?;
            probes.push(if is_minus {
                SqlDomainExpr::not_exists(inner)
            } else {
                SqlDomainExpr::exists(inner)
            });
        }
        let mut outer = Builder::from_frozen(
            queries[i].clone(),
            ScopeName::Resolved(scopes[i]),
            active_columns[i].clone(),
            names.clone(),
            std::rc::Rc::clone(&ctx.identities),
        )?;
        if !probes.is_empty() {
            outer = outer.add_where(SqlPredicate::new(SqlDomainExpr::and(probes)))?;
        }
        // ONE ROAD. A minus's single branch reads its export binding; every
        // other operator reads its own branch's column of the run's.
        let items = align_arm_items(operator, binding, i, ctx)?;
        // The items already carry the resolver's output occurrences as
        // their aliases — publish that scope. Minting a fresh set here
        // orphans the occurrences every downstream reference was
        // addressed against (each half claims the shared output scope,
        // exactly as the padded arms of the plain corresponding road do).
        halves.push(outer.add_projection_publishing(
            items,
            result.scope(),
            columns_from_relation(&result, ctx)?,
        )?);
    }
    let mut halves = halves.into_iter();
    let mut combined = halves.next().expect("operand count checked");
    let accumulation = crate::pipeline::sql_ast::SetOperator::UnionAll;
    for half in halves {
        // Every half already claims the result's own positions; stacking
        // them keeps that claim rather than minting a scope over it.
        combined =
            combined.stack_at(half, accumulation.clone(), result.scope(), &output_columns)?;
    }
    Ok(combined)
}

fn resolved_column(expression: &ast_refined::DomainExpression) -> Option<crate::names::ColId> {
    match expression {
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Some(column.column()),
        ast_refined::DomainExpression::Reference(Reference::Physical(column)) => Some(*column),
        _ => None,
    }
}

fn intersection_column_pairs(
    expression: &ast_refined::TruthExpression,
    operands: &[Vec<ColumnMetadata>],
    identities: &crate::names::Registry,
) -> Result<Vec<(crate::names::ColId, crate::names::ColId)>> {
    /// WHICH OPERAND THE AUTHOR NAMED. A correlation's references are
    /// resolved against the operands' own scopes, so the operand that
    /// publishes the occurrence IS the operand — asking which heading
    /// CARRIES its value would answer for a sibling that merely holds the
    /// same value.
    fn owner(
        column: crate::names::ColId,
        operands: &[Vec<ColumnMetadata>],
        _identities: &crate::names::Registry,
    ) -> Result<usize> {
        let mut found = operands.iter().enumerate().filter(|(_, heading)| {
            heading
                .iter()
                .any(|candidate| candidate.identity() == column)
        });
        match (found.next(), found.next()) {
            (Some((index, _)), None) => Ok(index),
            (Some(_), Some(_)) => Err(DelightQLError::parse_error(
                "a bag intersection's correlation names a position both operands publish",
            )),
            (None, _) => Err(DelightQLError::parse_error(
                "a bag intersection's correlation names a position neither operand publishes",
            )),
        }
    }

    fn collect(
        expression: &ast_refined::TruthExpression,
        operands: &[Vec<ColumnMetadata>],
        identities: &crate::names::Registry,
        pairs: &mut Vec<(crate::names::ColId, crate::names::ColId)>,
    ) -> Result<()> {
        match expression {
            ast_refined::TruthExpression::Comparison(Comparison {
                operator,
                left,
                right,
            }) => {
                let operator = scalar::s_lower_comparison_op(*operator);
                if !matches!(
                    operator,
                    crate::pipeline::sql_ast::BinaryOperator::Equal
                        | crate::pipeline::sql_ast::BinaryOperator::IsNotDistinctFrom
                ) {
                    return Err(DelightQLError::validation_error_categorized(
                        "setop/min_multiplicity/correlation_operator",
                        "minimum-multiplicity intersection requires equality correlation",
                        "correlate the two operand columns with equality, or turn semantics/min_multiplicity OFF",
                    ));
                }
                let left = resolved_column(left).ok_or_else(|| DelightQLError::ParseError {
                    message: "bag intersection requires column-pair correlation".to_string(),
                    source: None,
                    subcategory: None,
                })?;
                let right = resolved_column(right).ok_or_else(|| DelightQLError::ParseError {
                    message: "bag intersection requires column-pair correlation".to_string(),
                    source: None,
                    subcategory: None,
                })?;
                match (
                    owner(left, operands, identities)?,
                    owner(right, operands, identities)?,
                ) {
                    (0, 1) => pairs.push((left, right)),
                    (1, 0) => pairs.push((right, left)),
                    _ => {
                        return Err(DelightQLError::ParseError {
                            message: "bag intersection correlation must cross its two operands"
                                .to_string(),
                            source: None,
                            subcategory: None,
                        });
                    }
                }
            }
            ast_refined::TruthExpression::Conjunction(parts) => {
                for part in parts.iter() {
                    collect(part, operands, identities, pairs)?;
                }
            }
            _ => {
                return Err(DelightQLError::ParseError {
                    message: "bag intersection requires a conjunction of column pairs".to_string(),
                    source: None,
                    subcategory: None,
                });
            }
        }
        Ok(())
    }

    let mut pairs = Vec::new();
    collect(expression, operands, identities, &mut pairs)?;
    Ok(pairs)
}

fn exact_republication(
    source: crate::names::ColId,
    before: &[ColumnMetadata],
    after: &[ColumnMetadata],
) -> Result<crate::names::ColId> {
    let mut positions = before
        .iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.identity() == source)
        .map(|(position, _)| position);
    match (positions.next(), positions.next()) {
        (Some(position), None) => after
            .get(position)
            .map(ColumnMetadata::identity)
            .ok_or_else(|| {
                DelightQLError::parse_error("an exact republication dropped its source position")
            }),
        (None, _) => Err(DelightQLError::parse_error(
            "an exact republication does not contain its source occurrence",
        )),
        (Some(_), Some(_)) => Err(DelightQLError::parse_error(
            "an exact occurrence appears twice in one physical heading",
        )),
    }
}

fn r_lower_intersect_min_multiplicity(
    correlation: &ast_refined::TruthExpression,
    operator: ast_refined::SetOperator,
    queries: Vec<crate::pipeline::sql_ast::QueryExpression>,
    source_columns: &[Vec<ColumnMetadata>],
    active_columns: &[Vec<ColumnMetadata>],
    scopes: &[crate::names::ScopeId],
    binding: crate::sql_binding::RunBinding,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{
        ordering::OrderDirection, BinaryOperator, DomainExpression as SqlDomainExpr, JoinCondition,
        JoinType,
    };
    let pairs = intersection_column_pairs(correlation, source_columns, &ctx.identities)?;
    let row_scope = ctx.identities.anonymous_scope(None);
    let left_row = ctx
        .identities
        .sql_column(row_scope, None, crate::names::Addressing::Hygienic);
    let right_row = ctx
        .identities
        .sql_column(row_scope, None, crate::names::Addressing::Hygienic);
    let left_base = Builder::from_frozen(
        queries[0].clone(),
        ScopeName::Resolved(scopes[0]),
        active_columns[0].clone(),
        names.clone(),
        std::rc::Rc::clone(&ctx.identities),
    )?;
    let right_base = Builder::from_frozen(
        queries[1].clone(),
        ScopeName::Resolved(scopes[1]),
        active_columns[1].clone(),
        names.clone(),
        std::rc::Rc::clone(&ctx.identities),
    )?;
    let left_partition = pairs
        .iter()
        .map(|(left, _)| {
            exact_republication(*left, &source_columns[0], left_base.columns())
                .map(SqlDomainExpr::Column)
        })
        .collect::<Result<Vec<_>>>()?;
    let right_partition = pairs
        .iter()
        .map(|(_, right)| {
            exact_republication(*right, &source_columns[1], right_base.columns())
                .map(SqlDomainExpr::Column)
        })
        .collect::<Result<Vec<_>>>()?;
    let left_order = left_partition
        .iter()
        .cloned()
        .map(|expr| (expr, OrderDirection::Asc))
        .collect();
    let right_order = right_partition
        .iter()
        .cloned()
        .map(|expr| (expr, OrderDirection::Asc))
        .collect();
    let left = left_base.project_all()?.add_window_column(
        "ROW_NUMBER",
        vec![],
        left_partition,
        left_order,
        left_row,
    )?;
    let left_row = left
        .columns()
        .last()
        .expect("window column is appended")
        .identity();
    let right = right_base.project_all()?.add_window_column(
        "ROW_NUMBER",
        vec![],
        right_partition,
        right_order,
        right_row,
    )?;
    let right_row = right
        .columns()
        .last()
        .expect("window column is appended")
        .identity();
    let left_window_columns = left.columns().to_vec();
    let right_window_columns = right.columns().to_vec();
    let left = left.demote()?.into_join_operand()?;
    let right = right.demote()?.into_join_operand()?;
    let mut conditions = Vec::new();
    for (source_left, source_right) in &pairs {
        let left_column = exact_republication(*source_left, &source_columns[0], left.columns())?;
        let right_column = exact_republication(*source_right, &source_columns[1], right.columns())?;
        conditions.push(SqlDomainExpr::Binary {
            left: Box::new(SqlDomainExpr::Column(left_column)),
            op: BinaryOperator::IsNotDistinctFrom,
            right: Box::new(SqlDomainExpr::Column(right_column)),
        });
    }
    conditions.push(SqlDomainExpr::Binary {
        left: Box::new(SqlDomainExpr::Column(exact_republication(
            left_row,
            &left_window_columns,
            left.columns(),
        )?)),
        op: BinaryOperator::Equal,
        right: Box::new(SqlDomainExpr::Column(exact_republication(
            right_row,
            &right_window_columns,
            right.columns(),
        )?)),
    });
    // The kept rows are the LEFT arm's, shaped to the output heading by the
    // operator's own alignment law — a corresponding union's heading is
    // wider than either arm, so a positional zip would drop its tail.
    // The binding named a column of the LEFT ARM'S BOUNDARY. The window
    // layer and the join operand republish that boundary in order, so the
    // road across is the position — asking which of the operand's columns
    // carries the value would pick between two positions carrying one.
    let across: std::collections::HashMap<_, _> = active_columns[0]
        .iter()
        .map(ColumnMetadata::identity)
        .zip(left.columns().iter().map(ColumnMetadata::identity))
        .collect();
    let output_items = align_arm_items(operator, binding, 0, ctx)?
        .into_iter()
        .map(|item| match item.expr() {
            Some(SqlDomainExpr::Column(source)) => across
                .get(source)
                .map(|column| item.with_expr(SqlDomainExpr::Column(*column)))
                .ok_or_else(|| {
                    DelightQLError::parse_error(
                        "a bound set column is not one the intersection's left operand carries",
                    )
                }),
            _ => Ok(item),
        })
        .collect::<Result<Vec<_>>>()?;
    Builder::from_join(
        left,
        right,
        JoinType::Inner,
        JoinCondition::On(SqlDomainExpr::and(conditions)),
        false,
    )?
    .add_projection(output_items)
}

#[cfg(test)]
mod destructure_mapping_tests {
    use super::make_destructure_shorthand_item;
    use crate::names::{Addressing, Registry};
    use crate::pipeline::ast_refined::{DestructureMapping, LiteralValue};
    use crate::pipeline::sql_ast::{DomainExpression, SelectItem};

    #[test]
    fn shorthand_reads_the_authored_key_when_its_output_name_collides() {
        let registry = Registry::new(&[]);
        let scope = registry.anonymous_scope(None);
        let spelling = registry.intern("def", false);
        let member = crate::relation::named_port(&registry, "def");
        let output = registry.sql_column(scope, Some(spelling), Addressing::Published);
        let source = DomainExpression::literal(LiteralValue::String("{}".to_string()));
        let item = make_destructure_shorthand_item(
            &source,
            member,
            output,
            &[DestructureMapping {
                json_key: "def".to_string(),
                column: member,
            }],
        )
        .unwrap();
        let SelectItem::Publishing {
            expr: DomainExpression::Function { args, .. },
            slot: alias,
            printed: true,
        } = item
        else {
            panic!("expected an aliased json_extract")
        };

        assert_eq!(alias, output);
        assert_eq!(
            args[1],
            DomainExpression::literal(LiteralValue::String("$.def".to_string()))
        );
    }
}
