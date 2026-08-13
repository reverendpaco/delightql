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
    pub cpr_schema: <P as crate::pipeline::asts::core::Phase>::Scope,
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
    wrap_origin, Alignment, Builder, NameGenerator, Projected, Publication, Qualify, ScopeName,
    Unprojected,
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

fn lower_join_type(
    join_type: Option<ast_refined::JoinType>,
) -> crate::pipeline::sql_ast::JoinType {
    use crate::pipeline::sql_ast::JoinType as SqlJoinType;

    match join_type {
        None | Some(ast_refined::JoinType::Inner) => SqlJoinType::Inner,
        Some(ast_refined::JoinType::LeftOuter) => SqlJoinType::Left,
        Some(ast_refined::JoinType::RightOuter) => SqlJoinType::Right,
        Some(ast_refined::JoinType::FullOuter) => SqlJoinType::Full,
    }
}

/// The relation the outermost node of a chain publishes.
#[stacksafe::stacksafe]
pub(crate) fn extract_cpr_schema(expr: &ast_refined::Chain) -> crate::names::ScopeId {
    use crate::pipeline::asts::core::expressions::relational::Relation;
    use crate::pipeline::asts::core::Grelex;
    if let Some(continuation) = expr.continuations.last() {
        return *continuation
            .cpr_schema()
            // Only an ER edge carries no publication, and an edge is
            // expanded into ordinary members by the resolver.
            .expect("an ER edge cannot reach lowering");
    }
    match &expr.head {
        Grelex::Literal(anon) => anon.table.cpr_schema,
        Grelex::Reference(rel) => match rel {
            Relation::Ground { cpr_schema, .. }
            | Relation::InnerRelation { cpr_schema, .. }
            | Relation::FunctorCall { cpr_schema, .. } => *cpr_schema,
            Relation::ConsultedView { scoped, .. } => *scoped,
        },
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

    SelectItem::Expression {
        expr: qualified_col_expr(col),
        alias: Some(col.identity()),
    }
}

/// Project builder columns according to the scope a segment publishes.
///
/// That heading is the resolver's authoritative answer about which columns
/// survive and in what order. This function matches each published column to
/// the corresponding builder column using original/provenance names (which are
/// stable across the transformer's `_2` disambiguation).
///
/// Used by pipe operators that filter or reorder columns (project-out,
/// reposition, rename-cover, etc.) to ensure the transformer respects
/// the resolver's decisions.
fn select_items_from_cpr_schema(
    builder_columns: &[ColumnMetadata],
    cpr_schema: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Result<Vec<crate::pipeline::sql_ast::SelectItem>> {
    let schema_columns = identities.known_heading(cpr_schema)?;

    let mut items = Vec::with_capacity(schema_columns.len());
    let mut used = vec![false; builder_columns.len()];
    for target in schema_columns {
        let found_idx = builder_columns
            .iter()
            .enumerate()
            .position(|(idx, candidate)| {
                !used[idx] && identities.same_value(candidate.identity(), target)
            });
        let Some(idx) = found_idx else {
            crate::probe::probe!(
                published,
                "unpaired {:?} among {:?}",
                crate::probe::chain(identities, target),
                builder_columns
                    .iter()
                    .map(|c| crate::probe::chain(identities, c.identity()))
                    .collect::<Vec<_>>()
            );
            return Err(DelightQLError::parse_error(
                "a published column has no column of this statement to stand for it",
            ));
        };
        used[idx] = true;
        let bc = &builder_columns[idx];
        items.push(crate::pipeline::sql_ast::SelectItem::Expression {
            expr: qualified_col_expr(bc),
            alias: Some(target),
        });
    }

    Ok(items)
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
    cpr_schema: crate::names::ScopeId,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::table::TvfArgument;

    let arguments: Vec<TvfArgument> = ho_arguments
        .ho_members()
        .filter_map(|arg| match arg {
            // A TVF argument is a resolved column or a literal, and a
            // crossing is neither — it says so here rather than reaching a
            // conversion that would make one look like a value.
            crate::pipeline::asts::core::operators::HoArgument::Value(
                crate::pipeline::asts::core::ArgumentValue::Truth(_),
            ) => Some(Err(DelightQLError::ParseError {
                message: "a TVF argument is a resolved column or a literal; a truth read \
                          as a value is neither"
                    .to_string(),
                source: None,
                subcategory: None,
            })),
            crate::pipeline::asts::core::operators::HoArgument::Value(
                crate::pipeline::asts::core::ArgumentValue::Domain { value, .. },
            ) => Some(lower_tvf_argument(value.clone())),
            crate::pipeline::asts::core::operators::HoArgument::Relation(_) => None,
            crate::pipeline::asts::core::operators::HoArgument::Skip => None,
            crate::pipeline::asts::core::operators::HoArgument::Landing(landing) => {
                match *landing {}
            }
        })
        .collect::<Result<_>>()?;

    let scope = cpr_schema;

    let table_expr = TableExpression::TVF {
        function,
        arguments,
        alias: scope,
    };

    let columns = columns_from_cpr_schema(cpr_schema, &ctx.identities);

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
        ))) => Ok(TvfArgument::Column(column)),
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
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    r_lower_anonymous(anon.table.body.rows, anon.table.cpr_schema, names, ctx)
}

fn r_lower_anonymous(
    rows: crate::pipeline::asts::vocabulary::Vec1<
        crate::pipeline::asts::core::TabularRow<crate::pipeline::asts::core::Datum<Refined>>,
    >,
    cpr_schema: crate::names::ScopeId,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{
        query::SetOperator, QueryExpression, SelectBuilder, SelectItem,
    };

    let scope = cpr_schema;
    let output_columns = cpr_output_columns(cpr_schema, &ctx.identities);

    let dummy = DummyQualify(&ctx.identities);
    // The publication belongs to the union's result, not to any one row: the
    // first row publishes it and every row after aligns with it.
    let published = Publication::at(
        scope,
        output_columns
            .iter()
            .copied()
            .map(ColumnMetadata::new)
            .collect(),
        &ctx.identities,
    )?;

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
                sb = sb.select(SelectItem::Expression {
                    expr: sql_expr,
                    alias,
                });
            } else {
                sb = sb.select(SelectItem::Expression {
                    expr: sql_expr,
                    alias: None,
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
            Alignment::with(&published).align(sb)?
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

    let columns = columns_from_cpr_schema(cpr_schema, &ctx.identities);

    Builder::from_frozen(
        query,
        ScopeName::Resolved(scope),
        columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
    )
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
    cpr_schema: crate::names::ScopeId,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use super::descend;

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

    let scope = cpr_schema;

    // A correlation carrier rides this relation's boundary, so this scope is
    // where the refiner put it and this scope is what says whether one
    // exists. The synthesized passthrough projection has to keep it: the
    // hoisted condition above names it, and a projection that drops it leaves
    // that condition standing on nothing.
    let carries_correlation =
        !super::builder::correlation_carriers(scope, &ctx.identities)?.is_empty();

    // Recursive descent into the subquery — same path as any exterior query.
    let inner_names = names.fork();
    let inner_builder = if carries_correlation {
        descend::descend_as_query_carrying_hygiene(*subquery, &inner_names, ctx)?
    } else {
        descend::descend_as_query(*subquery, &inner_names, ctx)?
    };
    let cpr_columns = columns_from_cpr_schema(cpr_schema, &ctx.identities);
    let cpr_output = cpr_output_columns(cpr_schema, &ctx.identities);

    // Compare inner output names with the published heading. If they differ
    // (e.g., the heading says "fn" but inner outputs "first_name"), inject a
    // rename projection so the finalized SQL outputs the published names.
    let query = reconcile_heading(inner_builder, &cpr_output)?;

    // Hygienic columns (__dql_corr_0 etc.) are in the subquery output for
    // JOIN ON but NOT in the published scope. The Qualify fallback uses the
    // scope's own name as qualifier, so join conditions still resolve correctly.

    // Return as Table with subquery — not Frozen. This way, the join
    // handler's into_table_expr() passes the TableExpression through
    // directly instead of wrapping it again with a generated alias.
    let table_expr = TableExpression::subquery(query, scope);
    Builder::from_table(
        table_expr,
        ScopeName::Resolved(scope),
        cpr_columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
    )
}

/// Lower a ConsultedView: view body inlined as a subquery, reconciled
/// against the boundary the resolver published.
fn r_lower_consulted_view(
    body: ast_refined::Query,
    scoped: crate::names::ScopeId,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let scope = scoped;
    let cpr_columns = columns_from_cpr_schema(scope, &ctx.identities);
    let cpr_output = cpr_output_columns(scope, &ctx.identities);

    let body_sql = {
        let ast_refined::Query { cfes: (), ctes, body } = body;
        let sql_ctes: Vec<crate::pipeline::sql_ast::Cte> = ctes
            .into_iter()
            .map(|binding| lower_cte_binding(binding, names, ctx))
            .collect::<Result<_>>()?;

        let inner_builder = super::descend::descend_as_final(body, names, ctx)?;
        let main_query = reconcile_heading(inner_builder, &cpr_output)?;

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
    table_expr: TableExpression,
    access: &ast_refined::Access,
    scope: crate::names::ScopeId,
    cpr_schema: crate::names::ScopeId,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectBuilder, SelectItem,
    };
    let columns = columns_from_cpr_schema(cpr_schema, &ctx.identities);
    let mut select_items = Vec::new();
    let mut read_scopes: Vec<crate::names::ScopeId> = Vec::new();
    for col in &columns {
        let output = col.identity();
        let source = match ctx.identities.origin_of_col(output) {
            crate::names::ColumnOrigin::Republished { from, .. } => from,
            _ => output,
        };
        let read = ctx.identities.scope_of(source);
        if !read_scopes.contains(&read) {
            read_scopes.push(read);
        }
        select_items.push(SelectItem::Expression {
            expr: SqlDomainExpr::Column(source),
            alias: Some(output),
        });
    }

    // `scope` is what this SELECT PUBLISHES; the select items read the source
    // occurrences the pattern republished from. Those are two different scopes
    // whenever a pattern renames, so the FROM must name the one being read.
    // Naming the published scope puts the projection's own output in its own
    // FROM, and every reference in the list is then owned by a scope the
    // statement never brought into view.
    //
    // A pattern binds the slots of one relation, so its heading reads exactly
    // one scope: either the relation `table_expr` names, or the scope those
    // slots were republished from. Anything else has no FROM this select list
    // can be read against, and refusing here says so where it is known rather
    // than emitting a statement for the self-check to reject.
    let from = match read_scopes.as_slice() {
        [read] if *read == scope => table_expr,
        // The scope read is a relation like any other, and how it is read is
        // the same question asked of `scope` above. Naming it instead spells
        // the occurrence, and an occurrence of something already named holds
        // no spelling of its own.
        [read] => ground_table_expression(*read, &ctx.identities),
        sources => {
            return Err(DelightQLError::ParseError {
                message: format!(
                    "a positional pattern's heading reads {} source scopes; \
                     one relation's pattern reads one",
                    sources.len()
                ),
                source: None,
                subcategory: None,
            })
        }
    };

    // THE CROSSED SLOT UNIFIES, NULL-SAFELY. The slot carries the column and
    // the truth read as a VALUE, and the comparison between them is spelled
    // HERE — where the source column is already in scope — rather than at
    // resolution, where the value operand would have to be able to hold a
    // truth. Plain SQL equality is the wrong operator: unification answers
    // yes when a null meets a null, and `=` answers unknown.
    let read = read_scopes.first().copied().unwrap_or(scope);
    let unifications = slot_unifications(access, read, ctx)?;

    let mut select = SelectBuilder::new()
        .select_all(select_items)
        .from_tables(vec![from]);
    if let Some(condition) = unifications {
        select = select.where_clause(condition);
    }
    let stmt = super::builder::publish_at(
        scope,
        columns.iter().map(ColumnMetadata::identity),
        select,
        &ctx.identities,
    )?;

    let query = QueryExpression::Select(Box::new(stmt));
    Builder::from_frozen(
        query,
        ScopeName::Resolved(scope),
        columns,
        names.fork(),
        std::rc::Rc::clone(&ctx.identities),
    )
}

/// The null-safe unifications a pattern's CROSSED slots state, conjoined.
///
/// Every other slot kind was spent at resolution — a binder became a
/// republished occurrence, a literal and a term became filters. The crossing
/// is the one whose comparison waits, because the exact slot is what carries
/// both of its operands.
fn slot_unifications(
    access: &ast_refined::Access,
    read: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Option<crate::pipeline::sql_ast::DomainExpression>> {
    use crate::pipeline::asts::core::{Slot, SlotConstraint};
    use crate::pipeline::sql_ast::DomainExpression as SqlDomainExpr;

    let ast_refined::Access::Slots(slots) = access else {
        return Ok(None);
    };
    // The crossing's interior was resolved against the relation's OWN
    // heading, which is the one this select reads from.
    let qualify = HeadingQualify {
        identities: &ctx.identities,
        columns: columns_from_cpr_schema(read, &ctx.identities),
    };
    let mut conditions = Vec::new();
    for slot in slots.iter() {
        let Slot::Constraint(SlotConstraint::Truth { column, value }) = slot else {
            continue;
        };
        // The truth becomes ONE operand, so it wears its own parentheses:
        // `a IS NOT DISTINCT FROM b IS NOT DISTINCT FROM c` re-associates
        // into a different question.
        let crossed = SqlDomainExpr::Parens(Box::new(
            super::scalar::s_lower_boolean(value.truth().clone(), &qualify, ctx)?.into_expr(),
        ));
        conditions.push(SqlDomainExpr::Column(*column).is_not_distinct_from(crossed));
    }
    Ok(match conditions.len() {
        0 => None,
        1 => conditions.pop(),
        _ => Some(SqlDomainExpr::and(conditions)),
    })
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
    scope: crate::names::ScopeId,
    cte: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> TableExpression {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, SelectStatement,
    };
    let inner = identities.heading(cte).columns_seen();
    let outer = identities.heading(scope).columns_seen();
    if inner.is_empty() || outer.is_empty() {
        return TableExpression::Scope(scope);
    }
    let matched: Vec<Vec<crate::names::ColId>> = outer
        .iter()
        .map(|target| {
            inner
                .iter()
                .copied()
                .filter(|source| identities.republishes(*target, *source))
                .collect()
        })
        .collect();
    if matched.iter().any(|sources| sources.len() > 1) {
        return TableExpression::Scope(scope);
    }
    let pairs: Vec<_> = if matched.iter().all(|sources| sources.len() == 1) {
        matched
            .iter()
            .zip(outer.iter())
            .map(|(sources, target)| (sources[0], *target))
            .collect()
    } else {
        let positional_agrees = inner.len() == outer.len()
            && matched.iter().enumerate().all(|(index, sources)| {
                sources.is_empty()
                    || sources.first().copied().is_some_and(|source| {
                        inner
                            .iter()
                            .nth(index)
                            .is_some_and(|column| source == *column)
                    })
            });
        if !positional_agrees {
            return TableExpression::Scope(scope);
        }
        inner.iter().copied().zip(outer.iter().copied()).collect()
    };
    let items: Vec<SelectItem> = pairs
        .into_iter()
        .map(|(source, target)| SelectItem::Expression {
            expr: SqlDomainExpr::Column(source),
            alias: Some(target),
        })
        .collect();
    match super::builder::publish_at(
        scope,
        outer.iter().copied(),
        SelectStatement::builder()
            .select_all(items)
            .from_tables(vec![TableExpression::Scope(cte)]),
        identities,
    ) {
        Ok(select) => TableExpression::subquery(QueryExpression::Select(Box::new(select)), scope),
        Err(_) => TableExpression::Scope(scope),
    }
}

fn ground_table_expression(
    scope: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> TableExpression {
    // A ground names the entity it reads, and an alias beside it so references
    // have something to qualify by. Emitting the scope alone leaves the FROM
    // entry spelled by whatever name the scope is given — and a scope competing
    // for the table's own name loses it, so the statement reads from a table
    // that does not exist. The entity is not a name to be assigned; it is the
    // one thing here that already has one.
    crate::probe::probe!(
        ground,
        "{scope:?} {:?} heading={:?}",
        identities.origin_of(scope),
        identities.heading(scope)
    );
    match identities.origin_of(scope) {
        crate::names::ScopeOrigin::BaseTable { entity } => {
            if identities.is_annihilated(scope) {
                annihilated_read(entity, scope)
            } else {
                TableExpression::Entity {
                    entity,
                    alias: Some(scope),
                }
            }
        }
        crate::names::ScopeOrigin::UserAlias { of } => {
            match identities.origin_of(of) {
                crate::names::ScopeOrigin::BaseTable { entity } => {
                    if identities.is_annihilated(scope) || identities.is_annihilated(of) {
                        annihilated_read(entity, scope)
                    } else {
                        TableExpression::Entity {
                            entity,
                            alias: Some(scope),
                        }
                    }
                }
                // An occurrence of a CTE is a second scope over a relation that
                // already has a name, and a FROM entry naming the occurrence
                // instead names nothing: the CTE holds the spelling, so the
                // occurrence is given a disambiguated one and the statement
                // reads from a table no WITH clause defines. A subquery over
                // the CTE puts the occurrence where SQL can carry it — on an
                // alias — and re-publishes the heading under it.
                crate::names::ScopeOrigin::Cte { .. } => cte_occurrence(scope, of, identities),
                _ => TableExpression::Scope(scope),
            }
        }
        _ => TableExpression::Scope(scope),
    }
}

/// into a fresh `Builder<Unprojected>`.
///
/// This is the leaf case — the base of the dive-and-bubble recursion.
pub(super) fn r_lower_read(
    rel: ast_refined::Relation,
    access: Option<ast_refined::Access>,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    match rel {
        ast_refined::Relation::Ground { cpr_schema, .. } => {
            let access = access.unwrap_or(ast_refined::Access::All);
            let scope = cpr_schema;
            let table_expr = ground_table_expression(scope, &ctx.identities);

            // A caller pattern: emit SELECT original AS alias for each column
            if matches!(access, ast_refined::Access::Slots(_)) {
                return r_lower_positional_relation(
                    table_expr, &access, scope, cpr_schema, names, ctx,
                );
            }

            // Glob/bare: all columns, no rename
            let columns = columns_from_cpr_schema(cpr_schema, &ctx.identities);

            Builder::from_table(
                table_expr,
                ScopeName::Resolved(scope),
                columns,
                names.fork(),
                std::rc::Rc::clone(&ctx.identities),
            )
        }

        ast_refined::Relation::InnerRelation {
            pattern,
            cpr_schema,
            ..
        } => r_lower_inner_relation(pattern, cpr_schema, names, ctx),

        ast_refined::Relation::ConsultedView { body, scoped, .. } => {
            r_lower_consulted_view(*body, scoped, names, ctx)
        }

        ast_refined::Relation::FunctorCall {
            call,
            alias: (),
            cpr_schema,
        } => {
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
            r_lower_tvf(function, call.arguments, cpr_schema, names, ctx)
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
pub(super) fn cte_cpr_columns(
    expr: &ast_refined::Chain,
    identities: &crate::names::Registry,
) -> Vec<crate::names::ColId> {
    cpr_output_columns(extract_cpr_schema(expr), identities)
}

/// Argumentative binding on the recursive
/// self-reference (`c(m)` inside c's own definition) does not bind today:
/// the rename mis-merges into a NULL-padded two-column union and returns
/// SILENTLY WRONG results. Hard-refuse until the rename-hoist legalization
/// (`WITH c(m) AS (…)` — needs the Cte column list) lands. Checked here,
/// at the one site that lowers every CTE binding, with its own walk — the
/// upstream is_recursive flag is not trusted (it historically never
/// engaged).
fn check_recursive_argumentative_binding(
    binding: &ast_refined::CteBinding,
    identities: &crate::names::Registry,
) -> Result<()> {
    let binding_scope = binding.subject;
    if expr_has_positional_self_ref(&binding.expression, binding_scope, identities) {
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
    if let ast_refined::Grelex::Reference(rel) = &expr.head {
        if read_is_positional_self_ref(rel, expr.head_access(), binding, identities) {
            return true;
        }
    }
    expr.continuations
        .iter()
        .any(|continuation| match continuation {
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
    access: Option<&ast_refined::Access>,
    binding: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> bool {
    match rel {
        ast_refined::Relation::Ground { cpr_schema, .. } => {
            matches!(access, Some(ast_refined::Access::Slots(_)))
                && identities.contains_scope(*cpr_schema, binding)
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
pub(super) fn lower_cte_binding(
    binding: ast_refined::CteBinding,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<crate::pipeline::sql_ast::Cte> {
    check_recursive_argumentative_binding(&binding, &ctx.identities)?;
    let cte_scope = binding.subject;
    // What a CTE publishes is its binding's heading — that is what every
    // reference through the name was addressed against. The body's own schema
    // answers only where the binding has none: reconciling to the body instead
    // leaves the CTE emitting occurrences its own name does not carry.
    crate::probe::probe!(
        recursion,
        "binding {cte_scope:?} {:?}",
        crate::probe::scope_chain(&ctx.identities, cte_scope)
    );
    let cte_cpr = match ctx.identities.known_heading(cte_scope)? {
        heading if !heading.is_empty() => heading,
        _ => crate::names::Candidates::from_vec(cte_cpr_columns(
            &binding.expression,
            &ctx.identities,
        )),
    };
    let inner_builder = super::descend::descend_as_final(binding.expression, names, ctx)?;
    let cte_query = if cte_cpr.is_empty() {
        inner_builder.to_sql()?
    } else {
        publish_cte_body(inner_builder, cte_scope, &cte_cpr.to_vec())?
    };
    // The resolver's stored decision, read — not re-derived.
    Ok(if binding.recursion.is_recursive() {
        crate::pipeline::sql_ast::Cte::new_recursive(cte_scope, cte_query)
    } else {
        crate::pipeline::sql_ast::Cte::new(cte_scope, cte_query)
    })
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
fn publish_cte_body(
    inner_builder: Builder<Projected>,
    cte_scope: crate::names::ScopeId,
    cte_cpr: &[crate::names::ColId],
) -> Result<crate::pipeline::sql_ast::QueryExpression> {
    let identities = std::rc::Rc::clone(inner_builder.identities());
    let outputs: Vec<_> = inner_builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .collect();
    if !heading_lines_up(&outputs, cte_cpr, &identities) {
        return reconcile_heading(inner_builder, cte_cpr);
    }
    let pairs: Vec<_> = outputs
        .iter()
        .copied()
        .zip(cte_cpr.iter().copied())
        .collect();
    let mut query = inner_builder.to_sql()?;
    super::builder::state::rewrite_output_aliases(&mut query, cte_scope, &pairs, &identities)?;
    Ok(query)
}

/// Reconcile an inner query's output with the heading its caller publishes.
///
/// THE reconciliation — every lowering road arrives here, and which
/// strategy applies is decided from the schema itself rather than from
/// which lowering function happens to be running: deciding by AST shape
/// at each call site instead tempts a regression, where a road added
/// later gets whichever strategy its author copied.
///
/// A published heading does not always describe the inner output index-for-index:
/// a positional caller pattern with discards keeps a SUBSET of the body
/// heading, so the source column must be IDENTIFIED, never taken from the
/// list index. Zipping by index misbinds — it shifts every binding after a
/// non-trailing discard.
///
/// The key is occurrence lineage: each published column is paired with the
/// nearest unconsumed inner occurrence on its republication chain. Discarded
/// heading columns are dropped here, not by downstream narrowing; unconsumed
/// hygienic columns pass through because a JOIN ON above may still need them.
///
/// Falls back to the aligned-rename reconciliation when neither key
/// addresses the inner output unambiguously.
pub(super) fn reconcile_heading(
    inner_builder: Builder<Projected>,
    cpr_columns: &[crate::names::ColId],
) -> Result<crate::pipeline::sql_ast::QueryExpression> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    if cpr_columns.is_empty() {
        return inner_builder.to_sql();
    }

    let identities = std::rc::Rc::clone(inner_builder.identities());
    let inner_columns: Vec<_> = inner_builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .collect();
    if inner_columns == cpr_columns {
        return inner_builder.to_sql();
    }

    crate::probe::probing!(reconcile, {
        crate::probe::probe!(
            reconcile,
            "reconcile into {:?}",
            crate::probe::scope_chain(&identities, identities.scope_of(cpr_columns[0]))
        );
        for target in cpr_columns {
            crate::probe::probe!(
                reconcile,
                "publishes {:?} {:?}",
                identities.addressing(*target),
                crate::probe::chain(&identities, *target)
            );
        }
        for candidate in &inner_columns {
            crate::probe::probe!(
                reconcile,
                "  inner  {:?} {:?}",
                identities.addressing(*candidate),
                crate::probe::chain(&identities, *candidate)
            );
        }
    });
    let mut consumed = std::collections::HashSet::new();
    let mut items = Vec::with_capacity(cpr_columns.len());
    let mut kept = Vec::with_capacity(cpr_columns.len());
    for (position, target) in cpr_columns.iter().enumerate() {
        // Pair by where the two meet, nearest first.
        //
        // Sharing a value is too coarse on its own: a higher-order view called
        // `f(x(*), x(*))` puts two inner columns carrying ONE value in front
        // of two published slots, and both answer "same value" for both slots.
        // They are told apart by WHERE they diverge — one published column and
        // the inner column it stands for meet at the argument they came
        // through, which is nearer the leaves than the single source they
        // ultimately share.
        //
        // So walk the published column's own chain outward and stop at the
        // first ancestor any inner column carries. Candidates only accumulate
        // as the walk descends — everything on an ancestor's chain is on its
        // descendant's — so the first non-empty answer is the nearest one, and
        // its size decides. Being the target itself is the first step, being
        // on its chain is a later one, and sharing only a progenitor is the
        // last: three tiers, one rule, ordered by construction.
        let hits = {
            let mut cur = *target;
            loop {
                let hits: Vec<_> = inner_columns
                    .iter()
                    .copied()
                    .filter(|candidate| {
                        !consumed.contains(candidate) && identities.republishes(*candidate, cur)
                    })
                    .collect();
                if !hits.is_empty() {
                    break hits;
                }
                match identities.origin_of_col(cur) {
                    crate::names::ColumnOrigin::Republished { from, .. } => cur = from,
                    _ => break Vec::new(),
                }
            }
        };
        let source = match hits.as_slice() {
            [source] => *source,
            [] if inner_columns.len() == cpr_columns.len() => inner_columns[position],
            // A hygienic target no inner column carries is a spent carrier:
            // the constraint that read it was applied inside the body, whose
            // final projection rightly dropped it, while the published heading
            // was minted from the pre-narrowing scope and still lists it.
            // Nothing above addresses a carrier, so the slot is omitted rather
            // than refused. A carrier still riding — one the body DOES offer —
            // never reaches this arm; it pairs like any other column or passes
            // through unconsumed below.
            [] if identities.addressing(*target) == crate::names::Addressing::Hygienic => continue,
            [] => {
                return Err(DelightQLError::parse_error(
                    "An inner heading cannot be reconciled with its published subset",
                ))
            }
            _ => {
                return Err(DelightQLError::validation_error_categorized(
                    "resolution/ambiguous",
                    "More than one inner column carries the published output value",
                    "disambiguate the inner heading before publishing it",
                ))
            }
        };
        consumed.insert(source);
        kept.push(*target);
        items.push(SelectItem::Expression {
            expr: SqlDomainExpr::Column(source),
            alias: Some(*target),
        });
    }
    // A hygienic column rides along in the SQL because a JOIN ON above still
    // needs it, but riding along under its OWN occurrence claims it as an
    // output of the scope this statement stands at, which does not own it.
    // Republishing costs the same act as emitting it and is the only spelling
    // that leaves the statement outputting what it says it outputs.
    let published = identities.scope_of(cpr_columns[0]);
    for column in inner_columns {
        if !consumed.contains(&column)
            && identities.addressing(column) == crate::names::Addressing::Hygienic
        {
            let carried = identities.republish_column(
                column,
                published,
                crate::names::Republish::Passthrough,
                identities.published(column),
                crate::names::Addressing::Hygienic,
                |_| {},
            );
            items.push(SelectItem::Expression {
                expr: SqlDomainExpr::Column(column),
                alias: Some(carried),
            });
            // A carrier the statement emits is an output of the statement.
            // Leaving it out of the heading while emitting it is the split
            // this authority exists to close: the layer would advertise less
            // than it produces, and the wrap above would read the shorter
            // list. It enters as a hygienic output, which is what keeps it
            // out of the VIEW without keeping it out of the publication.
            kept.push(carried);
        }
    }

    // Publish the heading just reconciled to, rather than minting over it.
    // `add_projection` re-aliases every item into a scope of its own, which
    // would throw away the correspondence built above and leave the statement
    // publishing occurrences no caller was addressed against — reconciling to
    // a heading and then not publishing it is the same act as not reconciling.
    if kept
        .iter()
        .all(|column| identities.scope_of(*column) == published)
    {
        let columns = kept
            .iter()
            .map(|column| ColumnMetadata::new(*column))
            .collect();
        return inner_builder
            .add_projection_publishing(items, published, columns)?
            .to_sql();
    }
    inner_builder.add_projection(items)?.to_sql()
}

/// Extract `Vec<ColumnMetadata>` from a scope, pushing a scope transition
/// onto each column's identity stack so the qualifier reflects the given scope.
///
/// This is the translation boundary: the scope the refiner bound flows in, and
/// the builder gets `Vec<ColumnMetadata>` with the identity stack updated to
/// reflect the current SQL scope. No information is discarded.
pub(super) fn columns_from_cpr_schema(
    schema: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Vec<ColumnMetadata> {
    identities
        .heading(schema)
        .columns_seen()
        .into_iter()
        .map(ColumnMetadata::new)
        .collect()
}

fn cpr_output_columns(
    schema: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Vec<crate::names::ColId> {
    identities.heading(schema).columns_seen()
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
        ast_refined::OutItem::One(one) => one.expr.into_domain(),
        ast_refined::OutItem::Many(_) | ast_refined::OutItem::Whole => None,
    }
}

pub(super) fn alias_unaliased(
    item: &mut crate::pipeline::sql_ast::SelectItem,
    column: crate::names::ColId,
) {
    if let crate::pipeline::sql_ast::SelectItem::Expression {
        alias: alias @ None,
        ..
    } = item
    {
        *alias = Some(column);
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
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let (child, condition) = anchors::publishing_in_condition(child, condition, ctx)?;
    let predicate = scalar::s_lower_boolean(condition, &child, ctx)?;
    child.add_where(predicate)
}

/// Lower a row bound: `#<n` is LIMIT n, `#>n` its OFFSET.
/// THE OPERATOR SAYS WHICH BOUND IT IS.
///
/// `#<n` caps the rows; `#>n` says where the count starts and selects no
/// maximum. Both are bounds and both denote a relation, so each stands on
/// its own level unless the refiner has already composed a skip into the
/// cap that follows it.
pub(super) fn r_lower_bound(
    child: Builder<Unprojected>,
    bound: crate::pipeline::asts::core::TupleOrdinalClause,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::asts::core::TupleOrdinalOperator;

    match bound.operator {
        TupleOrdinalOperator::LessThan => child.add_limit(bound.value, bound.offset),
        TupleOrdinalOperator::GreaterThan => child.add_offset(bound.value),
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
    correlation: Option<ast_refined::MemberCorrelation>,
    join_type: Option<ast_refined::JoinType>,
    #[expect(
        unused_variables,
        reason = "join operands currently determine the lowered heading"
    )]
    cpr_schema: crate::names::ScopeId,
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

    // Lower the join condition against the POST-WRAP scopes.
    // ChainedQualify lives in the builder module — the qualify logic stays
    // in one place instead of being reimplemented here.
    let condition =
        match correlation {
            Some(ast_refined::MemberCorrelation::Correspond(correspondence)) => {
                let mut using_columns = Vec::new();
                for name in correspondence.columns {
                    // Name-answering occurrences take the key. A chained USING
                    // join carries the previously merged key as a HYGIENIC
                    // rider, and counting riders alongside the answering
                    // occurrence would refuse every second join on the same
                    // key as ambiguous. But a heading can also carry the key
                    // ONLY hygienically (a constrained pattern slot mints its
                    // column hygienic), so when nothing answers the name, a
                    // sole hygienic occurrence still serves.
                    let hits = |columns: &[ColumnMetadata]| -> Vec<crate::names::ColId> {
                        let named: Vec<_> = columns
                            .iter()
                            .map(ColumnMetadata::identity)
                            .filter(|column| ctx.identities.published_sym(*column) == Some(name))
                            .collect();
                        let answering: Vec<_> = named
                            .iter()
                            .copied()
                            .filter(|column| {
                                ctx.identities.addressing(*column)
                                    != crate::names::Addressing::Hygienic
                            })
                            .collect();
                        if answering.is_empty() {
                            named
                        } else {
                            answering
                        }
                    };
                    let left_hits = hits(&left_op.columns);
                    let right_hits = hits(&right_op.columns);
                    let left = match left_hits.as_slice() {
                        [column] => *column,
                        [] => {
                            return Err(DelightQLError::parse_error(
                                "A resolved USING column is absent from the left heading",
                            ))
                        }
                        _ => return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            "A resolved USING column appears more than once in the left heading",
                            "publish a unique join key before lowering",
                        )),
                    };
                    match right_hits.as_slice() {
                        [_] => {}
                        [] => {
                            return Err(DelightQLError::parse_error(
                                "A resolved USING column is absent from the right heading",
                            ))
                        }
                        _ => return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            "A resolved USING column appears more than once in the right heading",
                            "publish a unique join key before lowering",
                        )),
                    }
                    using_columns.push(left);
                }
                if sql_join_type == SqlJoinType::Full {
                    // Full outer must project USING columns as COALESCE —
                    // either side's orphan rows carry the key alone.
                    return Builder::from_join_full_outer_using(left_op, right_op, using_columns)?
                        .demote();
                }
                SqlJoinCondition::Using(using_columns)
            }
            Some(ast_refined::MemberCorrelation::Condition(bool_expr)) => {
                let pred = scalar::s_lower_boolean(bool_expr, &combined, ctx)?;
                SqlJoinCondition::On(pred.into_expr())
            }
            None => SqlJoinCondition::Natural,
        };

    Builder::from_join(left_op, right_op, sql_join_type, condition)
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
    correlation: Option<ast_refined::MemberCorrelation>,
    join_type: Option<ast_refined::JoinType>,
    cpr_schema: crate::names::ScopeId,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let ast_refined::AnonRelation { table, alias, .. } = anon;
    let rows = table.body.rows;
    let anon_cpr_schema = table.cpr_schema;

    // Check if any row value contains a column reference.
    let has_column_refs = rows
        .iter()
        .any(|row| row.iter().any(|v| contains_column_reference(&v.value())));

    if !has_column_refs {
        // No correlated refs — use normal UNION ALL path.
        let right = r_lower_anonymous(rows, anon_cpr_schema, names, ctx)?;
        return r_lower_join(left, right, correlation, join_type, cpr_schema, ctx);
    }

    // --- JSON melt path ---
    r_lower_melt_join(
        left,
        rows,
        alias,
        anon_cpr_schema,
        correlation,
        join_type,
        cpr_schema,
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
    anon_cpr_schema: crate::names::ScopeId,
    correlation: Option<ast_refined::MemberCorrelation>,
    join_type: Option<ast_refined::JoinType>,
    cpr_schema: crate::names::ScopeId,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, JoinCondition as SqlJoinCondition, QueryExpression,
        SelectItem, SelectStatement,
    };

    let source_metadata = left.columns().to_vec();
    let input_scope = ColumnMetadata::common_identity_scope(&source_metadata, &ctx.identities)
        .unwrap_or_else(|| {
            ctx.identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
                None,
            )
        });
    // The packet is an output of this projection, not an extra output of the
    // relation being read. Owning it by the input mutates that input's
    // heading; a second melt over the same occurrence then mistakes the first
    // packet for caller data and emits a reference no input table publishes.
    let packet_scope = ctx.identities.mint_derived_scope(
        crate::names::ScopeOrigin::Wrap {
            input: input_scope,
            why: crate::names::WrapReason::Pivot,
        },
        crate::names::Hint::None,
    );
    let packet = ctx.identities.mint_column(
        packet_scope,
        crate::names::ColumnOrigin::Minted {
            by: crate::names::MintReason::Pivot,
        },
        None,
        crate::names::Addressing::Hygienic,
        crate::names::ValueFacts::default(),
    );
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
    source_items.push(SelectItem::Expression {
        expr: SqlDomainExpr::function("json_array", row_exprs),
        alias: Some(packet),
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
        crate::names::Republish::BoundaryExport,
    )?
    .into_iter()
    .map(|column| column.identity())
    .collect();
    let wrapped_packet = *wrapped_columns.last().expect("packet column exists");

    let tvf_scope = names
        .fresh(crate::names::ScopeOrigin::Interior {
            of: projected_packet,
        })
        .identity();
    let value_spelling = ctx.identities.intern("value", false);
    let value_column = ctx.identities.mint_column(
        tvf_scope,
        crate::names::ColumnOrigin::Computed {
            via: crate::names::Computation::Function,
        },
        Some(value_spelling),
        crate::names::Addressing::Published,
        crate::names::ValueFacts::default(),
    );

    let output_ids = cpr_output_columns(cpr_schema, &ctx.identities);
    let melt_ids = cpr_output_columns(anon_cpr_schema, &ctx.identities);
    let melt_metadata: Vec<_> = melt_ids.iter().copied().map(ColumnMetadata::new).collect();
    // The predicate is addressed against the logical join heading. The SQL
    // FROM below exposes neither half under those identities: the left half
    // has crossed a wrapper, and the right half exists only as extraction
    // expressions. Lower first against the complete logical heading, then
    // replace every logical occurrence with the expression its FROM exposes.
    let mut condition_columns = source_metadata.clone();
    condition_columns.extend(melt_metadata);
    let condition_qualify = MeltJoinQualify {
        columns: condition_columns,
        identities: &ctx.identities,
    };

    let mut lowered_condition = match correlation {
        Some(ast_refined::MemberCorrelation::Correspond(_)) => {
            return Err(DelightQLError::validation_error_categorized(
                "transform/melt-join/using",
                "a correlated anonymous join cannot lower an implicit USING condition",
                "write an explicit predicate between the left and anonymous columns",
            ));
        }
        Some(ast_refined::MemberCorrelation::Condition(condition)) => {
            scalar::s_lower_boolean(condition, &condition_qualify, ctx)?.into_expr()
        }
        None => SqlDomainExpr::literal(crate::pipeline::asts::core::LiteralValue::Boolean(true)),
    };

    let mut replacements = std::collections::HashMap::new();
    // `projected` is constructed as every source column followed by exactly
    // one packet, and `republish_under` preserves that order. Taking the first
    // source-width entries therefore enumerates the complete left heading.
    for (source, wrapped) in source_metadata
        .iter()
        .map(ColumnMetadata::identity)
        .zip(wrapped_columns.iter().copied())
    {
        replacements.insert(source, SqlDomainExpr::Column(wrapped));
    }
    let mut select_items = Vec::new();
    for (position, column) in wrapped_columns
        .iter()
        .take(source_metadata.len())
        .enumerate()
    {
        select_items.push(SelectItem::Expression {
            expr: SqlDomainExpr::Column(*column),
            alias: output_ids.get(position).copied(),
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
        replacements.insert(*column, extracted.clone());
        select_items.push(SelectItem::Expression {
            expr: extracted,
            alias: Some(
                output_ids
                    .get(source_metadata.len() + position)
                    .copied()
                    .unwrap_or(*column),
            ),
        });
    }
    replace_melt_join_columns(&mut lowered_condition, &replacements);
    let output_scope = cpr_schema;
    let columns = columns_from_cpr_schema(cpr_schema, &ctx.identities);
    let select = super::builder::publish_at(
        output_scope,
        columns.iter().map(ColumnMetadata::identity),
        SelectStatement::builder()
            .set_select(select_items)
            .from_tables(vec![TableExpression::Join {
                left: Box::new(TableExpression::subquery(source_query, source_scope)),
                right: Box::new(json_each_tvf(wrapped_packet, tvf_scope, &ctx.identities)),
                join_type: lower_join_type(join_type),
                join_condition: SqlJoinCondition::On(lowered_condition),
            }]),
        &ctx.identities,
    )?;
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
    identities: &'a crate::names::Registry,
}

impl Qualify for MeltJoinQualify<'_> {
    fn identities(&self) -> &crate::names::Registry {
        self.identities
    }

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.columns.clone()
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
fn contains_column_reference(expr: &ast_refined::DomainExpression) -> bool {
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
                                RecordMember::SelfKeyed(_) => true,
                                RecordMember::Keyed { value, .. } => {
                                    contains_column_reference(value)
                                }
                                // An induced level reads its own source, and a
                                // spread is spent before this phase.
                                RecordMember::Induced { .. } | RecordMember::Spread(_) => false,
                            })
                        }
                        Enclyph::EmptyRecord(_) => false,
                        Enclyph::Tuple(tuple) => {
                            tuple.elements.iter().any(contains_column_reference)
                        }
                    }
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// The scope a lowering reads FROM, for an expression standing beside its
/// select list rather than in it.
struct HeadingQualify<'a> {
    identities: &'a crate::names::Registry,
    columns: Vec<ColumnMetadata>,
}

impl Qualify for HeadingQualify<'_> {
    fn identities(&self) -> &crate::names::Registry {
        self.identities
    }

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.columns.clone()
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
}

/// Lower a pipe chain: left-fold segments over a base builder.
///
/// The fold starts with `Builder<Unprojected>` (the base) and produces
/// `Builder<Projected>` (the last segment must set a SELECT list).
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
        let PipeSegment { step, cpr_schema } = segment;
        let operator = match step {
            PipeStep::Operator(operator) => operator,
            // Every access is a no-op in SQL: qualification and USING
            // semantics are settled in the refiner's metadata, never
            // materialized here.
            PipeStep::Access => {
                let result = current.project_all()?;
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
                    ast_refined::StructuralForm::Ordering { specs } => {
                        r_lower_order_by(current, specs, cpr_schema, ctx)?
                    }
                    ast_refined::StructuralForm::Reposition { .. } => {
                        r_lower_reposition(current, cpr_schema, ctx)?
                    }
                    ast_refined::StructuralForm::Meta => {
                        r_lower_meta_ize(current, cpr_schema, ctx)?
                    }
                    ast_refined::StructuralForm::Witness { polarity } => {
                        r_lower_witness(current, polarity, cpr_schema, ctx)?
                    }
                    ast_refined::StructuralForm::SignedWitness => {
                        r_lower_signed_witness(current, cpr_schema, ctx)?
                    }
                    ast_refined::StructuralForm::Drill { drill } => r_lower_interior_drill_down(
                        current,
                        drill.column,
                        drill.columns,
                        drill.groundings,
                        cpr_schema,
                        ctx,
                    )?,
                    ast_refined::StructuralForm::Narrow {
                        nest,
                        pattern,
                        schema,
                    } => r_lower_narrowing_destructure(
                        current, nest, pattern, &schema, cpr_schema, ctx,
                    )?,
                };
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
            PipeOp::Project(items) => r_lower_projection(current, items, Some(cpr_schema), ctx)?,

            // Extension IS projection at this level: the resolved items
            // already carry the operand's expanded heading in front of the
            // added columns, so the two lower through one road.
            PipeOp::Embed(items) => r_lower_projection(current, items, Some(cpr_schema), ctx)?,

            PipeOp::ProjectOut(selector) => {
                r_lower_project_out(current, selector, cpr_schema, ctx)?
            }

            PipeOp::Rename(specs) => r_lower_rename_cover(current, specs, cpr_schema, ctx)?,

            PipeOp::Group(spec) => r_lower_group(current, spec, cpr_schema, ctx)?,

            PipeOp::Transform {
                items: transformations,
                guard: conditioned_on,
                ..
            } => r_lower_transform(current, transformations, conditioned_on, cpr_schema, ctx)?,

            PipeOp::MapCover(MapCover { guard, cells, .. }) => {
                r_lower_map_cover(current, cells, guard, cpr_schema, ctx)?
            }

            PipeOp::EmbedMapCover(EmbedMapCover { cells, .. }) => {
                r_lower_embed_map(current, cells, cpr_schema, ctx)?
            }
        };

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
    operands: Vec<Builder<Projected>>,
    operator: ast_refined::SetOperator,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    match operator {
        // The name-aligned operators shape each arm to the output heading:
        // corresponding pads what an arm lacks, smart reorders. Positional
        // aligns by ordinal, which the arms already stand in.
        ast_refined::SetOperator::UnionCorresponding | ast_refined::SetOperator::SmartUnionAll => {
            r_lower_aligned_union(operands, operator, cpr_schema, ctx)
        }
        ast_refined::SetOperator::UnionAllPositional => {
            let mut iter = operands.into_iter();
            let first = iter.next().ok_or_else(|| DelightQLError::ParseError {
                message: "r_lower_set_op: empty operands".to_string(),
                source: None,
                subcategory: None,
            })?;
            iter.try_fold(first, |accumulated, next| accumulated.union_all(next))
        }
        // Minus reaches lowering with its correlation filled in — a bare
        // minus IS the whole-tuple anti-semijoin, and that is where the
        // predicate is written. There is no set-difference capability to
        // fall back to.
        ast_refined::SetOperator::MinusCorresponding => Err(DelightQLError::ParseError {
            message: "minus reached lowering without its anti-semijoin correlation".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

/// Lower a name-aligned union: each arm is projected into the output
/// heading's shape, then the arms are combined.
fn r_lower_aligned_union(
    operands: Vec<Builder<Projected>>,
    operator: ast_refined::SetOperator,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{QueryExpression, SetOperator as SqlSetOp};

    let output_columns = cpr_output_columns(cpr_schema, &ctx.identities);
    if output_columns.is_empty() {
        let mut operands = operands.into_iter();
        let first = operands.next().ok_or_else(|| DelightQLError::ParseError {
            message: "r_lower_set_op: empty operands".to_string(),
            source: None,
            subcategory: None,
        })?;
        return operands.try_fold(first, |left, right| left.union_all(right));
    }
    let output_scope = cpr_schema;
    let mut padded_queries = Vec::new();
    for (arm, operand) in operands.into_iter().enumerate() {
        let metadata = operand.columns().to_vec();
        let mut query = operand.to_sql()?;
        let arm_scope = boundary_scope(&metadata, arm, ctx)?;
        let wrapped: Vec<_> = super::builder::republish_under(
            &mut query,
            arm_scope,
            &metadata,
            &ctx.identities,
            crate::names::Republish::BoundaryExport,
        )?
        .into_iter()
        .map(|column| column.identity())
        .collect();
        let items = align_arm_items(operator, &wrapped, &output_columns, ctx)?;
        // Every arm publishes the merged heading, which is what makes them
        // one set operation rather than two statements stacked.
        let select = super::builder::publish_at(
            output_scope,
            output_columns.iter().copied(),
            crate::pipeline::sql_ast::SelectStatement::builder()
                .select_all(items)
                .from_tables(vec![
                    crate::pipeline::sql_ast::TableExpression::subquery(query, arm_scope),
                ]),
            &ctx.identities,
        )?;
        padded_queries.push(QueryExpression::Select(Box::new(select)));
    }
    let combined = padded_queries
        .into_iter()
        .reduce(|left, right| QueryExpression::SetOperation {
            op: SqlSetOp::UnionAll,
            left: Box::new(left),
            right: Box::new(right),
        })
        .ok_or_else(|| DelightQLError::ParseError {
            message: "r_lower_set_op: empty operands".to_string(),
            source: None,
            subcategory: None,
        })?;
    // Each arm's SELECT was built AT the output scope and aliased to its
    // columns, so the combined query already publishes the heading it
    // claims. Projecting it again would wrap the whole union in a SELECT
    // that renames nothing — the positional road, which combines through
    // `union_all`, adds no such layer either.
    let output_metadata = columns_from_cpr_schema(cpr_schema, &ctx.identities);
    Builder::adopt_finished(
        combined,
        ScopeName::Resolved(output_scope),
        output_metadata,
        ctx.names.clone(),
        std::rc::Rc::clone(&ctx.identities),
    )
}

/// The scope one arm's rows cross into the operation under.
///
/// Minted the same way on both roads, so a bare union and a correlated one
/// name their arms alike.
fn boundary_scope(
    metadata: &[ColumnMetadata],
    arm: usize,
    ctx: &TransformCtx,
) -> Result<crate::names::ScopeId> {
    let origin = ColumnMetadata::common_identity_scope(metadata, &ctx.identities)
        .map(|of| crate::names::ScopeOrigin::SetArm {
            of,
            arm: arm as u16,
        })
        .unwrap_or(crate::names::ScopeOrigin::AnonRelation);
    Ok(ctx.names.fresh(origin).identity())
}

/// Shape one arm to the operation's output heading — the ONE home of the
/// alignment law.
///
/// Corresponding aligns by name and pads what an arm lacks with a typed
/// null. Smart aligns by name and requires the same names and count.
/// Positional aligns by ordinal and requires the same count. Minus
/// publishes its left operand's heading, so only its left arm is ever
/// shaped here.
fn align_arm_items(
    operator: ast_refined::SetOperator,
    arm_columns: &[crate::names::ColId],
    output_columns: &[crate::names::ColId],
    ctx: &TransformCtx,
) -> Result<Vec<crate::pipeline::sql_ast::SelectItem>> {
    use crate::pipeline::asts::core::LiteralValue;
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    let by_ordinal = matches!(operator, ast_refined::SetOperator::UnionAllPositional);
    if by_ordinal || matches!(operator, ast_refined::SetOperator::SmartUnionAll) {
        if arm_columns.len() != output_columns.len() {
            return Err(DelightQLError::validation_error_categorized(
                "set_operation/column_count_mismatch",
                format!(
                    "this set operator requires every operand to publish the same number \
                     of columns, and one publishes {} where the result has {}",
                    arm_columns.len(),
                    output_columns.len()
                ),
                "project the operands to the same width, or use `;` which pads by name",
            ));
        }
    }
    if by_ordinal {
        return Ok(arm_columns
            .iter()
            .zip(output_columns)
            .map(|(source, output)| {
                SelectItem::expression_with_alias(SqlDomainExpr::Column(*source), *output)
            })
            .collect());
    }

    let corresponding = ctx
        .identities
        .corresponding_slots(output_columns, arm_columns)?;
    output_columns
        .iter()
        .zip(corresponding)
        .map(|(output, corresponding)| match corresponding {
            Some(column) => Ok(SelectItem::expression_with_alias(
                SqlDomainExpr::Column(column),
                *output,
            )),
            None if matches!(operator, ast_refined::SetOperator::UnionCorresponding) => {
                // A typed NULL pad — `cast(NULL, t)`, not a bare NULL.
                // Postgres resolves union types pairwise, so two untyped
                // pad branches collapse the column to text before a typed
                // branch arrives.
                let null = SqlDomainExpr::literal(LiteralValue::Null);
                let pad = match ctx.identities.facts(*output).declared_type {
                    Some(type_name) => SqlDomainExpr::cast(null, type_name),
                    None => null,
                };
                Ok(SelectItem::Expression {
                    expr: pad,
                    alias: Some(*output),
                })
            }
            None => Err(DelightQLError::validation_error_categorized(
                "set_operation/column_name_mismatch",
                "smart union (|;|) requires every operand to publish the same names, \
                 and one operand does not publish every name the result has",
                "rename the operand's columns to match, use `;` to align by name and \
                 pad what is missing, or use `||` to align by position",
            )),
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
/// When `cpr_schema` is provided, uses it to fill in aliases for select items
/// that don't have one (e.g., JSON path expressions where the AST node carries
/// no alias but the refiner has computed one).
pub(super) fn r_lower_projection(
    builder: Builder<Unprojected>,
    publication: crate::pipeline::asts::vocabulary::Vec1<ast_refined::OutItem>,
    cpr_schema: Option<crate::names::ScopeId>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    // Lower the items first — this processes computed expressions,
    // function calls, etc. into SQL items via qualify().
    let items: Vec<_> = publication
        .into_vec()
        .into_iter()
        .map(|item| scalar::s_lower_out_item(item, &builder, ctx))
        .collect::<Result<_>>()?;

    // Now use the published heading to fix up aliases. It carries the
    // resolver's authoritative output names. Apply them positionally to the
    // lowered items.
    let items = if let Some(cpr) = cpr_schema {
        let cpr_columns = cpr_output_columns(cpr, &ctx.identities);
        let mut name_idx = 0;
        items
            .into_iter()
            .map(|item| match item {
                crate::pipeline::sql_ast::SelectItem::Star { .. } => {
                    name_idx += builder.columns().len();
                    item
                }
                crate::pipeline::sql_ast::SelectItem::Expression { expr, alias } => {
                    let cpr_alias = cpr_columns.get(name_idx).copied().or(alias);
                    name_idx += 1;
                    crate::pipeline::sql_ast::SelectItem::Expression {
                        expr,
                        alias: cpr_alias,
                    }
                }
            })
            .collect()
    } else {
        items
    };

    // Check for hygienic column references
    for item in &items {
        if let crate::pipeline::sql_ast::SelectItem::Expression { expr, .. } = item {
            if let crate::pipeline::sql_ast::DomainExpression::Column(column) = expr {
                if ctx.identities.addressing(*column) == crate::names::Addressing::Hygienic {
                    return Err(DelightQLError::ParseError {
                        message: "an internal hygiene column is not available for projection"
                            .to_string(),
                        source: None,
                        subcategory: None,
                    });
                }
            }
        }
    }

    project_publishing_resolved(builder, items, cpr_schema, ctx)
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
    cpr_schema: Option<crate::names::ScopeId>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    if let Some(scope) = cpr_schema {
        if adopt_heading(&mut items, scope, &ctx.identities) {
            let columns = columns_from_cpr_schema(scope, &ctx.identities);
            return builder.add_projection_publishing(items, scope, columns);
        }
    }

    builder.add_projection(items)
}

/// The same rule for a reducing segment, whose select list is keys then
/// aggregates and whose GROUP BY clause is read off the keys.
fn group_by_publishing_resolved(
    builder: Builder<Unprojected>,
    mut spec: super::builder::GroupBySpec,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    if let Some(scope) = Some(cpr_schema) {
        let keys = spec.keys.len();
        let mut items = spec.keys;
        items.append(&mut spec.aggregates);
        if adopt_heading(&mut items, scope, &ctx.identities) {
            let aggregates = items.split_off(keys);
            let columns = columns_from_cpr_schema(scope, &ctx.identities);
            return builder.add_group_by_publishing(
                super::builder::GroupBySpec {
                    keys: items,
                    aggregates,
                },
                scope,
                columns,
            );
        }
        let aggregates = items.split_off(keys);
        spec = super::builder::GroupBySpec {
            keys: items,
            aggregates,
        };
    }

    builder.add_group_by(spec)
}

/// Re-alias a lowered select list onto the occurrences the resolver published
/// for this segment, reporting whether it took.
///
/// Slot *i* of the published heading stands for slot *i* of the list when the
/// two occurrences are the same occurrence, or the published one is what a
/// boundary made of the list's — the directional test a reference crossing a
/// boundary answers. Failing that, they may still be one value reached by two
/// routes: a subquery the transformer inserted between segments republishes
/// the previous heading, and so does the scope the resolver minted for this
/// segment, which leaves the two *siblings* — neither on the other's chain.
/// Pairing siblings is sound only while the pairing is forced, so that tier
/// additionally requires each slot's value to be claimable by one published
/// column and no other; a heading that permutes its input therefore adopts
/// nothing.
///
/// Lining up is the whole condition otherwise: a list of a different length,
/// one that dropped a slot it could not place, or one carrying a glob or a
/// hygiene column is not this heading, adopts nothing, and is left untouched
/// for the caller to mint over as before.
/// Does slot *i* of `heading` stand for slot *i* of `outputs`?
///
/// Same occurrence, or the published one is what a boundary made of the
/// output's — the directional test a reference crossing a boundary answers.
/// Failing that, the two may still be one value reached by two routes, which
/// is what a resolver-minted scope and a transformer-inserted subquery leave
/// behind: siblings, neither on the other's chain. Pairing siblings is sound
/// only while forced, so that tier additionally requires each output's value
/// to be claimable by one published column and no other — a heading that
/// permutes its input therefore lines up with nothing.
fn heading_lines_up(
    outputs: &[crate::names::ColId],
    heading: &[crate::names::ColId],
    identities: &crate::names::Registry,
) -> bool {
    if outputs.len() != heading.len() {
        return false;
    }
    let on_chain = outputs.iter().zip(heading).all(|(output, published)| {
        *published == *output || identities.republishes(*published, *output)
    });
    on_chain
        || outputs.iter().zip(heading).all(|(output, published)| {
            identities.same_value(*output, *published)
                && heading
                    .iter()
                    .filter(|other| identities.same_value(*output, **other))
                    .count()
                    == 1
        })
}

fn adopt_heading(
    items: &mut [crate::pipeline::sql_ast::SelectItem],
    scope: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> bool {
    use crate::pipeline::sql_ast::SelectItem;

    let heading = identities.heading(scope).columns_seen();
    // Whether a pairing is possible is a fact about the two chains, so the
    // probe prints the chains and not just the ids: "col#5 vs col#12" says
    // nothing, "both descend from col#3" says the tier applies and "col#12
    // descends from nothing" says the item was minted rather than republished.
    crate::probe::probing!(adopt, {
        crate::probe::probe!(adopt, "{scope:?}");
        for published in heading.iter() {
            crate::probe::probe!(
                adopt,
                "  wants {:?}",
                crate::probe::chain(identities, *published)
            );
        }
        for item in items.iter() {
            match item {
                SelectItem::Expression {
                    alias: Some(alias), ..
                } => crate::probe::probe!(
                    adopt,
                    "  has   {:?}",
                    crate::probe::chain(identities, *alias)
                ),
                other => crate::probe::probe!(adopt, "  has   {other:?}"),
            }
        }
    });
    if heading.len() != items.len() {
        return false;
    }
    let Some(aliases) = items
        .iter()
        .map(|item| match item {
            SelectItem::Expression {
                alias: Some(alias), ..
            } => Some(*alias),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };

    if !heading_lines_up(&aliases, &heading.to_vec(), identities) {
        return false;
    }
    for (item, published) in items.iter_mut().zip(heading) {
        if let SelectItem::Expression { alias, .. } = item {
            *alias = Some(published);
        }
    }
    true
}

/// Lower ORDER BY: `|> #(col1, col2 descending)`.
///
/// Adds ORDER BY terms to the builder, then projects all (SELECT *) at the
/// scope the resolver bound to the segment.
///
/// Leaving the heading unchanged is not the same as standing at the input's
/// scope: the segment has one of its own, and every reference downstream of it
/// was addressed against that scope's occurrences.
pub(super) fn r_lower_order_by(
    builder: Builder<Unprojected>,
    specs: Vec<ast_refined::OrderingSpec>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{OrderDirection as SqlDir, OrderTerm};

    // Ensure we're not Frozen before lowering expressions — add_order_by
    // on Frozen wraps as subquery, changing the scope. Expressions must be
    // qualified against the post-wrap scope.
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

    let (builder, mut items) = builder.add_order_by(terms)?.projectable_star_items()?;
    if let Some(scope) = Some(cpr_schema) {
        if !items.is_empty() && adopt_heading(&mut items, scope, &ctx.identities) {
            let columns = columns_from_cpr_schema(scope, &ctx.identities);
            return builder.add_projection_publishing(items, scope, columns);
        }
    }
    builder.project_all()
}

/// Lower the Group operator: DISTINCT (`GroupSpec::Distinct`) or GROUP BY (`GroupSpec::Reduce`).
fn r_lower_group(
    builder: Builder<Unprojected>,
    spec: ast_refined::GroupSpec,
    cpr_schema: crate::names::ScopeId,
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
            let projected = project_publishing_resolved(builder, items, Some(cpr_schema), ctx)?;
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
                    builder, keys, reductions, plan, arbitrary, cpr_schema, ctx,
                );
            }

            // A single ordered delegate with no aggregates is the 1-arity
            // degenerate of the N-way join: one `row_number()=1` relation, no
            // join to make.
            if reductions.is_empty() && delegates.len() == 1 {
                let delegate = delegates.into_iter().next().unwrap();
                return r_lower_single_ordered_delegate(builder, keys, delegate, cpr_schema, ctx);
            }

            // General case: an aggregate relation (when there are aggregates)
            // plus one `row_number()=1` relation per delegate, joined on the
            // group key.
            r_lower_n_way_delegate_join(builder, keys, reductions, plan, delegates, cpr_schema, ctx)
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
        .filter_map(ast_refined::OutItem::domain_value)
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
        .unwrap_or_else(|| {
            ctx.identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
                None,
            )
        });
    let row_number = ctx.identities.mint_column(
        owner,
        crate::names::ColumnOrigin::Minted {
            by: crate::names::MintReason::RowNumber,
        },
        None,
        crate::names::Addressing::Hygienic,
        crate::names::ValueFacts::default(),
    );
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
    let row_number_here = demoted
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .find(|candidate| ctx.identities.republishes(*candidate, emitted_row_number))
        .unwrap_or(emitted_row_number);
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
    _cpr_schema: crate::names::ScopeId,
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
        let ast_refined::OutItem::One(one) = &item else {
            continue;
        };
        if one.output.is_none() {
            continue; // resolver stamped None — no output column
        }
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
    cpr_schema: crate::names::ScopeId,
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
        .filter_map(ast_refined::OutItem::domain_value)
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
    let key_set: std::collections::HashSet<_> = key_columns.iter().copied().collect();

    // Freeze the source once and rebuild a fresh frozen Builder per relation.
    // (Duplicating the source subquery is correct; CTE-hoisting it is a future
    // perf peephole, not a correctness concern.)
    let cols = builder.columns().to_vec();
    let names = builder.names().clone();
    let identities = std::rc::Rc::clone(builder.identities());
    let src = builder.project_all()?.to_sql()?;
    let fresh_source = || {
        Builder::from_frozen(
            src.clone(),
            ScopeName::Fresh(names.fresh(wrap_origin(
                &cols,
                &identities,
                crate::names::WrapReason::Projection,
            ))),
            cols.clone(),
            names.clone(),
            std::rc::Rc::clone(&identities),
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
            cpr_schema,
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
        .skip(1)
        .map(|operand| {
            let conds: Vec<SqlDomainExpr> = key_columns
                .iter()
                .filter_map(|key| {
                    let anchor = operands[0]
                        .columns
                        .iter()
                        .find(|column| identities.same_value(column.identity(), *key))?
                        .identity();
                    let other = operand
                        .columns
                        .iter()
                        .find(|column| identities.same_value(column.identity(), *key))?
                        .identity();
                    Some(SqlDomainExpr::Binary {
                        left: Box::new(SqlDomainExpr::Column(anchor)),
                        op: BinaryOperator::IsNotDistinctFrom,
                        right: Box::new(SqlDomainExpr::Column(other)),
                    })
                })
                .collect();
            (
                JoinType::Inner,
                JoinCondition::On(SqlDomainExpr::and(conds)),
            )
        })
        .collect();

    // Output projection in cpr order: keys, aggregates, then per-delegate
    // payloads — each explicitly qualified to the operand that owns it, so the
    // qualifier-aware `find_input_column` attaches correct provenance even
    // though all operands share the source column names.
    let mut output_items: Vec<SelectItem> = Vec::new();

    // (a) group keys — from the anchor operand, each aliased from its own
    // output stamp. The n-way path admits only plain-column keys.
    for (key, item) in key_columns.iter().zip(keys.iter()) {
        let anchor = operands[0]
            .columns
            .iter()
            .find(|column| identities.same_value(column.identity(), *key))
            .map(ColumnMetadata::identity)
            .unwrap_or(*key);
        let mut select = SelectItem::Expression {
            expr: SqlDomainExpr::Column(anchor),
            alias: None,
        };
        if let Some(col) = item.output() {
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
        for col in &operands[0].columns {
            if !key_set
                .iter()
                .any(|key| identities.same_value(col.identity(), *key))
            {
                let mut item = SelectItem::Expression {
                    expr: SqlDomainExpr::Column(col.identity()),
                    alias: None,
                };
                alias_unaliased(&mut item, col.identity());
                output_items.push(item);
            }
        }
    }

    // (c) delegate payloads — each from its own operand. Each payload
    // expression carries its own output stamp: `None` = the resolver decided
    // it yields no output column (duplicates a group key already emitted in
    // group position), `Some(col)` = emit, aliased from the stamp.
    // Deduplication is the resolver's; the stamp IS its decision.
    for (op_idx, payload) in &delegate_slots {
        for entry in payload {
            let (Some(col), Some(expr)) = (entry.output(), entry.value()) else {
                continue; // resolver stamped None — no output column
            };
            let mut item = SelectItem::Expression {
                expr: scalar::s_lower_out_value(expr.clone(), &operands[*op_idx], ctx)?,
                alias: None,
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
    cpr_schema: crate::names::ScopeId,
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
        let keys = published_values(keys);
        return r_lower_pivot(builder, keys, reductions, cpr_schema, ctx);
    }

    // Check if any keys expression is a tree group (a record or a
    // metadata level with nested reductions). This pattern:
    // `|> %( {key, "nested": ~> {...}} as tg ~> count:(*) )`
    let by_needs_cte = keys
        .iter()
        .enumerate()
        .any(|(index, _)| plan.needs_cte(ast_refined::TreeGroupLocation::InKeys, index));

    if by_needs_cte {
        // Tree-group-in-keys lowering owns its output schema; unwrap the
        // stamps at the boundary.
        let reductions = published_reduction_values(reductions)?;
        let keys = published_values(keys);
        return tree_group::r_lower_tree_group_in_keys(builder, keys, reductions, cpr_schema, ctx);
    }

    // Check if any reductions expression is a record or metadata level
    // needing CTEs.
    let needs_cte = reductions
        .iter()
        .enumerate()
        .any(|(index, item)| tree_group::reduction_item_needs_cte(item, index, &plan));

    if needs_cte {
        // A single pure tree reduction takes the CTE chain directly; a
        // MIX of CTE-needing trees with other reductions builds one arm
        // per tree joined to a straight arm on the keys.
        if reductions.len() == 1 && arbitrary.is_empty() {
            // Tree-group CTE lowering owns its output schema; unwrap the stamps.
            let reductions = published_reductions(reductions);
            let keys = published_values(keys);
            return tree_group::r_lower_tree_group_cte(builder, keys, reductions, cpr_schema, ctx);
        }
        return tree_group::r_lower_tree_group_mixed(
            builder, keys, reductions, plan, arbitrary, cpr_schema, ctx,
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
        if let Some(col) = output {
            alias_unaliased(&mut item, col);
        }
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
        use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};
        let output = entry.output();
        let Some(expr) = into_published_value(entry) else {
            continue;
        };
        let Some(col) = output else {
            continue; // resolver stamped None — no output column
        };
        let mut item = match scalar::s_lower_select_item(expr, &builder, ctx)? {
            SelectItem::Expression { expr, alias } => SelectItem::Expression {
                expr: SqlDomainExpr::intrinsic(crate::names::Intrinsic::Arbitrary, vec![expr]),
                alias,
            },
            other => other,
        };
        alias_unaliased(&mut item, col);
        aggregates.push(item);
    }

    group_by_publishing_resolved(builder, GroupBySpec { keys, aggregates }, cpr_schema, ctx)
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
    ctx.identities.mint_column(
        scope,
        crate::names::ColumnOrigin::Minted {
            by: crate::names::MintReason::Pivot,
        },
        None,
        crate::names::Addressing::Hygienic,
        crate::names::ValueFacts::default(),
    )
}

fn rebind_pivot_expression(
    mut expr: crate::pipeline::sql_ast::DomainExpression,
    candidates: &[ColumnMetadata],
    identities: &crate::names::Registry,
) -> Result<crate::pipeline::sql_ast::DomainExpression> {
    struct Rebind<'a> {
        candidates: &'a [ColumnMetadata],
        identities: &'a crate::names::Registry,
        ambiguous: bool,
    }
    impl crate::pipeline::sql_ast::walk::SqlVisitorMut for Rebind<'_> {
        fn expr(&mut self, expr: &mut crate::pipeline::sql_ast::DomainExpression) {
            let crate::pipeline::sql_ast::DomainExpression::Column(source) = expr else {
                return;
            };
            let source = *source;
            let mut matches = self
                .candidates
                .iter()
                .map(ColumnMetadata::identity)
                .filter(|candidate| self.identities.same_value(*candidate, source));
            match (matches.next(), matches.next()) {
                (Some(column), None) => {
                    *expr = crate::pipeline::sql_ast::DomainExpression::Column(column);
                }
                (None, None) => {}
                (Some(_), Some(_)) => self.ambiguous = true,
                (None, Some(_)) => unreachable!("second match requires a first"),
            }
        }
    }

    let mut rebind = Rebind {
        candidates,
        identities,
        ambiguous: false,
    };
    crate::pipeline::sql_ast::walk::visit_expression_mut(&mut expr, &mut rebind);
    if rebind.ambiguous {
        Err(DelightQLError::ParseError {
            message: "pivot expression maps to more than one CTE column".to_string(),
            source: None,
            subcategory: None,
        })
    } else {
        Ok(expr)
    }
}

fn r_lower_pivot(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::DomainExpression>,
    reductions: Vec<ast_refined::ReductionItem>,
    cpr_schema: crate::names::ScopeId,
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

    let output_columns = cpr_output_columns(cpr_schema, &ctx.identities);
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

    let internal_scope = ctx.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
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
            let input_columns = input.scope_columns();
            let rebound_groups = group_sql
                .iter()
                .cloned()
                .map(|expr| rebind_pivot_expression(expr, &input_columns, &identities))
                .collect::<Result<Vec<_>>>()?;
            let rebound_keys = key_sql
                .iter()
                .cloned()
                .map(|expr| rebind_pivot_expression(expr, &input_columns, &identities))
                .collect::<Result<Vec<_>>>()?;
            let rebound_values = value_sql
                .iter()
                .map(|group| {
                    group
                        .iter()
                        .cloned()
                        .map(|expr| rebind_pivot_expression(expr, &input_columns, &identities))
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
            let (at, outputs) = super::builder::stand_cte_body_at(
                &mut items,
                input.scope(),
                crate::names::WrapReason::Pivot,
                &identities,
            )?;
            let query = super::builder::publish_at(
                at,
                outputs.iter().copied(),
                SelectBuilder::new()
                    .set_select(items)
                    .from_tables(vec![TableExpression::Scope(input.scope())])
                    .group_by(rebound_groups.into_iter().chain(rebound_keys).collect()),
                &identities,
            )?;
            Ok(CteBody {
                query: QueryExpression::Select(Box::new(query)),
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
                .map(|expr| rebind_pivot_expression(expr, &input_columns, &identities))
                .collect::<Result<Vec<_>>>()?;
            let keys = key_sql_for_prepivot
                .iter()
                .cloned()
                .map(|expr| rebind_pivot_expression(expr, &input_columns, &identities))
                .collect::<Result<Vec<_>>>()?;
            let values = value_sql_for_prepivot
                .iter()
                .map(|group| {
                    group
                        .iter()
                        .cloned()
                        .map(|expr| rebind_pivot_expression(expr, &input_columns, &identities))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            (groups, keys, values)
        };
        let regular = regular_sql_for_prepivot
            .iter()
            .cloned()
            .map(|expr| rebind_pivot_expression(expr, &input_columns, &identities))
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
        let (at, outputs) = super::builder::stand_cte_body_at(
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
        let query = super::builder::publish_at(at, outputs.iter().copied(), select, &identities)?;
        Ok(CteBody {
            query: QueryExpression::Select(Box::new(query)),
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
/// are kept — the column value is prepended as the first argument.
pub(super) fn r_lower_map_cover(
    builder: Builder<Unprojected>,
    cells: Vec<ast_refined::AppliedCell>,
    guard: Option<Box<ast_refined::TruthExpression>>,
    cpr_schema: crate::names::ScopeId,
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
    // cells rather than substituting anything.
    let items: Vec<SelectItem> = builder
        .columns()
        .iter()
        .map(|c| {
            let column = c.identity();
            let applied = cells
                .iter()
                .find(|cell| ctx.identities.same_value(cell.column, column));
            match applied {
                Some(cell) => {
                    let col_expr = qualified_col_expr(c);
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
                    Ok(SelectItem::Expression {
                        expr: final_expr,
                        alias: Some(column),
                    })
                }
                None => Ok(passthrough_item(c)),
            }
        })
        .collect::<Result<_>>()?;

    project_publishing_resolved(builder, items, Some(cpr_schema), ctx)
}

/// Lower project-out: `|> -(cols)`.
///
/// Trusts the published heading — the resolver already determined which
/// columns survive.
pub(super) fn r_lower_project_out(
    builder: Builder<Unprojected>,
    _selector: Vec<ast_refined::SelectorItem>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_items_from_cpr_schema(builder.columns(), cpr_schema, &ctx.identities)?;
    project_publishing_resolved(builder, items, Some(cpr_schema), ctx)
}

/// Lower rename-cover: `|> *(old as new)`.
///
/// Trusts the published heading — the resolver already determined the
/// output names.
pub(super) fn r_lower_rename_cover(
    builder: Builder<Unprojected>,
    _specs: crate::pipeline::asts::vocabulary::Vec1<ast_refined::RenameSpec>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_items_from_cpr_schema(builder.columns(), cpr_schema, &ctx.identities)?;
    project_publishing_resolved(builder, items, Some(cpr_schema), ctx)
}

/// Lower transform (basic-cover): `|> $$(expr as col)`.
///
/// Projects all scope columns, replacing those whose name matches a
/// transformation alias with the transformed expression in place.
pub(super) fn r_lower_transform(
    builder: Builder<Unprojected>,
    transformations: crate::pipeline::asts::vocabulary::Vec1<ast_refined::NamedOutItem>,
    conditioned_on: Option<Box<ast_refined::TruthExpression>>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem, WhenClause};

    // THE TARGET IS THE ITEM'S OUTPUT. Resolution addressed the written name
    // once, against the heading the transform stands on; re-addressing the
    // same characters here would answer against a later heading, and a
    // folded second answer is free to disagree with the first.
    let replacements: Vec<(crate::names::ColId, ast_refined::OutValue)> = transformations
        .into_vec()
        .into_iter()
        .filter_map(|item| item.output.map(|target| (target, item.expr)))
        .collect();

    // Lower the guard condition once (if present)
    let sql_condition: Option<SqlDomainExpr> = match conditioned_on {
        Some(cond) => Some(super::scalar::s_lower_boolean(*cond, &builder, ctx)?.into_expr()),
        None => None,
    };

    let items: Vec<SelectItem> = builder
        .columns()
        .iter()
        .map(|c| {
            if let Some((_, replacement_expr)) = replacements
                .iter()
                .find(|(target, _)| ctx.identities.same_value(*target, c.identity()))
            {
                let col_expr = qualified_col_expr(c);
                let sql_expr = scalar::s_lower_out_value(replacement_expr.clone(), &builder, ctx)?;
                // Wrap in CASE WHEN guard THEN new_val ELSE original END
                let final_expr = match &sql_condition {
                    Some(cond) => SqlDomainExpr::Case {
                        expr: None,
                        when_clauses: vec![WhenClause::new(cond.clone(), sql_expr)],
                        else_clause: Some(Box::new(col_expr)),
                    },
                    None => sql_expr,
                };
                Ok(SelectItem::Expression {
                    expr: final_expr,
                    alias: Some(c.identity()),
                })
            } else {
                Ok(passthrough_item(c))
            }
        })
        .collect::<Result<_>>()?;

    project_publishing_resolved(builder, items, Some(cpr_schema), ctx)
}

/// Lower embed-map-cover: `|> +$(fn:() as :"{@}_suffix")(cols)`.
///
/// Keeps all existing columns, then appends new columns by applying the
/// function to each target column with a templated alias name.
pub(super) fn r_lower_embed_map(
    builder: Builder<Unprojected>,
    cells: Vec<ast_refined::AppliedCell>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::SelectItem;

    let outputs = cpr_output_columns(cpr_schema, &ctx.identities);
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

        items.push(SelectItem::Expression {
            expr: fn_expr,
            alias: Some(alias),
        });
    }

    project_publishing_resolved(builder, items, Some(cpr_schema), ctx)
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
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, SelectStatement,
        SetOperator,
    };
    let source_columns = builder.columns().to_vec();
    let output_columns = cpr_output_columns(cpr_schema, &ctx.identities);
    if source_columns.is_empty() || output_columns.len() < 3 {
        return Err(DelightQLError::ParseError {
            message: "meta-ize requires an input heading and three output columns".to_string(),
            source: None,
            subcategory: None,
        });
    }
    let scope = ctx
        .identities
        .common_scope(&output_columns)
        .ok_or_else(|| DelightQLError::parse_error("meta output has no common scope"))?;
    let make_row = |position: usize, column: &ColumnMetadata| {
        vec![
            // The relation the column BELONGS to, not the one publishing it
            // here. A join republishes both arms into one scope so it has a
            // heading of its own; reading that scope would report every
            // column of a two-relation join as one relation's, and the
            // reader's whole question is which relation a column is from.
            SqlDomainExpr::ScopeNameLiteral(ctx.identities.owner_of(column.identity())),
            SqlDomainExpr::PublishedNameLiteral(column.identity()),
            SqlDomainExpr::literal(ast_refined::LiteralValue::Number(
                (position + 1).to_string(),
            )),
        ]
    };
    let mut rows = source_columns.iter().enumerate();
    let (position, column) = rows.next().expect("non-empty checked");
    let published = Publication::at(
        scope,
        output_columns
            .iter()
            .copied()
            .map(ColumnMetadata::new)
            .collect(),
        &ctx.identities,
    )?;
    let first = published.publish(
        SelectStatement::builder().select_all(
            make_row(position, column)
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
        let row = Alignment::with(&published).align(
            SelectStatement::builder().select_all(
                make_row(position, column)
                    .into_iter()
                    .map(SelectItem::expression)
                    .collect(),
            ),
        )?;
        query = QueryExpression::SetOperation {
            op: SetOperator::UnionAll,
            left: Box::new(query),
            right: Box::new(QueryExpression::Select(Box::new(row))),
        };
    }
    Builder::from_query(
        query,
        ScopeName::Resolved(scope),
        columns_from_cpr_schema(cpr_schema, &ctx.identities),
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
    cpr_schema: crate::names::ScopeId,
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
    let output_columns = cpr_output_columns(cpr_schema, &ctx.identities);
    let output = output_columns
        .first()
        .copied()
        .ok_or_else(|| DelightQLError::ParseError {
            message: "witness requires one resolved output column".to_string(),
            source: None,
            subcategory: None,
        })?;
    let scope = ctx.identities.scope_of(output);
    let select = super::builder::publish_at(
        scope,
        [output],
        SelectStatement::builder().select(SelectItem::expression_with_alias(exists_expr, output)),
        &ctx.identities,
    )?;

    let query = QueryExpression::Select(Box::new(select));

    Builder::from_query(
        query,
        ScopeName::Resolved(scope),
        columns_from_cpr_schema(cpr_schema, &ctx.identities),
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
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::names::{
        Addressing, ColumnOrigin, Computation, Hint, Republish, ScopeOrigin, ValueFacts, WrapReason,
    };
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

    let dee_scope = ctx.identities.mint_derived_scope(
        ScopeOrigin::Wrap {
            input: source_scope,
            why: WrapReason::Witness,
        },
        Hint::None,
    );
    let dee_column = ctx.identities.mint_column(
        dee_scope,
        ColumnOrigin::Computed {
            via: Computation::Literal,
        },
        None,
        Addressing::Hygienic,
        ValueFacts::default(),
    );
    let dee = super::builder::publish_at(
        dee_scope,
        [dee_column],
        SelectStatement::builder().select(SelectItem::expression_with_alias(one(), dee_column)),
        &ctx.identities,
    )?;

    let source_alias_scope = ctx.identities.mint_derived_scope(
        ScopeOrigin::Wrap {
            input: source_scope,
            why: WrapReason::Witness,
        },
        Hint::None,
    );
    let source_alias_columns: Vec<_> = super::builder::republish_under(
        &mut source_query,
        source_alias_scope,
        &source_columns,
        &ctx.identities,
        Republish::Passthrough,
    )?
    .into_iter()
    .map(|column| column.identity())
    .collect();
    let sentinel_scope = ctx.identities.mint_derived_scope(
        ScopeOrigin::Wrap {
            input: source_alias_scope,
            why: WrapReason::Witness,
        },
        Hint::Exact(ctx.identities.intern("r", false)),
    );
    let sentinel_column = ctx.identities.mint_column(
        sentinel_scope,
        ColumnOrigin::Computed {
            via: Computation::Literal,
        },
        Some(ctx.identities.intern("__p", false)),
        Addressing::Hygienic,
        ValueFacts::default(),
    );
    let sentinel_payload = source_alias_columns
        .iter()
        .map(|column| {
            ctx.identities.republish_column(
                *column,
                sentinel_scope,
                Republish::Passthrough,
                ctx.identities.published(*column),
                ctx.identities.addressing(*column),
                |_| {},
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
    let sentinel = super::builder::publish_at(
        sentinel_scope,
        std::iter::once(sentinel_column).chain(sentinel_payload.iter().copied()),
        sentinel,
        &ctx.identities,
    )?;

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

    let output_columns = cpr_output_columns(cpr_schema, &ctx.identities);
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
        let expr = if ctx.identities.facts(*source).interior.is_some() {
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
    let select = super::builder::publish_at(
        scope,
        output_columns.iter().copied(),
        SelectStatement::builder()
            .select_all(items)
            .from_tables(vec![join]),
        &ctx.identities,
    )?;

    let query = QueryExpression::Select(Box::new(select));
    Builder::from_query(
        query,
        ScopeName::Resolved(scope),
        columns_from_cpr_schema(cpr_schema, &ctx.identities),
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
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_items_from_cpr_schema(builder.columns(), cpr_schema, &ctx.identities)?;
    project_publishing_resolved(builder, items, Some(cpr_schema), ctx)
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
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::asts::core::{NamedReference, Reference};

    let Reference::Named(NamedReference(ColumnOccurrence { column, .. })) = nest;
    let matches = builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .filter(|candidate| ctx.identities.same_value(*candidate, column))
        .collect::<Vec<_>>();
    let source_column = match matches.as_slice() {
        [column] => *column,
        [] => {
            return Err(DelightQLError::ParseError {
                message: "narrowing source is not present".to_string(),
                source: None,
                subcategory: None,
            })
        }
        _ => {
            return Err(DelightQLError::ParseError {
                message: "narrowing source is ambiguous".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };
    let mut outputs = cpr_output_columns(cpr_schema, &ctx.identities)
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
        |_source_columns| vec![],
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
    column: crate::names::ColId,
    interior_cols: Vec<crate::names::ColId>,
    groundings: Vec<crate::pipeline::asts::core::operators::ResolvedInteriorGrounding>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    let matches = builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .filter(|candidate| ctx.identities.same_value(*candidate, column))
        .collect::<Vec<_>>();
    let drilled = match matches.as_slice() {
        [column] => *column,
        [] => {
            return Err(DelightQLError::ParseError {
                message: "interior drill source is not present".to_string(),
                source: None,
                subcategory: None,
            })
        }
        _ => {
            return Err(DelightQLError::ParseError {
                message: "interior drill source is ambiguous".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };
    let context_columns: Vec<_> = builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .filter(|candidate| !ctx.identities.same_value(*candidate, drilled))
        .collect();
    let num_context = context_columns.len();
    let output_columns = cpr_output_columns(cpr_schema, &ctx.identities);
    if output_columns.len() < num_context + interior_cols.len() {
        return Err(DelightQLError::ParseError {
            message: "interior drill output heading is incomplete".to_string(),
            source: None,
            subcategory: None,
        });
    }

    builder.expand_with_json_each(
        drilled,
        "_drill",
        super::builder::JsonEachKind::Array,
        |source_columns| {
            source_columns
                .iter()
                .filter(|candidate| !ctx.identities.same_value(**candidate, drilled))
                .enumerate()
                .map(|(i, source)| {
                    SelectItem::expression_with_alias(
                        SqlDomainExpr::Column(*source),
                        output_columns[i],
                    )
                })
                .collect()
        },
        |_key_column, value_column| {
            interior_cols
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
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let source_expr = scalar::s_lower_expression(json_column.clone(), &builder, ctx)?;
    let input_columns: Vec<_> = builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .collect();
    let published = cpr_output_columns(cpr_schema, &ctx.identities);
    crate::probe::probing!(destructure, {
        for column in &input_columns {
            crate::probe::probe!(
                destructure,
                "  input  {:?}",
                crate::probe::chain(&ctx.identities, *column)
            );
        }
        for column in &published {
            let kept = !input_columns
                .iter()
                .any(|input| ctx.identities.same_value(*input, *column));
            crate::probe::probe!(
                destructure,
                "  output {} {:?}",
                if kept { "take" } else { "SKIP" },
                crate::probe::chain(&ctx.identities, *column)
            );
        }
    });
    let mut outputs = published
        .into_iter()
        .filter(|output| {
            !input_columns
                .iter()
                .any(|input| ctx.identities.same_value(*input, *output))
        })
        .peekable();

    let lowered = if matches!(mode, ast_refined::DestructureMode::Aggregate) {
        let json_column = match &json_column {
            ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) => *column,
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
    let scope = ctx.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
    ctx.identities.mint_column(
        scope,
        crate::names::ColumnOrigin::Computed {
            via: crate::names::Computation::Operator,
        },
        None,
        crate::names::Addressing::Hygienic,
        crate::names::ValueFacts::default(),
    )
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
            let projected = builder.add_projection(proj)?;
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
                let current = builder
                    .columns()
                    .iter()
                    .find(|column| ctx.identities.same_value(column.identity(), temp))
                    .map(ColumnMetadata::identity)
                    .ok_or_else(|| DelightQLError::ParseError {
                        message: "nested destructure source was not carried forward".to_string(),
                        source: None,
                        subcategory: None,
                    })?;
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
            builder.add_projection(items)?.demote()
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
    let projected = builder.add_projection(proj)?;
    let temp_column = projected.columns()[context_len].identity();
    let builder = projected.demote()?;
    let key_output = next_destructure_output(outputs)?;
    let value_alias = destructure_temp(ctx);
    let context_len = builder.columns().len();
    let expanded = builder.expand_with_json_each(
        temp_column,
        "_je",
        super::builder::JsonEachKind::Object,
        |source_columns| {
            source_columns
                .iter()
                .map(|column| {
                    SelectItem::expression_with_alias(SqlDomainExpr::Column(*column), *column)
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
    let value_column = demoted.columns()[context_len + 1].identity();
    let builder = remove_column(demoted, temp_column, ctx)?;
    let value_column = carried_now(builder.columns(), value_column, &ctx.identities)?;
    let builder = match target {
        PatternTarget::Pattern(inner) => {
            lower_with_json_each(builder, value_column, inner, mappings, outputs, ctx)?
        }
        // `g:~> _` binds the keys and disregards the contents: one row per
        // key, and nothing under it to reach.
        PatternTarget::Disregarded => builder,
    };
    remove_column(builder, value_column, ctx)
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
        |source_columns| {
            source_columns
                .iter()
                .map(|source| {
                    SelectItem::expression_with_alias(SqlDomainExpr::Column(*source), *source)
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
    remove_column(builder, value_column, ctx)
}

/// The occurrence a builder carries now for a column it has republished.
///
/// Every wrap re-publishes the heading, so a column read before one is an
/// ancestor of what the statement offers rather than the thing itself. Matching
/// it back by value would find any sibling sharing a progenitor; descent finds
/// the one this builder actually made of it.
///
/// One or none, and neither of the other two answers may be guessed at.
/// Returning the pre-wrap occurrence when nothing carries it hands back
/// exactly the dangling reference this function exists to prevent, and taking
/// the first of several publishes one occurrence under a claim that two make
/// equally — the refusal every other reader in this stack gives.
fn carried_now(
    columns: &[ColumnMetadata],
    column: crate::names::ColId,
    identities: &crate::names::Registry,
) -> Result<crate::names::ColId> {
    let mut carrying = columns
        .iter()
        .map(ColumnMetadata::identity)
        .filter(|candidate| identities.republishes(*candidate, column));
    match (carrying.next(), carrying.next()) {
        (Some(carried), None) => Ok(carried),
        (None, _) => Err(DelightQLError::parse_error(
            "the column this step stands on is not published by the statement it now stands in",
        )),
        (Some(_), Some(_)) => Err(DelightQLError::parse_error(
            "the column this step stands on is published more than once here, so naming it \
             names no single column",
        )),
    }
}

fn remove_column(
    builder: Builder<Unprojected>,
    column: crate::names::ColId,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

    let keep: Vec<_> = builder
        .columns()
        .iter()
        .map(ColumnMetadata::identity)
        .filter(|candidate| !ctx.identities.same_value(*candidate, column))
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
    builder.add_projection(items)?.demote()
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
    member: crate::names::ColId,
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
        crate::pipeline::sql_ast::FunctionName::Intrinsic(
            crate::names::Intrinsic::JsonExtractRaw,
        ),
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

    let left_columns = identities.known_heading(left)?.to_vec();
    let right_columns = identities.known_heading(right)?.to_vec();
    let mut pairs: Vec<(crate::names::ColId, crate::names::ColId)> = Vec::new();
    if by_name {
        for right_column in right_columns {
            let Some(name) = identities.published_sym(right_column) else {
                continue;
            };
            let matches: Vec<crate::names::ColId> = left_columns
                .iter()
                .copied()
                .filter(|left| identities.published_sym(*left) == Some(name))
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
                    crate::pipeline::asts::core::NamedReference(ColumnOccurrence {
                        column: left,
                        explicit_qualifier: true,
                    }),
                ),
            )),
            right: Box::new(ast_refined::DomainExpression::Reference(
                crate::pipeline::asts::core::Reference::Named(
                    crate::pipeline::asts::core::NamedReference(ColumnOccurrence {
                        column: right,
                        explicit_qualifier: true,
                    }),
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
    operands: Vec<Builder<Projected>>,
    operator: ast_refined::SetOperator,
    correlations: Vec<ArmCorrelation>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::SqlPredicate;
    use crate::pipeline::sql_ast::{DomainExpression as SqlDomainExpr, SelectItem};

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
    let output_columns = cpr_output_columns(cpr_schema, &ctx.identities);
    let names = ctx.names.clone();
    let mut source_columns = Vec::with_capacity(operands.len());
    let mut queries = Vec::with_capacity(operands.len());
    for operand in operands {
        source_columns.push(operand.scope_columns());
        queries.push(operand.to_sql()?);
    }

    let mut scopes = Vec::with_capacity(source_columns.len());
    let mut active_columns = Vec::with_capacity(source_columns.len());
    for (arm, columns) in source_columns.iter().enumerate() {
        let origin = ColumnMetadata::common_identity_scope(columns, &ctx.identities)
            .map(|of| crate::names::ScopeOrigin::SetArm {
                of,
                arm: arm as u16,
            })
            .unwrap_or(crate::names::ScopeOrigin::AnonRelation);
        let scope = names.fresh(origin).identity();
        scopes.push(scope);
        active_columns.push(super::builder::republish_under(
            &mut queries[arm],
            scope,
            columns,
            &ctx.identities,
            crate::names::Republish::BoundaryExport,
        )?);
    }

    let is_minus = matches!(operator, ast_refined::SetOperator::MinusCorresponding);
    if !is_minus && queries.len() == 2 && correlations.iter().any(|c| c.min_multiplicity) {
        let correlation = &correlations[0];
        return r_lower_intersect_min_multiplicity(
            &correlation.predicate,
            operator,
            queries,
            &source_columns,
            &active_columns,
            &scopes,
            &output_columns,
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
            let candidates = active_columns[i]
                .iter()
                .chain(active_columns[counterpart].iter())
                .cloned()
                .collect::<Vec<_>>();
            let predicate = scalar::s_lower_boolean(
                correlation.predicate.clone(),
                &DummyQualify(&ctx.identities),
                ctx,
            )?
            .into_expr();
            let condition = rebind_pivot_expression(predicate, &candidates, &ctx.identities)?;
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
        let arm_columns = active_columns[i]
            .iter()
            .map(ColumnMetadata::identity)
            .collect::<Vec<_>>();
        // Minus publishes its left operand's heading; every other operator
        // shapes each arm to the merged one.
        let items = if is_minus {
            arm_columns
                .iter()
                .zip(output_columns.iter())
                .map(|(source, output)| {
                    SelectItem::expression_with_alias(SqlDomainExpr::Column(*source), *output)
                })
                .collect()
        } else {
            align_arm_items(operator, &arm_columns, &output_columns, ctx)?
        };
        // The items already carry the resolver's output occurrences as
        // their aliases — publish that scope. Minting a fresh set here
        // orphans the occurrences every downstream reference was
        // addressed against (each half claims the shared output scope,
        // exactly as the padded arms of the plain corresponding road do).
        halves.push(outer.add_projection_publishing(
            items,
            cpr_schema,
            columns_from_cpr_schema(cpr_schema, &ctx.identities),
        )?);
    }
    let mut halves = halves.into_iter();
    let mut combined = halves.next().expect("operand count checked");
    for half in halves {
        combined = combined.union_all(half)?;
    }
    Ok(combined)
}

fn resolved_column(expression: &ast_refined::DomainExpression) -> Option<crate::names::ColId> {
    match expression {
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Some(*column),
        _ => None,
    }
}

fn intersection_column_pairs(
    expression: &ast_refined::TruthExpression,
    operands: &[Vec<ColumnMetadata>],
    identities: &crate::names::Registry,
) -> Result<Vec<(crate::names::ColId, crate::names::ColId)>> {
    fn owner(
        column: crate::names::ColId,
        operands: &[Vec<ColumnMetadata>],
        identities: &crate::names::Registry,
    ) -> Result<usize> {
        let matches = operands
            .iter()
            .enumerate()
            .filter(|(_, heading)| {
                heading
                    .iter()
                    .any(|candidate| identities.same_value(candidate.identity(), column))
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [index] => Ok(*index),
            [] => Err(DelightQLError::ParseError {
                message: "intersection correlation references no operand".to_string(),
                source: None,
                subcategory: None,
            }),
            _ => Err(DelightQLError::ParseError {
                message: "intersection correlation reference belongs to multiple operands"
                    .to_string(),
                source: None,
                subcategory: None,
            }),
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

fn unique_same_value(
    source: crate::names::ColId,
    candidates: &[ColumnMetadata],
    identities: &crate::names::Registry,
) -> Result<crate::names::ColId> {
    let matches = candidates
        .iter()
        .map(ColumnMetadata::identity)
        .filter(|candidate| identities.same_value(*candidate, source))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok(*column),
        [] => Err(DelightQLError::ParseError {
            message: "structural column was not carried into intersection".to_string(),
            source: None,
            subcategory: None,
        }),
        _ => Err(DelightQLError::ParseError {
            message: "structural column is ambiguous inside intersection".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

fn r_lower_intersect_min_multiplicity(
    correlation: &ast_refined::TruthExpression,
    operator: ast_refined::SetOperator,
    queries: Vec<crate::pipeline::sql_ast::QueryExpression>,
    source_columns: &[Vec<ColumnMetadata>],
    active_columns: &[Vec<ColumnMetadata>],
    scopes: &[crate::names::ScopeId],
    output_columns: &[crate::names::ColId],
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{
        ordering::OrderDirection, BinaryOperator, DomainExpression as SqlDomainExpr, JoinCondition,
        JoinType, SelectItem,
    };
    let pairs = intersection_column_pairs(correlation, source_columns, &ctx.identities)?;
    let left_partition = pairs
        .iter()
        .map(|(left, _)| {
            unique_same_value(*left, &active_columns[0], &ctx.identities).map(SqlDomainExpr::Column)
        })
        .collect::<Result<Vec<_>>>()?;
    let right_partition = pairs
        .iter()
        .map(|(_, right)| {
            unique_same_value(*right, &active_columns[1], &ctx.identities)
                .map(SqlDomainExpr::Column)
        })
        .collect::<Result<Vec<_>>>()?;
    let row_scope = ctx.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
    let left_row = ctx.identities.mint_column(
        row_scope,
        crate::names::ColumnOrigin::Minted {
            by: crate::names::MintReason::RowNumber,
        },
        None,
        crate::names::Addressing::Hygienic,
        crate::names::ValueFacts::default(),
    );
    let right_row = ctx.identities.mint_column(
        row_scope,
        crate::names::ColumnOrigin::Minted {
            by: crate::names::MintReason::RowNumber,
        },
        None,
        crate::names::Addressing::Hygienic,
        crate::names::ValueFacts::default(),
    );
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
    let left = Builder::from_frozen(
        queries[0].clone(),
        ScopeName::Resolved(scopes[0]),
        active_columns[0].clone(),
        names.clone(),
        std::rc::Rc::clone(&ctx.identities),
    )?
    .project_all()?
    .add_window_column("ROW_NUMBER", vec![], left_partition, left_order, left_row)?;
    let left_row = left
        .columns()
        .last()
        .expect("window column is appended")
        .identity();
    let right = Builder::from_frozen(
        queries[1].clone(),
        ScopeName::Resolved(scopes[1]),
        active_columns[1].clone(),
        names.clone(),
        std::rc::Rc::clone(&ctx.identities),
    )?
    .project_all()?
    .add_window_column(
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
    let left = left.demote()?.into_join_operand()?;
    let right = right.demote()?.into_join_operand()?;
    let mut conditions = Vec::new();
    for (source_left, source_right) in &pairs {
        let left_column = unique_same_value(*source_left, &left.columns, &ctx.identities)?;
        let right_column = unique_same_value(*source_right, &right.columns, &ctx.identities)?;
        conditions.push(SqlDomainExpr::Binary {
            left: Box::new(SqlDomainExpr::Column(left_column)),
            op: BinaryOperator::IsNotDistinctFrom,
            right: Box::new(SqlDomainExpr::Column(right_column)),
        });
    }
    conditions.push(SqlDomainExpr::Binary {
        left: Box::new(SqlDomainExpr::Column(unique_same_value(
            left_row,
            &left.columns,
            &ctx.identities,
        )?)),
        op: BinaryOperator::Equal,
        right: Box::new(SqlDomainExpr::Column(unique_same_value(
            right_row,
            &right.columns,
            &ctx.identities,
        )?)),
    });
    // The kept rows are the LEFT arm's, shaped to the output heading by the
    // operator's own alignment law — a corresponding union's heading is
    // wider than either arm, so a positional zip would drop its tail.
    let left_columns = active_columns[0]
        .iter()
        .map(ColumnMetadata::identity)
        .collect::<Vec<_>>();
    let output_items = align_arm_items(operator, &left_columns, output_columns, ctx)?
        .into_iter()
        .map(|item| match item {
            SelectItem::Expression {
                expr: SqlDomainExpr::Column(source),
                alias,
            } => unique_same_value(source, &left.columns, &ctx.identities).map(|column| {
                SelectItem::Expression {
                    expr: SqlDomainExpr::Column(column),
                    alias,
                }
            }),
            other => Ok(other),
        })
        .collect::<Result<Vec<_>>>()?;
    Builder::from_join(
        left,
        right,
        JoinType::Inner,
        JoinCondition::On(SqlDomainExpr::and(conditions)),
    )?
    .add_projection(output_items)
}

#[cfg(test)]
mod cte_occurrence_tests {
    //! What `cte_occurrence` must REFUSE.
    //!
    //! It pairs an occurrence's heading with the CTE columns each target's
    //! own chain carries. A wrong pairing is ordinary-looking SQL selecting
    //! the wrong values, so ambiguity refuses by keeping the bare scope —
    //! which fails loudly downstream — and position may stand in only where
    //! no piece of chain evidence disputes it.

    use super::cte_occurrence;
    use crate::names::{
        Addressing, ColId, ColumnOrigin, Computation, CteRole, Hint, Registry, Republish, ScopeId,
        ScopeOrigin, ValueFacts,
    };
    use crate::pipeline::sql_ast::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, TableExpression,
    };

    fn cte_with(registry: &Registry, names: &[&str]) -> (ScopeId, Vec<ColId>) {
        let entity = registry.mint_entity(registry.intern("t", false));
        let base = registry.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::None, None);
        let cte = registry.mint_scope(
            ScopeOrigin::Cte {
                input: base,
                role: CteRole::Materialize,
            },
            Hint::None,
            None,
        );
        let columns = names
            .iter()
            .map(|name| {
                registry.mint_column(
                    cte,
                    ColumnOrigin::Computed {
                        via: Computation::Operator,
                    },
                    Some(registry.intern(name, false)),
                    Addressing::Published,
                    ValueFacts::default(),
                )
            })
            .collect();
        (cte, columns)
    }

    fn occurrence(registry: &Registry, cte: ScopeId) -> ScopeId {
        registry.mint_derived_scope(
            ScopeOrigin::Wrap {
                input: cte,
                why: crate::names::WrapReason::Projection,
            },
            Hint::None,
        )
    }

    fn carried(registry: &Registry, source: ColId, into: ScopeId) -> ColId {
        registry.republish_column(
            source,
            into,
            Republish::Passthrough,
            registry.published(source),
            Addressing::Published,
            |_| {},
        )
    }

    fn computed(registry: &Registry, into: ScopeId, name: &str) -> ColId {
        registry.mint_column(
            into,
            ColumnOrigin::Computed {
                via: Computation::Operator,
            },
            Some(registry.intern(name, false)),
            Addressing::Published,
            ValueFacts::default(),
        )
    }

    #[stacksafe::stacksafe]
    fn pairs_of(expr: TableExpression) -> Vec<(ColId, ColId)> {
        let TableExpression::Subquery { query, .. } = expr else {
            panic!("expected the identified subquery form");
        };
        let QueryExpression::Select(select) = &**query else {
            panic!("expected a select");
        };
        select
            .select_list()
            .iter()
            .map(|item| match item {
                SelectItem::Expression {
                    expr: SqlDomainExpr::Column(source),
                    alias: Some(target),
                } => (*source, *target),
                other => panic!("expected an aliased column, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn subset_pairs_by_chain_not_position() {
        let registry = Registry::new(&[]);
        let (cte, columns) = cte_with(&registry, &["a", "b", "c"]);
        let scope = occurrence(&registry, cte);
        let target = carried(&registry, columns[1], scope);
        assert_eq!(
            pairs_of(cte_occurrence(scope, cte, &registry)),
            vec![(columns[1], target)]
        );
    }

    #[test]
    fn shifted_evidence_refuses_the_positional_zip() {
        // Same width, one target with exact provenance NOT at its own index,
        // one computed target without any. Zipping by position would remap
        // the carried column to a different source than the one the arena
        // already identified.
        let registry = Registry::new(&[]);
        let (cte, columns) = cte_with(&registry, &["a", "b"]);
        let scope = occurrence(&registry, cte);
        computed(&registry, scope, "x");
        carried(&registry, columns[0], scope);
        assert!(matches!(
            cte_occurrence(scope, cte, &registry),
            TableExpression::Scope(_)
        ));
    }

    #[test]
    fn two_sources_for_one_target_refuse() {
        // Both CTE columns sit on the target's chain; picking either would
        // decide by table order what the chain left ambiguous.
        let registry = Registry::new(&[]);
        let (cte, columns) = cte_with(&registry, &["a"]);
        let rechained = carried(&registry, columns[0], cte);
        let scope = occurrence(&registry, cte);
        carried(&registry, rechained, scope);
        assert!(matches!(
            cte_occurrence(scope, cte, &registry),
            TableExpression::Scope(_)
        ));
    }

    #[test]
    fn no_evidence_same_width_still_zips() {
        let registry = Registry::new(&[]);
        let (cte, columns) = cte_with(&registry, &["a", "b"]);
        let scope = occurrence(&registry, cte);
        let first = computed(&registry, scope, "x");
        let second = computed(&registry, scope, "y");
        assert_eq!(
            pairs_of(cte_occurrence(scope, cte, &registry)),
            vec![(columns[0], first), (columns[1], second)]
        );
    }

    #[test]
    fn evidence_agreeing_with_position_completes_the_zip() {
        let registry = Registry::new(&[]);
        let (cte, columns) = cte_with(&registry, &["a", "b"]);
        let scope = occurrence(&registry, cte);
        let first = carried(&registry, columns[0], scope);
        let second = computed(&registry, scope, "y");
        assert_eq!(
            pairs_of(cte_occurrence(scope, cte, &registry)),
            vec![(columns[0], first), (columns[1], second)]
        );
    }
}

#[cfg(test)]
mod carried_now_tests {
    //! What `carried_now` must REFUSE.
    //!
    //! It is asked for the occurrence a statement now carries for a column
    //! read before a wrap. Answering with the pre-wrap column when nothing
    //! carries it hands back exactly the dangling reference the call exists to
    //! prevent, and answering with the first of several publishes one
    //! occurrence under a claim two make equally.

    use super::carried_now;
    use crate::names::{
        Addressing, ColId, ColumnOrigin, Hint, Registry, Republish, ScopeOrigin, ValueFacts,
    };
    use crate::pipeline::asts::core::ColumnMetadata;

    fn source(registry: &Registry) -> ColId {
        let entity = registry.mint_entity(registry.intern("t", false));
        let scope = registry.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::None, None);
        registry.mint_column(
            scope,
            ColumnOrigin::CatalogColumn {
                entity,
                position: 0,
            },
            Some(registry.intern("a", false)),
            Addressing::Published,
            ValueFacts::default(),
        )
    }

    fn wrap_of(registry: &Registry, column: ColId, how_many: usize) -> Vec<ColumnMetadata> {
        let scope = registry.mint_derived_scope(
            ScopeOrigin::Wrap {
                input: registry.scope_of(column),
                why: crate::names::WrapReason::Projection,
            },
            Hint::None,
        );
        (0..how_many)
            .map(|_| {
                let carried = registry.republish_column(
                    column,
                    scope,
                    Republish::Passthrough,
                    registry.published(column),
                    Addressing::Published,
                    |_| {},
                );
                ColumnMetadata::new(carried)
            })
            .collect()
    }

    #[test]
    fn one_carrier_answers() {
        let registry = Registry::new(&[]);
        let column = source(&registry);
        let columns = wrap_of(&registry, column, 1);
        assert_eq!(
            carried_now(&columns, column, &registry).unwrap(),
            columns[0].identity()
        );
    }

    #[test]
    fn no_carrier_refuses_rather_than_returning_the_stale_one() {
        let registry = Registry::new(&[]);
        let column = source(&registry);
        let other = source(&registry);
        let columns = wrap_of(&registry, other, 1);
        let answer = carried_now(&columns, column, &registry);
        assert!(answer.is_err());
        assert_ne!(
            answer.ok(),
            Some(column),
            "the pre-wrap occurrence is the defect"
        );
    }

    #[test]
    fn two_carriers_refuse() {
        let registry = Registry::new(&[]);
        let column = source(&registry);
        let columns = wrap_of(&registry, column, 2);
        assert!(carried_now(&columns, column, &registry).is_err());
    }
}

#[cfg(test)]
mod destructure_mapping_tests {
    use super::make_destructure_shorthand_item;
    use crate::names::{
        Addressing, ColumnOrigin, Computation, Hint, Registry, ScopeOrigin, ValueFacts,
    };
    use crate::pipeline::ast_refined::{DestructureMapping, LiteralValue};
    use crate::pipeline::sql_ast::{DomainExpression, SelectItem};

    #[test]
    fn shorthand_reads_the_authored_key_when_its_output_name_collides() {
        let registry = Registry::new(&[]);
        let scope = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        let spelling = registry.intern("def", false);
        registry.mint_column(
            scope,
            ColumnOrigin::Computed {
                via: Computation::Operator,
            },
            Some(spelling),
            Addressing::Published,
            ValueFacts::default(),
        );
        let output = registry.mint_column(
            scope,
            ColumnOrigin::Computed {
                via: Computation::Operator,
            },
            Some(spelling),
            Addressing::Published,
            ValueFacts::default(),
        );
        let source = DomainExpression::literal(LiteralValue::String("{}".to_string()));
        let item = make_destructure_shorthand_item(
            &source,
            output,
            output,
            &[DestructureMapping {
                json_key: "def".to_string(),
                column: output,
            }],
        )
        .unwrap();
        let SelectItem::Expression {
            expr: DomainExpression::Function { args, .. },
            alias: Some(alias),
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

#[cfg(test)]
mod adopt_heading_tests {
    //! What `adopt_heading` must REFUSE.
    //!
    //! Adoption re-aliases a select list onto occurrences the resolver
    //! published, so a wrong pairing produces ordinary-looking SQL that returns
    //! the right values under the wrong names — a wrong answer, not a compile
    //! error. The sibling tier is what makes a wrong pairing conceivable at
    //! all: it pairs occurrences that are merely the same value, so the two
    //! bounds that keep it honest are pinned here rather than left to the
    //! comment that states them.

    use super::adopt_heading;
    use crate::names::{
        Addressing, ColId, ColumnOrigin, Hint, Registry, Republish, ScopeId, ScopeOrigin,
        ValueFacts,
    };
    use crate::pipeline::sql_ast::{DomainExpression, SelectItem};

    /// A base scope with `n` distinct catalog columns.
    fn base(registry: &Registry, names: &[&str]) -> (ScopeId, Vec<ColId>) {
        let entity = registry.mint_entity(registry.intern("t", false));
        let scope = registry.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::None, None);
        let columns = names
            .iter()
            .enumerate()
            .map(|(position, name)| {
                registry.mint_column(
                    scope,
                    ColumnOrigin::CatalogColumn {
                        entity,
                        position: position as u32,
                    },
                    Some(registry.intern(name, false)),
                    Addressing::Published,
                    ValueFacts::default(),
                )
            })
            .collect();
        (scope, columns)
    }

    /// Republish `columns` into a fresh scope over `input` — one arm of the
    /// sibling pair a pipe segment produces.
    fn republished(
        registry: &Registry,
        input: ScopeId,
        columns: &[ColId],
    ) -> (ScopeId, Vec<ColId>) {
        let scope = registry.mint_scope(ScopeOrigin::PipeStage { input }, Hint::None, None);
        let republished = columns
            .iter()
            .map(|column| {
                registry.republish_column(
                    *column,
                    scope,
                    Republish::Passthrough,
                    registry.published(*column),
                    registry.addressing(*column),
                    |_| {},
                )
            })
            .collect();
        (scope, republished)
    }

    fn items(columns: &[ColId]) -> Vec<SelectItem> {
        columns
            .iter()
            .map(|column| {
                SelectItem::expression_with_alias(DomainExpression::Column(*column), *column)
            })
            .collect()
    }

    #[test]
    fn siblings_in_the_same_order_adopt() {
        let registry = Registry::new(&[]);
        let (source, columns) = base(&registry, &["id", "age"]);
        let (_, published) = republished(&registry, source, &columns);
        let published_scope = registry.scope_of(published[0]);
        let (_, emitted) = republished(&registry, source, &columns);

        let mut list = items(&emitted);
        assert!(
            adopt_heading(&mut list, published_scope, &registry),
            "slot-for-slot siblings of one heading are one value in one order"
        );
        assert_eq!(
            list.iter()
                .map(|item| match item {
                    SelectItem::Expression { alias, .. } => *alias,
                    _ => None,
                })
                .collect::<Vec<_>>(),
            published.iter().copied().map(Some).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_permuted_sibling_heading_refuses() {
        let registry = Registry::new(&[]);
        let (source, columns) = base(&registry, &["id", "age"]);
        let (published_scope, _) = republished(&registry, source, &columns);
        let (_, emitted) = republished(&registry, source, &columns);

        // The list carries the same two values in the other order, so pairing
        // by position would publish `age` under `id`'s occurrence. Refused
        // because the values differ slot by slot — the count guard never has
        // to speak. Pinned anyway: it is the claim the comment makes, and a
        // future tier that paired by name or by ordinal alone would break it
        // here rather than in the corpus.
        let mut list = items(&[emitted[1], emitted[0]]);
        assert!(
            !adopt_heading(&mut list, published_scope, &registry),
            "a heading that permutes its input adopts nothing"
        );
    }

    #[test]
    fn duplicated_same_value_candidates_refuse() {
        let registry = Registry::new(&[]);
        let (source, columns) = base(&registry, &["id"]);

        // `(id as a, id as b)` — two slots of one projection, one value.
        let projected =
            registry.mint_scope(ScopeOrigin::PipeStage { input: source }, Hint::None, None);
        let twice: Vec<ColId> = ["a", "b"]
            .iter()
            .map(|name| {
                registry.republish_column(
                    columns[0],
                    projected,
                    Republish::Rename,
                    Some(registry.intern(name, false)),
                    Addressing::Published,
                    |_| {},
                )
            })
            .collect();

        let (published_scope, _) = republished(&registry, projected, &twice);
        let (_, emitted) = republished(&registry, projected, &twice);

        let mut list = items(&emitted);
        assert!(
            !adopt_heading(&mut list, published_scope, &registry),
            "when one value could claim either slot, nothing forces the pairing"
        );
    }
}
