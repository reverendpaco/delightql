// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// expression.rs - Expression and relation flattening logic

use super::context::FlattenContext;
use super::predicates::extract_references;
use super::types::{
    AnonymousTableData, FlatOperator, FlatOperatorKind, FlatPredicate, FlatSegment, FlatTable,
    TvfData,
};
use crate::error::Result;
use crate::pipeline::asts::resolved;

/// Recursively flatten an expression
#[stacksafe::stacksafe]
pub(super) fn flatten_expression(
    expr: resolved::Chain,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    // A chain standing on a bag step is opaque here: the bag road owns its
    // arms and the correlation among them, and the rebuilder refines it
    // whole through `refine_internal`.
    if expr.stands_on_bag_step() {
        return flatten_opaque(expr, segment, ctx);
    }

    let mut expr = expr;
    let last = expr.pop_step();
    match last {
        None => {
            let (head, access, _) = expr.split_head_access();
            match head {
                resolved::Grelex::Reference(rel) => {
                    flatten_read(rel, access, segment, ctx)?;
                }
                resolved::Grelex::Literal(anon) => {
                    flatten_anon_table(anon, segment, ctx)?;
                }
            }
        }

        // A dimension access on the relation the chain has built publishes
        // its own heading, exactly as a pipe stage does; the rebuilder
        // refines the stored chain.
        Some(step @ resolved::Continuation::Access { .. }) => {
            flatten_opaque(expr.then(step), segment, ctx)?;
        }

        // A whole-heading correlation relates two ARMS of a set operation.
        // A chain standing on a bag step is opaque above, so reaching here
        // means there is no run for it to correlate.
        Some(resolved::Continuation::Correlate { .. }) => {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "resolution/setop/correlation_owner",
                "a whole-heading correlation relates two operands of a set operation, \
                 and this one stands on no set operation",
                "correlate the arms of a `;`, `|;|`, `||` or `-` step: \
                 `x(*) as a ; y(*) as b, a.* = b.*`",
            ));
        }

        Some(resolved::Continuation::Member {
            rhs, correlation, ..
        }) => {
            let left = expr;
            let right = rhs;
            // Flatten left side
            let left_start = segment.tables.len();
            flatten_expression(left, segment, ctx)?;
            let left_end = segment.tables.len();

            // Flatten right side
            let right_start = segment.tables.len();
            flatten_expression(right, segment, ctx)?;
            let right_end = segment.tables.len();

            // Record the join operator
            let left_tables = segment.tables[left_start..left_end]
                .iter()
                .map(|table| table.identity)
                .collect();

            let right_tables = segment.tables[right_start..right_end]
                .iter()
                .map(|table| table.identity)
                .collect();

            // The member's correlation says which of the two it is. A
            // correspondence belongs to the operator; a condition becomes a
            // predicate below.
            let correspondence = correlation
                .as_ref()
                .and_then(resolved::MemberCorrelation::correspondence)
                .cloned();

            // Store the join operator
            segment.operators.push(FlatOperator {
                position: ctx.position,
                kind: FlatOperatorKind::Join { correspondence },
                left_tables,
                right_tables,
            });

            // Add join condition as predicate (skips USING — already handled above)
            add_correlation(correlation, segment, ctx);

            ctx.position += 1;
        }

        Some(resolved::Continuation::BagOp { .. }) => {
            unreachable!("a chain standing on a bag step is opaque to flattening")
        }

        Some(resolved::Continuation::Restrict {
            condition,
            origin,
            cpr_schema,
        }) => {
            let source = expr;
            // HoGroundScalar filters must stay bound to their source ConsultedView.
            // Don't pool them into the segment's global predicates — that would
            // lose which ConsultedView each _label_0 constraint belongs to,
            // causing qualifier mismatches when multiple HO views are joined.
            if matches!(origin, resolved::FilterOrigin::HoGroundScalar) {
                flatten_expression(source.clone(), segment, ctx)?;
                // Attach the filter to the last-added table (the ConsultedView)
                if let Some(last_table) = segment.tables.last_mut() {
                    last_table._table_filters.push((condition, origin));
                    last_table.schema = cpr_schema;
                }
                return Ok(());
            }

            flatten_expression(source.clone(), segment, ctx)?;
            add_predicate(condition, origin, segment, ctx);
        }

        // A bound and a destructure must stay with their source as a UNIT.
        // Flattening through either loses left-to-right meaning:
        // `users(*), #<5, products(*)` bounds the users read, not the join,
        // and a destructure's added columns belong to the relation it
        // expanded. Both are stored whole and refined as their own stage.
        Some(
            step @ (resolved::Continuation::Bound { .. }
            | resolved::Continuation::Destructure { .. }),
        ) => {
            let cpr_schema = *step
                .cpr_schema()
                .expect("a bound or destructure carries its heading");
            segment.tables.push(FlatTable {
                identity: cpr_schema,
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: resolved::Access::All,
                schema: cpr_schema,
                outer: false,
                anonymous_data: None,
                inner_relation_pattern: None,
                preminted_scope: None,
                pipe_expr: Some(Box::new(expr.then(step))),
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: None,
                subquery_segment: None,
            });
            ctx.position += 1;
            return Ok(());
        }

        Some(
            step @ (resolved::Continuation::Pipe { .. } | resolved::Continuation::Structural(_)),
        ) => {
            // A pipe stage and the structural forms each publish their own
            // heading, so the segment sees only the relation produced. The
            // rebuilder refines the stored chain.
            flatten_opaque(expr.then(step), segment, ctx)?;
        }
        Some(resolved::Continuation::ErJoin(_)) => {
            unreachable!("ER-join consumed by resolver")
        }
    }

    Ok(())
}

/// Record a chain the segment reads only as the relation it publishes.
///
/// Storing the whole chain in `pipe_expr` is what keeps its interior out of
/// the segment's table and predicate pools: the rebuilder refines it
/// independently, so its own filters, arms, and correlations stay its own.
fn flatten_opaque(
    expr: resolved::Chain,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    let schema = *expr
        .continuations
        .last()
        .and_then(resolved::Continuation::cpr_schema)
        .unwrap_or_else(|| panic!("an opaque chain reached flattening without a heading"));
    segment.tables.push(FlatTable {
        identity: schema,
        position: ctx.position,
        _scope_id: ctx.scope_id,
        access: resolved::Access::All,
        schema,
        outer: false,
        anonymous_data: None,
        inner_relation_pattern: None,
        preminted_scope: None,
        pipe_expr: Some(Box::new(expr)),
        consulted_view_query: None,
        _table_filters: vec![],
        tvf_data: None,
        subquery_segment: None,
    });
    ctx.position += 1;
    Ok(())
}

/// Flatten a READ: a relation, and what its parens asked of it.
pub(super) fn flatten_read(
    rel: resolved::Relation,
    access: Option<resolved::Access>,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    match rel {
        resolved::Relation::Ground {
            cpr_schema, outer, ..
        } => {
            let access = access.unwrap_or(resolved::Access::All);
            segment.tables.push(FlatTable {
                identity: cpr_schema,
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: access.clone(),
                schema: cpr_schema,
                outer,
                anonymous_data: None,
                inner_relation_pattern: None,
                preminted_scope: None,
                pipe_expr: None,
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: None,
                subquery_segment: None,
            });

            ctx.tables_in_scope.insert(
                segment
                    .tables
                    .last()
                    .expect("the ground relation was just flattened")
                    .identity,
            );
            ctx.position += 1;
        }

        resolved::Relation::FunctorCall {
            call,
            alias: (),
            cpr_schema: published,
        } => {
            let function = call.call().callee;
            let arguments: Vec<Option<resolved::DomainExpression>> = match &call.call().arguments {
                crate::pipeline::asts::core::operators::CallArguments::None => Vec::new(),
                crate::pipeline::asts::core::operators::CallArguments::HigherOrder(part) => part
                    .members
                    .iter()
                    .map(|arg| match arg {
                        crate::pipeline::asts::core::operators::HoArgument::Value(value) => {
                            value.domain().cloned()
                        }
                        crate::pipeline::asts::core::operators::HoArgument::Skip => None,
                        crate::pipeline::asts::core::operators::HoArgument::Relation(_) => {
                            unreachable!("a higher-order table argument survived grounding")
                        }
                        crate::pipeline::asts::core::operators::HoArgument::Landing(landing) => {
                            match *landing {}
                        }
                    })
                    .collect(),
                crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => members
                    .iter()
                    .map(|member| match member {
                        crate::pipeline::asts::core::operators::ScalarArgument::Value(value) => {
                            value.domain().cloned()
                        }
                        // A callable's BODY is what the callee applies, so
                        // that is the term this position hands it.
                        crate::pipeline::asts::core::operators::ScalarArgument::Callable(
                            crate::pipeline::asts::core::Callable::Lambda(lambda),
                        ) => Some((*lambda.body).clone()),
                        crate::pipeline::asts::core::operators::ScalarArgument::Callable(_) => {
                            unreachable!("only a lambda is written as a callable argument")
                        }
                        crate::pipeline::asts::core::operators::ScalarArgument::Spread(_)
                        | crate::pipeline::asts::core::operators::ScalarArgument::Star => None,
                        crate::pipeline::asts::core::operators::ScalarArgument::Context(marker) => {
                            match *marker {}
                        }
                    })
                    .collect(),
            };
            let access = access.clone().unwrap_or(resolved::Access::All);
            let cpr_schema = published;
            segment.tables.push(FlatTable {
                identity: cpr_schema,
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: access.clone(),
                schema: cpr_schema,
                outer: call.call().marks.outer(),
                anonymous_data: None,
                inner_relation_pattern: None,
                preminted_scope: None,
                pipe_expr: None,
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: Some(TvfData {
                    function,
                    arguments,
                    access,
                }),
                subquery_segment: None,
            });
            ctx.position += 1;
        }

        resolved::Relation::InnerRelation {
            pattern,
            preminted_scope,
            alias: _,
            outer,
            cpr_schema,
        } => {
            // This is handled in inner_relation.rs
            super::inner_relation::flatten_inner_relation(
                pattern,
                preminted_scope,
                outer,
                cpr_schema,
                segment,
                ctx,
            )?;
        }

        resolved::Relation::ConsultedView {
            body,
            scoped,
            outer,
        } => {
            // Store the resolved Query as-is for the rebuilder to refine independently.
            // The body is a self-contained subquery — it doesn't participate in the
            // outer segment's FAR cycle. The rebuilder will call refine_query() on it.
            segment.tables.push(FlatTable {
                identity: scoped,
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: resolved::Access::All,
                schema: scoped,
                outer,
                anonymous_data: None,
                inner_relation_pattern: None,
                preminted_scope: None,
                subquery_segment: None,
                pipe_expr: None,
                consulted_view_query: Some(body),
                _table_filters: vec![],
                tvf_data: None,
            });
            ctx.position += 1;
        }
    }

    Ok(())
}

/// Add a join condition to the segment
pub(super) fn add_correlation(
    correlation: Option<resolved::MemberCorrelation>,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) {
    // Only a CONDITION is a predicate. A correspondence was taken by the
    // operator above — it tests no row, so pooling it here would classify a
    // non-predicate against the table pool.
    if let Some(expr) = correlation.and_then(resolved::MemberCorrelation::into_condition) {
        add_predicate(expr, resolved::FilterOrigin::UserWritten, segment, ctx);
    }
}

/// Add a sigma condition to the segment
/// Add a predicate to the segment
pub(super) fn add_predicate(
    expr: resolved::TruthExpression,
    origin: resolved::FilterOrigin,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) {
    let references = extract_references(&expr);

    segment.predicates.push(FlatPredicate {
        expr,
        position: ctx.position,
        references,
        _scope_id: ctx.scope_id,
        origin,
    });
}

/// Flatten an anonymous table into the segment.
pub(super) fn flatten_anon_table(
    anon: resolved::AnonRelation,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    let resolved::AnonRelation {
        table,
        alias: _alias,
        outer,
    } = anon;
    let cpr_schema = table.cpr_schema;

    log::debug!(
        "Flattening anonymous table with {} headers",
        table.body.header.as_ref().map_or(0, |h| h.len())
    );
    segment.tables.push(FlatTable {
        identity: cpr_schema,
        position: ctx.position,
        _scope_id: ctx.scope_id,
        access: resolved::Access::All,
        schema: cpr_schema,
        outer,
        anonymous_data: Some(AnonymousTableData { body: table.body }),
        inner_relation_pattern: None,
        preminted_scope: None,
        pipe_expr: None,
        consulted_view_query: None,
        _table_filters: vec![],
        tvf_data: None,
        subquery_segment: None,
    });
    ctx.position += 1;
    Ok(())
}
