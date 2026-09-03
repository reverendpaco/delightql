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

    // The outermost step and the operand it consumes travel as ONE value,
    // so the arms below that put the step back cannot put it anywhere else.
    let peeled = match expr.peel() {
        Ok(peeled) => peeled,
        Err(expr) => {
            // THE HEAD TRAVELS WHOLE. Taking its form out and its relation
            // out beside it is what used to make the refined head a fresh
            // assembly of two loose halves; the node crosses instead.
            let (head, access, _) = expr.split_head_access();
            flatten_head(head, access, segment, ctx)?;
            return Ok(());
        }
    };

    // A ZERO-WIDTH anonymous table is still THIS segment's table. Its
    // trailing unasked access is the read's own narrowing (RULINGS
    // 2026-08-19: an all-consumed slot row publishes no columns and keeps
    // its row count) — sending it down the opaque road below would hide
    // its header constraints from this segment's analyzer, and a header
    // term reaching a sibling table would then have no join to stand on.
    if matches!(
        peeled.last().form(),
        resolved::Continuation::Access {
            access: resolved::Access::Unasked,
            ..
        }
    ) && !peeled.prefix().has_steps()
        && matches!(
            peeled.prefix().head().form(),
            resolved::GroundForm::Literal(_)
        )
    {
        let (prefix, _last) = peeled.split();
        let read_result = *prefix.head().result();
        let resolved::GroundForm::Literal(anon) = prefix
            .into_bare_head()
            .expect("a stepless prefix is a bare head")
            .into_form()
        else {
            unreachable!("the head form was just matched as a literal");
        };
        let resolved::AnonRelation { table, outer, .. } = anon;
        segment.tables.push(FlatTable {
            relation: read_result,
            head: None,
            position: ctx.position,
            _scope_id: ctx.scope_id,
            access: resolved::Access::Unasked,
            outer,
            anonymous_data: Some(AnonymousTableData { body: table.body }),
            pipe_expr: None,
            _table_filters: vec![],
            tvf_data: None,
            subquery_segment: None,
        });
        ctx.position += 1;
        return Ok(());
    }

    // A dimension access, a pipe stage and the structural forms each
    // publish a heading of their own, so the segment sees only the relation
    // produced; the rebuilder refines the stored chain. The step goes back
    // exactly where it came from.
    if matches!(
        peeled.last().form(),
        resolved::Continuation::Access { .. }
            | resolved::Continuation::Pipe { .. }
            | resolved::Continuation::Structural(_)
    ) {
        return flatten_opaque(peeled.rejoin(), segment, ctx);
    }

    // A bound and a destructure must stay with their source as a UNIT.
    // Flattening through either loses left-to-right meaning:
    // `users(*), #<5, products(*)` bounds the users read, not the join,
    // and a destructure's added columns belong to the relation it
    // expanded. Both are stored whole and refined as their own stage.
    if matches!(
        peeled.last().form(),
        resolved::Continuation::Bound { .. } | resolved::Continuation::Destructure { .. }
    ) {
        let result = peeled.last().result().to_owned();
        segment.tables.push(FlatTable {
            relation: result,
            head: None,
            position: ctx.position,
            _scope_id: ctx.scope_id,
            access: resolved::Access::All,
            outer: false,
            anonymous_data: None,
            pipe_expr: Some(Box::new(peeled.rejoin())),
            _table_filters: vec![],
            tvf_data: None,
            subquery_segment: None,
        });
        ctx.position += 1;
        return Ok(());
    }

    let (expr, last) = peeled.split();
    let result = *last.result();
    let form = last.into_form();
    match form {
        // A whole-heading correlation relates two ARMS of a set operation.
        // A chain standing on a bag step is opaque above, so reaching here
        // means there is no run for it to correlate.
        resolved::Continuation::Correlate { .. } => {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "resolution/setop/correlation_owner",
                "a whole-heading correlation relates two operands of a set operation, \
                 and this one stands on no set operation",
                "correlate the arms of a `;`, `|;|`, `||` or `-` step: \
                 `x(*) as a ; y(*) as b, a.* = b.*`",
            ));
        }

        resolved::Continuation::Member {
            rhs, correlation, ..
        } => {
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
                .map(|table| table.relation.scope())
                .collect();

            let right_tables = segment.tables[right_start..right_end]
                .iter()
                .map(|table| table.relation.scope())
                .collect();

            // The member's correlation is TOTAL and belongs to the operator
            // WHOLE — correspondence, condition, or decided Cartesian alike.
            // Pooling the condition among the ambient predicates spent the
            // construction's judgment and asked the classifier to buy it
            // back out of the predicate's references.
            segment.operators.push(FlatOperator {
                position: ctx.position,
                kind: FlatOperatorKind::Join { correlation },
                left_tables,
                right_tables,
            });

            ctx.position += 1;
        }

        resolved::Continuation::BagOp { .. } => {
            unreachable!("a chain standing on a bag step is opaque to flattening")
        }

        resolved::Continuation::Restrict { condition, origin } => {
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
                    last_table.relation = result;
                }
                return Ok(());
            }

            flatten_expression(source.clone(), segment, ctx)?;
            add_predicate(condition, origin, segment, ctx);
        }

        resolved::Continuation::Access { .. }
        | resolved::Continuation::Pipe { .. }
        | resolved::Continuation::Structural(_)
        | resolved::Continuation::Bound { .. }
        | resolved::Continuation::Destructure { .. } => {
            unreachable!("the opaque and unit shapes returned above")
        }
        resolved::Continuation::ErJoin(_) => {
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
    let schema = expr.semantic_relation();
    segment.tables.push(FlatTable {
        relation: schema,
        head: None,
        position: ctx.position,
        _scope_id: ctx.scope_id,
        access: resolved::Access::All,
        outer: false,
        anonymous_data: None,
        pipe_expr: Some(Box::new(expr)),
        _table_filters: vec![],
        tvf_data: None,
        subquery_segment: None,
    });
    ctx.position += 1;
    Ok(())
}

/// Flatten a READ: the head node, and what its parens asked of it.
///
/// The node is what the rebuilder crosses back into the refined phase, so
/// it is stored whole for every read kind that has one. The relation is read
/// OUT of it here rather than carried beside it.
pub(super) fn flatten_head(
    head: resolved::Grelex,
    access: Option<resolved::Access>,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    let result = *head.result();
    match head.form().clone() {
        resolved::GroundForm::Literal(anon) => {
            return flatten_anon_table(anon, result, segment, ctx);
        }
        resolved::GroundForm::Reference(rel) => {
            flatten_read(rel, head, result, access, segment, ctx)
        }
    }
}

/// Flatten a READ: a relation, and what its parens asked of it.
fn flatten_read(
    rel: resolved::Relation,
    head: resolved::Grelex,
    result: crate::relation::SemanticRelation,
    access: Option<resolved::Access>,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    match rel {
        resolved::Relation::Ground { outer, .. } => {
            let access = access.unwrap_or(resolved::Access::All);
            segment.tables.push(FlatTable {
                relation: result,
                head: Some(head),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: access.clone(),
                outer,
                anonymous_data: None,
                pipe_expr: None,
                _table_filters: vec![],
                tvf_data: None,
                subquery_segment: None,
            });

            ctx.tables_in_scope.insert(
                segment
                    .tables
                    .last()
                    .expect("the ground relation was just flattened")
                    .relation
                    .scope(),
            );
            ctx.position += 1;
        }

        resolved::Relation::FunctorCall { call, alias: () } => {
            let published = result;
            let function = call.call().callee;
            let arguments: Vec<Option<resolved::DomainExpression>> = match &call.call().arguments {
                crate::pipeline::asts::core::operators::CallArguments::None => Vec::new(),
                crate::pipeline::asts::core::operators::CallArguments::HigherOrder(part) => part
                    .members()
                    .iter()
                    .map(|arg| match arg {
                        crate::pipeline::asts::core::operators::HoArgument::Value(value) => {
                            Some(value.value.clone())
                        }
                        crate::pipeline::asts::core::operators::HoArgument::Skip => None,
                        crate::pipeline::asts::core::operators::HoArgument::Relation(_)
                        | crate::pipeline::asts::core::operators::HoArgument::Rule(_)
                        | crate::pipeline::asts::core::operators::HoArgument::Landed(_) => {
                            unreachable!("a higher-order value argument survived grounding")
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
                            Some(value.value.clone())
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
            let result = published;
            segment.tables.push(FlatTable {
                relation: result,
                head: Some(head),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: access.clone(),
                outer: call.call().marks.outer(),
                anonymous_data: None,
                pipe_expr: None,
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
            alias: _,
            outer,
        } => {
            // This is handled in inner_relation.rs
            super::inner_relation::flatten_inner_relation(
                pattern, head, outer, result, segment, ctx,
            )?;
        }

        resolved::Relation::ConsultedView { body: _, outer } => {
            let scoped = result;
            // Store the resolved Query as-is for the rebuilder to refine independently.
            // The body is a self-contained subquery — it doesn't participate in the
            // outer segment's FAR cycle. The rebuilder will call refine_query() on it.
            segment.tables.push(FlatTable {
                relation: scoped,
                head: Some(head),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: resolved::Access::All,
                outer,
                anonymous_data: None,
                subquery_segment: None,
                pipe_expr: None,
                _table_filters: vec![],
                tvf_data: None,
            });
            ctx.position += 1;
        }
    }

    Ok(())
}

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
    result: crate::relation::SemanticRelation,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    let resolved::AnonRelation {
        table,
        alias: _alias,
        outer,
    } = anon;

    log::debug!(
        "Flattening anonymous table with {} headers",
        table.body.header.as_ref().map_or(0, |h| h.len())
    );
    segment.tables.push(FlatTable {
        relation: result,
        head: None,
        position: ctx.position,
        _scope_id: ctx.scope_id,
        access: resolved::Access::All,
        outer,
        anonymous_data: Some(AnonymousTableData { body: table.body }),
        pipe_expr: None,
        _table_filters: vec![],
        tvf_data: None,
        subquery_segment: None,
    });
    ctx.position += 1;
    Ok(())
}
