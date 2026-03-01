// expression.rs - Expression and relation flattening logic

use super::context::FlattenContext;
use super::predicates::extract_references;
use super::types::{
    AnonymousTableData, FlatOperator, FlatOperatorKind, FlatPredicate, FlatSegment, FlatTable,
    OperationContext, TvfData,
};
use crate::error::Result;
use crate::pipeline::asts::refined::QualifiedName;
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::unresolved::NamespacePath;

/// Recursively flatten an expression
pub(super) fn flatten_expression(
    expr: resolved::RelationalExpression,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    match expr {
        resolved::RelationalExpression::Relation(rel) => {
            flatten_relation(rel, segment, ctx)?;
        }

        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            // Flatten left side
            let left_start = segment.tables.len();
            flatten_expression(*left, segment, ctx)?;
            let left_end = segment.tables.len();

            // Flatten right side
            let right_start = segment.tables.len();
            flatten_expression(*right, segment, ctx)?;
            let right_end = segment.tables.len();

            // Record the join operator
            let left_tables: Vec<String> = segment.tables[left_start..left_end]
                .iter()
                .map(|t| {
                    t.alias
                        .clone()
                        .unwrap_or_else(|| t.identifier.name.to_string())
                })
                .collect();

            let right_tables: Vec<String> = segment.tables[right_start..right_end]
                .iter()
                .map(|t| {
                    t.alias
                        .clone()
                        .unwrap_or_else(|| t.identifier.name.to_string())
                })
                .collect();

            // Extract USING columns if the join condition carries them
            // (e.g. from positional pattern unification: shared lvar names)
            let using_columns = match &join_condition {
                Some(resolved::BooleanExpression::Using { columns }) => Some(
                    columns
                        .iter()
                        .map(|col| match col {
                            resolved::UsingColumn::Regular(qname) => qname.name.to_string(),
                            resolved::UsingColumn::Negated(qname) => qname.name.to_string(),
                        })
                        .collect(),
                ),
                // Explicit ON conditions (Comparison, And, Or, etc.) and cross joins (None)
                // don't carry USING columns — only the Using variant does.
                Some(resolved::BooleanExpression::Comparison { .. })
                | Some(resolved::BooleanExpression::And { .. })
                | Some(resolved::BooleanExpression::Or { .. })
                | Some(resolved::BooleanExpression::Not { .. })
                | Some(resolved::BooleanExpression::InnerExists { .. })
                | Some(resolved::BooleanExpression::In { .. })
                | Some(resolved::BooleanExpression::InRelational { .. })
                | Some(resolved::BooleanExpression::BooleanLiteral { .. })
                | Some(resolved::BooleanExpression::Sigma { .. })
                | Some(resolved::BooleanExpression::GlobCorrelation { .. })
                | Some(resolved::BooleanExpression::OrdinalGlobCorrelation { .. })
                | None => None,
            };

            // Store the join operator
            segment.operators.push(FlatOperator {
                position: ctx.position,
                kind: FlatOperatorKind::Join { using_columns },
                left_tables,
                right_tables,
            });

            // Add join condition as predicate (skips USING — already handled above)
            add_join_condition(join_condition, segment, ctx);

            ctx.position += 1;
        }

        resolved::RelationalExpression::SetOperation {
            operator, operands, ..
        } => {
            // For SetOperations, flatten each operand but track them.
            // Operands that are simple Relations are flattened normally.
            // Complex operands (Filters, Pipes, etc.) are treated as opaque
            // to prevent their predicates from being extracted and pooled
            // at the segment level — each UNION branch keeps its own filters.
            let mut operand_tables = Vec::new();

            for (i, operand) in operands.into_iter().enumerate() {
                let start = segment.tables.len();

                let saved_context = ctx.scope_id;
                ctx.scope_id = ctx.position * 100 + i; // Unique scope per operand

                let is_simple_relation =
                    matches!(operand, resolved::RelationalExpression::Relation(_));

                if is_simple_relation {
                    // Simple relation: flatten normally (table entry, no predicates)
                    flatten_expression(operand, segment, ctx)?;
                } else {
                    // Complex operand (Filter, Pipe, Join, etc.): treat as opaque.
                    // Store the full expression in pipe_expr so the rebuilder
                    // refines it independently, preserving its internal predicates.
                    let operand_schema = match &operand {
                        resolved::RelationalExpression::Filter { cpr_schema, .. }
                        | resolved::RelationalExpression::Join { cpr_schema, .. }
                        | resolved::RelationalExpression::SetOperation { cpr_schema, .. } => {
                            cpr_schema.get().clone()
                        }
                        resolved::RelationalExpression::Pipe(pipe) => {
                            pipe.cpr_schema.get().clone()
                        }
                        other => panic!("catch-all hit in flattener/expression.rs flatten_expression (operand_schema): {:?}", other),
                    };
                    segment.tables.push(FlatTable {
                        identifier: QualifiedName {
                            namespace_path: NamespacePath::empty(),
                            name: "__SETOP_OPERAND__".into(),
                            grounding: None,
                        },
                        canonical_name: None,
                        alias: None,
                        position: ctx.position,
                        _scope_id: ctx.scope_id,
                        domain_spec: resolved::DomainSpec::Glob,
                        operation_context: OperationContext::Direct,
                        schema: operand_schema,
                        outer: false,
                        anonymous_data: None,
                        correlation_refs: Vec::new(),
                        inner_relation_pattern: None,
                        pipe_expr: Some(Box::new(operand)),
                        consulted_view_query: None,
                        _table_filters: vec![],
                        tvf_data: None,
                        subquery_segment: None,
                    });
                }

                // Mark all tables from this operand
                for j in start..segment.tables.len() {
                    segment.tables[j].operation_context = OperationContext::FromSetOp;
                }

                let end = segment.tables.len();
                let tables: Vec<String> = segment.tables[start..end]
                    .iter()
                    .map(|t| {
                        t.alias
                            .clone()
                            .unwrap_or_else(|| t.identifier.name.to_string())
                    })
                    .collect();

                operand_tables.push(tables);
                ctx.scope_id = saved_context;
            }

            // Record the SetOp operator
            if operand_tables.len() >= 2 {
                for i in 1..operand_tables.len() {
                    segment.operators.push(FlatOperator {
                        position: ctx.position,
                        kind: FlatOperatorKind::SetOp { operator },
                        left_tables: if i == 1 {
                            operand_tables[0].clone()
                        } else {
                            vec![] // Already combined in previous iteration
                        },
                        right_tables: operand_tables[i].clone(),
                    });
                }
            }

            ctx.position += 1;
        }

        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            // TupleOrdinal (LIMIT/OFFSET) must stay with its source as a unit
            // Don't flatten through it - treat it as a derived table
            // This preserves CPR LTR semantics: users(*), #<5, products(*)
            // should keep the limit as a separate stage
            if !matches!(condition, resolved::SigmaCondition::Predicate(_)) {
                // Wrap the entire Filter expression as a special table
                // The rebuilder will recognize this and refine it separately
                segment.tables.push(FlatTable {
                    identifier: QualifiedName {
                        namespace_path: NamespacePath::empty(),
                        name: "__LIMIT__".into(),
                        grounding: None,
                    },
                    canonical_name: None,
                    alias: None,
                    position: ctx.position,
                    _scope_id: ctx.scope_id,
                    domain_spec: resolved::DomainSpec::Glob,
                    operation_context: OperationContext::Direct,
                    schema: cpr_schema.get().clone(),
                    outer: false,
                    anonymous_data: None,
                    correlation_refs: Vec::new(),
                    inner_relation_pattern: None,
                    pipe_expr: Some(Box::new(resolved::RelationalExpression::Filter {
                        source,
                        condition,
                        origin,
                        cpr_schema,
                    })),
                    consulted_view_query: None,
                    _table_filters: vec![],
                    tvf_data: None,
                    subquery_segment: None,
                });
                ctx.position += 1;
                return Ok(());
            }

            flatten_expression(*source, segment, ctx)?;
            add_sigma_condition(condition, origin, segment, ctx);
        }

        resolved::RelationalExpression::Pipe(pipe) => {
            // Store pipes as special table entries with __PIPE__ marker
            // Design rationale:
            // 1. Pipes produce table-like results (they ARE tables from join's perspective)
            // 2. Storing in tables list allows uniform iteration and position tracking
            // 3. The rebuilder recognizes __PIPE__ and recursively refines the pipe_expr
            // 4. Alternative (separate pipes list) would complicate position tracking
            // This is a pragmatic choice - slightly hacky but keeps code simple
            segment.tables.push(FlatTable {
                identifier: QualifiedName {
                    namespace_path: NamespacePath::empty(),
                    name: "__PIPE__".into(),
                    grounding: None,
                },
                canonical_name: None,
                alias: None,
                position: ctx.position,
                _scope_id: ctx.scope_id,
                domain_spec: resolved::DomainSpec::Glob,
                operation_context: OperationContext::Direct,
                schema: pipe.cpr_schema.get().clone(),
                outer: false,
                anonymous_data: None,
                correlation_refs: Vec::new(),
                inner_relation_pattern: None,
                pipe_expr: Some(Box::new(resolved::RelationalExpression::Pipe(pipe))),
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: None,
                subquery_segment: None,
            });
            ctx.position += 1;
        }
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join consumed by resolver")
        }
    }

    Ok(())
}

/// Flatten a relation (table)
pub(super) fn flatten_relation(
    rel: resolved::Relation,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    match rel {
        resolved::Relation::Ground {
            identifier,
            canonical_name,
            domain_spec,
            alias,
            cpr_schema,
            outer,
            mutation_target: _,
            passthrough: _,
            hygienic_injections: _,
        } => {
            let table_name = alias
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| identifier.name.to_string());

            segment.tables.push(FlatTable {
                identifier: identifier.clone(),
                canonical_name: canonical_name.get().cloned(),
                alias: alias.map(|s| s.to_string()),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                domain_spec: domain_spec.clone(),
                operation_context: OperationContext::Direct,
                schema: cpr_schema.get().clone(),
                outer,
                anonymous_data: None,
                correlation_refs: Vec::new(),
                inner_relation_pattern: None,
                pipe_expr: None,
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: None,
                subquery_segment: None,
            });

            ctx.tables_in_scope.insert(table_name);
            ctx.position += 1;
        }

        resolved::Relation::Anonymous {
            column_headers,
            rows,
            alias,
            qua_target: _,
            cpr_schema,
            outer,
            exists_mode,
        } => {
            log::debug!(
                "Flattening anonymous table with {} headers",
                column_headers.as_ref().map_or(0, |h| h.len())
            );
            let anon_name = format!("_anon_{}", ctx.anon_counter);
            ctx.anon_counter += 1;
            segment.tables.push(FlatTable {
                identifier: QualifiedName {
                    namespace_path: NamespacePath::empty(),
                    name: anon_name.into(),
                    grounding: None,
                },
                canonical_name: None,
                alias: alias.map(|s| s.to_string()),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                domain_spec: resolved::DomainSpec::Glob,
                operation_context: OperationContext::Direct,
                schema: cpr_schema.get().clone(),
                outer,
                anonymous_data: Some(AnonymousTableData {
                    column_headers,
                    rows,
                    exists_mode,
                }),
                correlation_refs: Vec::new(),
                inner_relation_pattern: None,
                pipe_expr: None,
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: None,
                subquery_segment: None,
            });
            ctx.position += 1;
        }

        resolved::Relation::TVF {
            function,
            arguments,
            domain_spec,
            alias,
            namespace,
            grounding,
            ..
        } => {
            segment.tables.push(FlatTable {
                identifier: QualifiedName {
                    namespace_path: NamespacePath::empty(),
                    name: function.clone().into(),
                    grounding: None,
                },
                canonical_name: None,
                alias: alias.map(|s| s.to_string()),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                domain_spec: domain_spec.clone(),
                operation_context: OperationContext::Direct,
                schema: resolved::CprSchema::Unknown,
                outer: false,
                anonymous_data: None,
                correlation_refs: Vec::new(),
                inner_relation_pattern: None,
                pipe_expr: None,
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: Some(TvfData {
                    function: function.to_string(),
                    arguments: arguments.clone(),
                    domain_spec: domain_spec.clone(),
                    namespace: namespace.clone(),
                    grounding: grounding.clone(),
                }),
                subquery_segment: None,
            });
            ctx.position += 1;
        }
        resolved::Relation::InnerRelation {
            pattern,
            alias,
            outer,
            cpr_schema,
        } => {
            // This is handled in inner_relation.rs
            super::inner_relation::flatten_inner_relation(
                pattern,
                alias.map(|s| s.to_string()),
                outer,
                cpr_schema,
                segment,
                ctx,
            )?;
        }

        resolved::Relation::ConsultedView {
            identifier,
            body,
            scoped,
            outer,
        } => {
            // Store the resolved Query as-is for the rebuilder to refine independently.
            // The body is a self-contained subquery — it doesn't participate in the
            // outer segment's FAR cycle. The rebuilder will call refine_query() on it.
            let scoped_data = scoped.get();
            segment.tables.push(FlatTable {
                identifier,
                canonical_name: None,
                alias: Some(scoped_data.alias().to_string()),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                domain_spec: resolved::DomainSpec::Glob,
                operation_context: OperationContext::Direct,
                schema: scoped_data.schema().clone(),
                outer,
                anonymous_data: None,
                correlation_refs: Vec::new(),
                inner_relation_pattern: None,
                subquery_segment: None,
                pipe_expr: None,
                consulted_view_query: Some(body),
                _table_filters: vec![],
                tvf_data: None,
            });
            ctx.position += 1;
        }

        resolved::Relation::PseudoPredicate { .. } => {
            panic!(
                "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                 Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
            )
        }
    }

    Ok(())
}

/// Add a join condition to the segment
pub(super) fn add_join_condition(
    cond: Option<resolved::BooleanExpression>,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) {
    if let Some(expr) = cond {
        if matches!(expr, resolved::BooleanExpression::Using { .. }) {
            return;
        }
        add_predicate(expr, resolved::FilterOrigin::UserWritten, segment, ctx);
    }
}

/// Add a sigma condition to the segment
pub(super) fn add_sigma_condition(
    cond: resolved::SigmaCondition,
    origin: resolved::FilterOrigin,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) {
    if let resolved::SigmaCondition::Predicate(expr) = cond {
        add_predicate(expr, origin, segment, ctx);
    }
}

/// Add a predicate to the segment
pub(super) fn add_predicate(
    expr: resolved::BooleanExpression,
    origin: resolved::FilterOrigin,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) {
    let (qualified, unqualified) = extract_references(&expr);

    segment.predicates.push(FlatPredicate {
        expr,
        position: ctx.position,
        qualified_refs: qualified,
        unqualified_refs: unqualified,
        _scope_id: ctx.scope_id,
        origin,
    });
}
