// Pattern Classifier for INNER-RELATION (SNEAKY-PARENTHESES)
//
// Classifies Indeterminate patterns into:
// - UDT (Uncorrelated Derived Table)
// - CDT-SJ (Correlated Derived Table - Scalar Join)
// - CDT-GJ (Correlated Derived Table - Group Join)
// - CDT-WJ (Correlated Derived Table - Window Join)
//
// Classification uses the AstTransform walk infrastructure to descend into
// all node types (including operators, ConsultedView bodies, ScalarSubquery,
// InnerExists). This fixes the classify_operator() no-op bug by construction.

use super::correlation_analyzer;
use crate::error::Result;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::resolved::{InnerRelationPattern, QualifiedName, Resolved};
use crate::pipeline::asts::unresolved::NamespacePath;

// =============================================================================
// ClassifierFold — AstTransform<Resolved, Resolved>
// =============================================================================
//
// A same-phase fold that classifies Indeterminate InnerRelation patterns.
// Uses the walk infrastructure to descend into operators, ConsultedView bodies,
// ScalarSubquery, InnerExists — everywhere the hand-rolled classify_patterns
// failed to recurse (the "classify_operator no-op" bug).
//
// Since this is Resolved→Resolved, it doesn't change the phase or run FAR.
// It only classifies InnerRelation patterns encountered during the walk.

struct ClassifierFold;

impl AstTransform<Resolved, Resolved> for ClassifierFold {
    fn transform_inner_relation(
        &mut self,
        pattern: InnerRelationPattern<Resolved>,
    ) -> Result<InnerRelationPattern<Resolved>> {
        match pattern {
            InnerRelationPattern::Indeterminate {
                identifier,
                subquery,
            } => {
                // Recursively classify the subquery first (the walk calls
                // transform_relational_action on the subquery, which eventually
                // calls transform_inner_relation for any nested patterns).
                let classified_subquery = self
                    .transform_relational_action(*subquery)?
                    .into_inner();

                // Classify this pattern based on the classified subquery.
                classify_inner_relation_pattern(identifier, classified_subquery)
            }
            // Already classified — let the walk handle recursion into children
            other => crate::pipeline::ast_transform::walk_transform_inner_relation(self, other),
        }
    }
}

/// Classify all InnerRelation patterns in an AST using the walk infrastructure.
///
/// The walk descends into all node types by construction, including operators,
/// ConsultedView bodies, ScalarSubquery, InnerExists — fixing the
/// classify_operator() no-op bug.
pub fn classify_patterns_via_fold(
    ast: resolved::RelationalExpression,
) -> Result<resolved::RelationalExpression> {
    let mut fold = ClassifierFold;
    fold.transform_relational_action(ast)
        .map(|a| a.into_inner())
}

// =============================================================================
// Core Classification Logic
// =============================================================================

/// Core classification logic for a single InnerRelation pattern.
/// Inspects the subquery for correlation, aggregation, and limits.
pub fn classify_inner_relation_pattern(
    identifier: resolved::QualifiedName,
    subquery: resolved::RelationalExpression,
) -> Result<InnerRelationPattern<Resolved>> {
    // Step 1: Detect (but don't extract!) correlation filters from the subquery
    // The filters stay IN the subquery - we just use them for pattern detection
    let correlation_filters = correlation_analyzer::detect_correlation_filters(&subquery)?;

    // Step 2: Check if uncorrelated
    if correlation_filters.is_empty() {
        // No correlation → UDT
        return Ok(InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery: Box::new(subquery),
            is_consulted_view: false,
        });
    }

    // Step 3: Has correlation - check for LIMIT (CDT-WJ pattern)
    if has_limit(&subquery) {
        let order_by = extract_order_by(&subquery)?;
        let limit = extract_limit_value(&subquery)?;
        return Ok(InnerRelationPattern::CorrelatedWindowJoin {
            identifier,
            correlation_filters, // Metadata only - filters stay in subquery
            order_by,
            limit,
            subquery: Box::new(subquery),
        });
    }

    // Inject hygienic columns if projection excludes correlation columns
    // This must happen BEFORE flattening so the flattener can rewrite predicates
    let (final_subquery, injections) =
        inject_hygienic_columns_if_needed(subquery, &correlation_filters, &identifier)?;

    // Step 4: Check for aggregation (CDT-GJ pattern)
    if has_aggregation(&final_subquery) {
        let aggregations = extract_aggregations(&final_subquery)?;
        return Ok(InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery: Box::new(final_subquery),
            hygienic_injections: injections,
        });
    }

    // Step 5: Default - Correlated Scalar Join
    Ok(InnerRelationPattern::CorrelatedScalarJoin {
        identifier,
        correlation_filters,
        subquery: Box::new(final_subquery),
        hygienic_injections: injections,
    })
}

// ============================================================================
// Helper Functions - Aggregation Detection
// ============================================================================

fn has_aggregation(expr: &resolved::RelationalExpression) -> bool {
    match expr {
        resolved::RelationalExpression::Pipe(pipe_expr) => {
            // Check if operator is aggregation (AggregatePipe for ~> and %(...))
            matches!(
                pipe_expr.operator,
                resolved::UnaryRelationalOperator::AggregatePipe { .. }
                    | resolved::UnaryRelationalOperator::Modulo { .. }
            ) || has_aggregation(&pipe_expr.source)
        }
        resolved::RelationalExpression::Filter { source, .. } => has_aggregation(source),
        // Relation, Join, SetOperation: no aggregation operator
        resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::Join { .. }
        | resolved::RelationalExpression::SetOperation { .. } => false,
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before pattern classification")
        }
    }
}

fn extract_aggregations(
    _expr: &resolved::RelationalExpression,
) -> Result<Vec<resolved::DomainExpression>> {
    // TODO: Extract aggregation expressions from GroupBy/WholeTableAggregation operators
    Ok(Vec::new())
}

// ============================================================================
// Helper Functions - Limit/Order By Detection
// ============================================================================

fn has_limit(expr: &resolved::RelationalExpression) -> bool {
    match expr {
        resolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            // Check if this is a TupleOrdinal LIMIT filter (#<N)
            matches!(
                condition,
                resolved::SigmaCondition::TupleOrdinal(resolved::TupleOrdinalClause {
                    operator: resolved::TupleOrdinalOperator::LessThan,
                    value: _,
                    offset: _,
                })
            ) || has_limit(source)
        }
        resolved::RelationalExpression::Pipe(pipe_expr) => has_limit(&pipe_expr.source),
        // Relation (leaf), Join, SetOperation: no limit
        resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::Join { .. }
        | resolved::RelationalExpression::SetOperation { .. } => false,
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before pattern classification")
        }
    }
}

fn extract_limit_value(expr: &resolved::RelationalExpression) -> Result<Option<i64>> {
    match expr {
        resolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            if let resolved::SigmaCondition::TupleOrdinal(resolved::TupleOrdinalClause {
                operator: resolved::TupleOrdinalOperator::LessThan,
                value,
                offset: _,
            }) = condition
            {
                return Ok(Some(*value));
            }
            extract_limit_value(source)
        }
        resolved::RelationalExpression::Pipe(pipe_expr) => extract_limit_value(&pipe_expr.source),
        // Relation, Join, SetOperation: no limit
        resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::Join { .. }
        | resolved::RelationalExpression::SetOperation { .. } => Ok(None),
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before pattern classification")
        }
    }
}

fn extract_order_by(
    expr: &resolved::RelationalExpression,
) -> Result<Vec<resolved::DomainExpression>> {
    // Recursively search for TupleOrdering operators and extract their column expressions
    match expr {
        resolved::RelationalExpression::Pipe(pipe_expr) => {
            // Check if this pipe is a TupleOrdering operator
            if let resolved::UnaryRelationalOperator::TupleOrdering { specs, .. } =
                &pipe_expr.operator
            {
                // Extract column expressions from the ordering specs
                return Ok(specs.iter().map(|spec| spec.column.clone()).collect());
            }
            // Otherwise, recurse into the source
            extract_order_by(&pipe_expr.source)
        }
        resolved::RelationalExpression::Filter { source, .. } => {
            // Recurse into the source
            extract_order_by(source)
        }
        resolved::RelationalExpression::Join { left, right, .. } => {
            // Check left first, then right
            let left_order = extract_order_by(left)?;
            if !left_order.is_empty() {
                return Ok(left_order);
            }
            extract_order_by(right)
        }
        // Relation, SetOperation: no order by
        resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::SetOperation { .. } => Ok(vec![]),
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before pattern classification")
        }
    }
}

// ============================================================================
// Hygienic Column Injection
// ============================================================================

/// Inject hygienic columns into projection if correlation columns are missing
///
/// Returns: (modified_subquery, list_of_injections)
/// where injections = Vec<(original_column_name, hygienic_alias)>
fn inject_hygienic_columns_if_needed(
    subquery: resolved::RelationalExpression,
    correlation_filters: &[resolved::BooleanExpression],
    table_identifier: &QualifiedName,
) -> Result<(resolved::RelationalExpression, Vec<(String, String)>)> {
    use crate::pipeline::asts::resolved;

    // Extract correlation column names from filters
    let correlation_columns = correlation_analyzer::extract_correlation_column_names(
        correlation_filters,
        table_identifier,
    );

    if correlation_columns.is_empty() {
        return Ok((subquery, vec![]));
    }

    // Check if subquery ends with a projection
    let needs_injection = match &subquery {
        resolved::RelationalExpression::Pipe(pipe) => {
            matches!(
                pipe.operator,
                resolved::UnaryRelationalOperator::General {
                    containment_semantic: resolved::ContainmentSemantic::Parenthesis,
                    ..
                }
            )
        }
        // Non-Pipe expressions (Filter, Relation, Join, SetOperation):
        // No explicit projection → all columns preserved → no injection needed.
        resolved::RelationalExpression::Filter { .. }
        | resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::Join { .. }
        | resolved::RelationalExpression::SetOperation { .. } => false,
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before pattern classification")
        }
    };

    if !needs_injection {
        // Subquery doesn't end with projection - all columns preserved (map-cover or glob)
        return Ok((subquery, vec![]));
    }

    // Extract the projection expressions
    if let resolved::RelationalExpression::Pipe(pipe) = &subquery {
        if let resolved::UnaryRelationalOperator::General {
            containment_semantic: resolved::ContainmentSemantic::Parenthesis,
            expressions,
        } = &pipe.operator
        {
            // Check which correlation columns are missing from projection
            let projected_columns: std::collections::HashSet<String> = expressions
                .iter()
                .filter_map(|expr| {
                    if let resolved::DomainExpression::Lvar { name, .. } = expr {
                        Some(name.to_string())
                    } else {
                        None
                    }
                })
                .collect();

            let mut injections = vec![];
            let mut new_expressions = expressions.clone();

            for (idx, col_name) in correlation_columns.iter().enumerate() {
                if !projected_columns.contains(col_name) {
                    // Correlation column missing - inject with hygienic name
                    let hygienic_name = format!("__dql_corr_{}", idx);

                    new_expressions.push(resolved::DomainExpression::Lvar {
                        name: col_name.clone().into(),
                        qualifier: None,
                        namespace_path: NamespacePath::empty(),
                        alias: Some(hygienic_name.clone().into()),
                        provenance: resolved::PhaseBox::phantom(),
                    });

                    injections.push((col_name.clone(), hygienic_name));
                }
            }

            if injections.is_empty() {
                // All correlation columns already present
                return Ok((subquery, vec![]));
            }

            // Rebuild pipe with injected columns
            let new_pipe = resolved::PipeExpression {
                source: pipe.source.clone(),
                operator: resolved::UnaryRelationalOperator::General {
                    containment_semantic: resolved::ContainmentSemantic::Parenthesis,
                    expressions: new_expressions,
                },
                cpr_schema: pipe.cpr_schema.clone(),
            };

            return Ok((
                resolved::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(new_pipe))),
                injections,
            ));
        }
    }

    Ok((subquery, vec![]))
}
