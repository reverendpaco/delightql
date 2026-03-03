//! Unified continuation handler implementing CPR semantics
//! CPR + Continuation → New CPR

use super::operators::{parse_ordering, parse_unary_operator_no_continuation};
use super::predicates::{
    is_destructuring_predicate, parse_destructuring_sigma, parse_predicate_as_boolean,
};
use super::relations::{
    parse_anonymous_table, parse_catalog_functor, parse_table_access, parse_tvf_call,
};
use super::{parse_expression, parse_limit_offset};
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::{FeatureCollector, QueryFeature};

/// Parse and apply a continuation to a CPR
#[stacksafe::stacksafe]
pub(super) fn handle_continuation(
    cont: CstNode,
    cpr: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    // Linearize consecutive unary (pipe) continuations.
    // Collect the CST chain: each relational_continuation may contain a
    // unary_operator_expression with a nested relational_continuation child.
    // We process annotations and operators iteratively instead of recursing.
    handle_continuation_inner(cont, cpr, features)
}

/// Non-recursive continuation handler. Loops over consecutive unary pipe
/// continuations, falling through to recursive handling for binary operators.
fn handle_continuation_inner(
    mut cont: CstNode,
    mut cpr: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    loop {
        // Process annotations on this continuation node
        for child in cont.children() {
            if child.kind() == "annotation" {
                if let Some(assertion) = extract_assertion_from_annotation(child, &cpr, features)? {
                    features.add_assertion(assertion);
                }
                if let Some(emit) = extract_emit_from_annotation(child, &cpr, features)? {
                    features.add_emit(emit);
                }
                if let Some(danger) = extract_danger_from_annotation(child)? {
                    features.add_danger(danger);
                }
                if let Some(option) = extract_option_from_annotation(child)? {
                    features.add_option(option);
                }
            }
        }

        // Find the operator expression (may be absent for trailing meta-constructs)
        let op_expr = match cont.children().find(|n| {
            n.kind() == "binary_operator_expression" || n.kind() == "unary_operator_expression"
        }) {
            Some(op) => op,
            None => return Ok(cpr), // Trailing annotations only, pass through
        };

        match op_expr.kind() {
            "unary_operator_expression" => {
                // Parse the unary operator WITHOUT following its nested continuation.
                // We handle the continuation ourselves in this loop.
                cpr = parse_unary_operator_no_continuation(op_expr, cpr, features)?;

                // Check if the unary_operator_expression has a nested continuation
                match op_expr.find_child("relational_continuation") {
                    Some(next_cont) => {
                        cont = next_cont;
                        // Loop continues with the next continuation
                    }
                    None => return Ok(cpr), // End of pipe chain
                }
            }
            "binary_operator_expression"
            | "union_corresponding"
            | "smart_union_all"
            | "union_all_positional"
            | "minus_corresponding" => {
                // Binary operators break the pipe chain — handle non-iteratively.
                // For comma, there's an operator field. For unions, the whole node IS the operator type

                // Get what comes after operator - could be various node types
                let right = op_expr
                    .find_first_of(&[
                        "relational_expression",
                        "continuation_expression",
                        "base_expression",
                    ])
                    .ok_or_else(|| DelightQLError::parse_error("Nothing after operator"))?;

                // Check what type of operator this is
                return if let Some(operator) = op_expr.field("operator") {
                    // Has operator field - check what kind
                    match operator.kind() {
                        "comma_operator" => apply_comma(cpr, right, features),
                        "union_corresponding" => build_set_operation(
                            SetOperator::UnionCorresponding,
                            cpr,
                            right,
                            op_expr,
                            features,
                        ),
                        "union_all_positional" => build_set_operation(
                            SetOperator::UnionAllPositional,
                            cpr,
                            right,
                            op_expr,
                            features,
                        ),
                        "minus_corresponding" => build_set_operation(
                            SetOperator::MinusCorresponding,
                            cpr,
                            right,
                            op_expr,
                            features,
                        ),
                        "smart_union_all" => build_set_operation(
                            SetOperator::SmartUnionAll,
                            cpr,
                            right,
                            op_expr,
                            features,
                        ),
                        "er_join_operator" => {
                            // & creates ER-context direct join
                            // Right side is a base_expression (table_access, etc.)
                            let right_relation = parse_base_as_relation(right, features)?;

                            // Flatten: if cpr is already an ErJoinChain, append; otherwise create new chain
                            let mut chain = match cpr {
                                RelationalExpression::ErJoinChain { relations } => relations,
                                _ => {
                                    // Left must be a single relation for ErJoinChain
                                    let left_relation = extract_relation_from_expr(cpr)?;
                                    vec![left_relation]
                                }
                            };
                            chain.push(right_relation);

                            let er_join = RelationalExpression::ErJoinChain { relations: chain };

                            // Check for continuation after the chain
                            if let Some(cont) = op_expr.find_child("relational_continuation") {
                                handle_continuation(cont, er_join, features)
                            } else {
                                Ok(er_join)
                            }
                        }
                        "er_transitive_join_operator" => {
                            // && creates ER-context transitive join
                            let right_relation = parse_base_as_relation(right, features)?;

                            let er_trans = RelationalExpression::ErTransitiveJoin {
                                left: Box::new(cpr),
                                right: Box::new(RelationalExpression::Relation(right_relation)),
                            };

                            // Check for continuation
                            if let Some(cont) = op_expr.find_child("relational_continuation") {
                                handle_continuation(cont, er_trans, features)
                            } else {
                                Ok(er_trans)
                            }
                        }
                        _ => Err(DelightQLError::parse_error(format!(
                            "Unknown operator field: {}",
                            operator.kind()
                        ))),
                    }
                } else {
                    // No operator field - the node itself IS the operator
                    match op_expr.kind() {
                        "union_outer_all" => build_set_operation(
                            SetOperator::UnionCorresponding,
                            cpr,
                            right,
                            op_expr,
                            features,
                        ),
                        "union_all_operator" => build_set_operation(
                            SetOperator::SmartUnionAll,
                            cpr,
                            right,
                            op_expr,
                            features,
                        ),

                        _ => Err(DelightQLError::parse_error(format!(
                            "Unknown node type: {}",
                            op_expr.kind()
                        ))),
                    }
                };
            }
            other => panic!("catch-all hit in builder_v2/continuation.rs handle_continuation: unexpected node kind {:?}", other),
        }
    }
}

/// Build a set operation (union, minus, etc.) from left/right operands,
/// then apply any trailing continuation.
fn build_set_operation(
    op: SetOperator,
    left: RelationalExpression,
    right_node: CstNode,
    op_expr: CstNode,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let right_expr = parse_right_side(right_node, features)?;
    let result = RelationalExpression::SetOperation {
        operator: op,
        operands: vec![left, right_expr],
        correlation: PhaseBox::no_correlation(),
        cpr_schema: PhaseBox::phantom(),
    };
    if let Some(cont) = op_expr.find_child("relational_continuation") {
        handle_continuation(cont, result, features)
    } else {
        Ok(result)
    }
}

/// Apply comma - just handle the immediate operation
fn apply_comma(
    cpr: RelationalExpression,
    right: CstNode,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    // Mark that we're doing a join
    features.mark(QueryFeature::Joins);
    // For continuation_expression with tables, just join the table, don't parse its continuations
    if right.kind() == "continuation_expression" {
        // Check what the base contains
        let base = right
            .find_child("continuation_base")
            .and_then(|b| b.children().next());

        match base.as_ref().map(|n| n.kind()) {
            Some("table_access")
            | Some("anonymous_table")
            | Some("tvf_call")
            | Some("catalog_functor") => {
                // Parse just the table itself
                let base_node = base.expect("Matched Some(...) above, base must exist");
                let mut table_expr = match base_node.kind() {
                    "table_access" => parse_table_access(base_node, features)?,
                    "catalog_functor" => parse_catalog_functor(base_node, features)?,
                    "anonymous_table" => {
                        RelationalExpression::Relation(parse_anonymous_table(base_node, features)?)
                    }
                    "tvf_call" => {
                        RelationalExpression::Relation(parse_tvf_call(base_node, features)?)
                    }
                    _ => unreachable!(),
                };

                // Check if the continuation contains a Using operator
                // If so, extract the columns and apply them to the table's domain_spec
                let mut using_for_join: Option<Vec<String>> = None;
                let remaining_cont = if let Some(cont) = right.find_child("relational_continuation")
                {
                    let (using_columns, remaining) = extract_using_from_continuation(cont);
                    if let Some(cols) = using_columns {
                        if matches!(
                            &table_expr,
                            RelationalExpression::Relation(Relation::Ground { .. })
                        ) {
                            table_expr = apply_using_columns_to_rel_expr(table_expr, cols);
                        } else {
                            using_for_join = Some(cols);
                        }
                    }
                    remaining
                } else {
                    None
                };

                let table = table_expr;

                // Build the join, attaching USING if the right side is non-Ground
                let mut builder = RelationalExpression::join_builder(cpr, table);
                if let Some(cols) = using_for_join {
                    let using_columns = cols
                        .into_iter()
                        .map(|name| {
                            UsingColumn::Regular(QualifiedName {
                                namespace_path: NamespacePath::empty(),
                                name: name.into(),
                                grounding: None,
                            })
                        })
                        .collect();
                    builder = builder.with_using_expr(BooleanExpression::using(using_columns));
                }
                let joined = builder.build();

                // Now handle any remaining continuation (minus the Using which was extracted)
                if let Some(cont) = remaining_cont {
                    handle_continuation(cont, joined, features)
                } else {
                    Ok(joined)
                }
            }
            Some("relational_expression") => {
                // Full relational expression - parse it completely
                let right_expr = parse_right_side(right, features)?;
                Ok(RelationalExpression::join_builder(cpr, right_expr).build())
            }
            Some("predicate") => {
                // It's a predicate - check if it's destructuring or regular
                let base_pred = base.expect("Matched Some(...) above, base must exist");

                let condition = if is_destructuring_predicate(base_pred) {
                    // Parse as Destructure
                    parse_destructuring_sigma(base_pred)?
                } else {
                    // Parse as regular predicate
                    let pred = parse_predicate_as_boolean(base_pred, features)?;
                    SigmaCondition::Predicate(pred)
                };

                let mut result = RelationalExpression::filter_builder(cpr)
                    .with_condition(condition)
                    .build();

                // Check for continuation after the predicate
                if let Some(cont) = right.find_child("relational_continuation") {
                    result = handle_continuation(cont, result, features)?;
                }
                Ok(result)
            }
            Some("ordering") => {
                // Handle ORDER BY with possible continuations
                let base_node = base.expect("Matched Some(...) above, base must exist");
                let mut result = parse_ordering(base_node, cpr)?;

                // Check for continuation
                if let Some(cont) = right.find_child("relational_continuation") {
                    result = handle_continuation(cont, result, features)?;
                }
                Ok(result)
            }
            Some("limit_offset") => {
                // Handle limit/offset with possible continuations
                let base_node = base.expect("Matched Some(...) above, base must exist");
                let clause = parse_limit_offset(base_node, features)?;
                let mut result = RelationalExpression::filter_builder(cpr)
                    .with_condition(SigmaCondition::TupleOrdinal(clause))
                    .build();

                // Check for continuation
                if let Some(cont) = right.find_child("relational_continuation") {
                    result = handle_continuation(cont, result, features)?;
                }
                Ok(result)
            }
            _ => {
                // Unknown base type - just handle the base directly
                if let Some(base_node) = base {
                    apply_comma_to_node(cpr, base_node, features)
                } else {
                    Ok(cpr)
                }
            }
        }
    } else {
        // Not a continuation_expression, handle the node directly
        apply_comma_to_node(cpr, right, features)
    }
}

/// Apply comma to a specific node type (not continuation_expression)
fn apply_comma_to_node(
    cpr: RelationalExpression,
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    match node.kind() {
        "table_access" => {
            // Join with a table
            let table = parse_table_access(node, features)?;
            Ok(RelationalExpression::join_builder(cpr, table).build())
        }
        "catalog_functor" => {
            let table = parse_catalog_functor(node, features)?;
            Ok(RelationalExpression::join_builder(cpr, table).build())
        }
        "anonymous_table" => {
            // Join with anonymous table
            let table = RelationalExpression::Relation(parse_anonymous_table(node, features)?);
            Ok(RelationalExpression::join_builder(cpr, table).build())
        }
        "tvf_call" => {
            // Join with TVF
            let table = RelationalExpression::Relation(parse_tvf_call(node, features)?);
            Ok(RelationalExpression::join_builder(cpr, table).build())
        }
        "predicate" => {
            // Always create filter - refiner will detect if it's correlation
            // Check if it's destructuring or regular predicate
            let condition = if is_destructuring_predicate(node) {
                parse_destructuring_sigma(node)?
            } else {
                let pred = parse_predicate_as_boolean(node, features)?;
                SigmaCondition::Predicate(pred)
            };
            Ok(RelationalExpression::filter_builder(cpr)
                .with_condition(condition)
                .build())
        }
        "relational_expression" => {
            // Full relational expression after comma - parse and join
            let right_expr = parse_expression(node, features)?;
            Ok(RelationalExpression::join_builder(cpr, right_expr).build())
        }
        "ordering" => {
            // ORDER BY clause after comma
            parse_ordering(node, cpr)
        }
        "limit_offset" => {
            // Limit/offset clause after comma
            let clause = parse_limit_offset(node, features)?;
            Ok(RelationalExpression::filter_builder(cpr)
                .with_condition(SigmaCondition::TupleOrdinal(clause))
                .build())
        }
        "parenthesized_expression" => {
            // Look inside parentheses - might contain predicates
            // This is a necessary case for filters with multiple conditions
            let mut predicates = Vec::new();

            // Find all predicate children using find_all
            for child in node.children() {
                if child.kind() == "domain_expression" {
                    for domain_child in child.children() {
                        if domain_child.kind() == "predicate" {
                            predicates.push(domain_child);
                        }
                    }
                } else if child.kind() == "predicate" {
                    predicates.push(child);
                }
            }

            if predicates.is_empty() {
                // No predicates, just return CPR
                return Ok(cpr);
            }

            // Parse all predicates and combine
            let mut conditions = Vec::new();
            for pred_node in predicates {
                let pred = parse_predicate_as_boolean(pred_node, features)?;
                conditions.push(pred);
            }

            // Combine with AND
            let combined = if conditions.len() == 1 {
                conditions.into_iter().next().expect("Checked len==1 above")
            } else {
                conditions
                    .into_iter()
                    .reduce(|acc, p| BooleanExpression::And {
                        left: Box::new(acc),
                        right: Box::new(p),
                    })
                    .expect("conditions is non-empty, reduce must succeed")
            };

            Ok(RelationalExpression::filter_builder(cpr)
                .with_condition(SigmaCondition::Predicate(combined))
                .build())
        }
        other => {
            panic!("catch-all hit in builder_v2/continuation.rs apply_comma_to_node: unexpected node kind {:?}", other)
        }
    }
}

/// Parse the right side of a binary operator
fn parse_right_side(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    match node.kind() {
        "relational_expression" => {
            // Full relational expression - parse it completely
            parse_expression(node, features)
        }
        "continuation_expression" => {
            // Parse the base and handle any continuation
            let base = node
                .find_child("continuation_base")
                .and_then(|b| b.children().next())
                .ok_or_else(|| DelightQLError::parse_error("Empty continuation expression"))?;

            // Parse the base - but this might not be a full expression!
            // For predicates, limit_offset, etc. we need to handle them differently
            // This function is for parsing RIGHT SIDE of operators that expect expressions
            // Not all continuations are expressions!
            let base_expr = match base.kind() {
                "table_access" => parse_table_access(base, features)?,
                "catalog_functor" => parse_catalog_functor(base, features)?,
                "anonymous_table" => RelationalExpression::Relation(parse_anonymous_table(base, features)?),
                "tvf_call" => RelationalExpression::Relation(parse_tvf_call(base, features)?),
                "relational_expression" => parse_expression(base, features)?,
                _ => {
                    return Err(DelightQLError::parse_error(format!(
                        "Can't parse {} in continuation",
                        base.kind()
                    )))
                }
            };

            // Check for continuation
            if let Some(cont) = node.find_child("relational_continuation") {
                handle_continuation(cont, base_expr, features)
            } else {
                Ok(base_expr)
            }
        }
        "base_expression" => {
            // base_expression has structure: content + optional sibling continuation
            // We need to parse BOTH to be complete!
            let child = node
                .children()
                .next()
                .ok_or_else(|| DelightQLError::parse_error("Empty base expression"))?;

            let base = match child.kind() {
                "table_access" => parse_table_access(child, features)?,
                "catalog_functor" => parse_catalog_functor(child, features)?,
                "anonymous_table" => RelationalExpression::Relation(parse_anonymous_table(child, features)?),
                "tvf_call" => RelationalExpression::Relation(parse_tvf_call(child, features)?),
                "relational_expression" => parse_expression(child, features)?,
                _ => {
                    return Err(DelightQLError::parse_error(format!(
                        "Can't parse {} in base_expression",
                        child.kind()
                    )))
                }
            };

            // NECESSARY: Check for continuation - it's part of base_expression's structure!
            // Look for a sibling relational_continuation
            // The continuation would be a sibling of the base_expression node
            // But we only have access to the node itself, not its siblings...
            // This is where the CST structure fights us - we need the parent context

            // For now, just return the base. The real fix would be to handle this
            // at the level that can see both the base_expression and its siblings.
            Ok(base)
        }
        _ => Err(DelightQLError::parse_error(format!(
            "Can't parse right side of type {}",
            node.kind()
        ))),
    }
}

// ============================================================================
// ER-join helpers
// ============================================================================

/// Parse a base_expression CST node into a Relation (not a full RelationalExpression).
/// Used by ER-join operators where the right side must be a table/relation.
fn parse_base_as_relation(node: CstNode, features: &mut FeatureCollector) -> Result<Relation> {
    // base_expression wraps a single child: table_access, tvf_call, anonymous_table, etc.
    let child = if node.kind() == "base_expression" {
        node.children()
            .next()
            .ok_or_else(|| DelightQLError::parse_error("Empty base_expression in ER-join"))?
    } else {
        node
    };

    match child.kind() {
        "table_access" => {
            let expr = parse_table_access(child, features)?;
            match expr {
                RelationalExpression::Relation(rel) => Ok(rel),
                other => Err(DelightQLError::parse_error(format!(
                    "ER-join operand must be a simple relation, got complex expression: {:?}",
                    std::mem::discriminant(&other)
                ))),
            }
        }
        "catalog_functor" => match parse_catalog_functor(child, features)? {
            RelationalExpression::Relation(rel) => Ok(rel),
            other => Err(DelightQLError::parse_error(format!(
                "ER-join operand must be a simple relation, got complex expression: {:?}",
                std::mem::discriminant(&other)
            ))),
        },
        "tvf_call" => {
            features.mark(QueryFeature::TableValuedFunctions);
            parse_tvf_call(child, features)
        }
        "anonymous_table" => {
            features.mark(QueryFeature::AnonymousTables);
            parse_anonymous_table(child, features)
        }
        _ => Err(DelightQLError::parse_error(format!(
            "ER-join operand must be a table, got: {}",
            child.kind()
        ))),
    }
}

/// Extract a Relation from a RelationalExpression that wraps a single Relation.
/// Used to flatten ER-join chains: the left side of `&` must be a relation or
/// an existing ErJoinChain.
fn extract_relation_from_expr(expr: RelationalExpression) -> Result<Relation> {
    match expr {
        RelationalExpression::Relation(rel) => Ok(rel),
        _ => Err(DelightQLError::parse_error(
            "ER-join chain (&) requires table operands. Left side is not a simple table reference.",
        )),
    }
}

// ============================================================================
// Assertion handling
// ============================================================================

/// Check if an annotation CST node is an assertion annotation, and if so,
/// parse it into an AssertionSpec. Returns None for non-assertion annotations.
fn extract_assertion_from_annotation(
    meta_node: CstNode,
    cpr: &RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<Option<AssertionSpec>> {
    // annotation → annotation_body → assertion_annotation
    let ann_body = match meta_node.find_child("annotation_body") {
        Some(b) => b,
        None => return Ok(None),
    };
    let assertion_ann = match ann_body.find_child("assertion_annotation") {
        Some(a) => a,
        None => return Ok(None),
    };

    // Found an assertion annotation — parse it
    let assertion_body = assertion_ann
        .field("assertion_body")
        .ok_or_else(|| DelightQLError::parse_error("No assertion_body in assertion_annotation"))?;

    // Detect assertion view (exists/notexists/forall/equals) from the body CST.
    // If no view found, default to Exists (bare assertion).
    // For equals, also extract the right_operand node from the reverse pipe.
    let (predicate, right_operand_node) =
        detect_assertion_view(assertion_body).unwrap_or((AssertionPredicate::Exists, None));

    // Fork: parse the assertion body as a continuation on the current CPR.
    // Assertion views are no-ops in parse_piped_invocation, so the result is the
    // forked relation with all filters/pipes applied but without the view itself.
    let assertion_expr = handle_continuation(assertion_body, cpr.clone(), features)?;

    // For Forall: rewrite the body by negating terminal predicates.
    // Spec: `, P |> forall(*)` → `, NOT(P) |> notexists(*)`
    // The builder negates P so that NOT EXISTS produces the correct semantics.
    // This handles aggregation correctly (COUNT comparison does not).
    let assertion_expr = if predicate == AssertionPredicate::Forall {
        negate_terminal_filters_for_forall(assertion_expr, cpr)?
    } else {
        assertion_expr
    };

    // Source location for error reporting
    let source_location = Some((
        assertion_ann.raw_node().start_byte(),
        assertion_ann.raw_node().end_byte(),
    ));

    // Equals needs the right operand (from reverse pipe <|).
    // The right_operand field is a base_expression node wrapping
    // table_access / anonymous_table / cte_usage.
    let right_operand = match (&predicate, right_operand_node) {
        (AssertionPredicate::Equals, Some(ro_node)) => {
            // Get the first child of the base_expression wrapper
            let child = ro_node
                .children()
                .next()
                .ok_or_else(|| DelightQLError::parse_error("Empty right operand in equals(*)"))?;
            let right_expr = match child.kind() {
                "table_access" => parse_table_access(child, features)?,
                "catalog_functor" => parse_catalog_functor(child, features)?,
                "anonymous_table" => {
                    features.mark(QueryFeature::AnonymousTables);
                    RelationalExpression::Relation(parse_anonymous_table(child, features)?)
                }
                other => {
                    return Err(DelightQLError::parse_error(format!(
                        "Unsupported right operand type in equals(*): {}",
                        other
                    )));
                }
            };
            Some(right_expr)
        }
        (AssertionPredicate::Equals, None) => {
            return Err(DelightQLError::parse_error(
                "equals(*) requires a right operand via reverse pipe <|",
            ));
        }
        // Exists, NotExists, Forall: no right operand needed
        (AssertionPredicate::Exists, _)
        | (AssertionPredicate::NotExists, _)
        | (AssertionPredicate::Forall, _) => None,
    };

    Ok(Some(AssertionSpec {
        body: assertion_expr,
        predicate,
        right_operand,
        source_location,
    }))
}

/// Check if an annotation CST node is an emit annotation, and if so,
/// parse it into an EmitSpec. Returns None for non-emit annotations.
fn extract_emit_from_annotation(
    meta_node: CstNode,
    cpr: &RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<Option<crate::pipeline::asts::core::EmitSpec>> {
    // annotation → annotation_body → emit_annotation
    let ann_body = match meta_node.find_child("annotation_body") {
        Some(b) => b,
        None => return Ok(None),
    };
    let emit_ann = match ann_body.find_child("emit_annotation") {
        Some(a) => a,
        None => return Ok(None),
    };

    // Emit destination: either ://uri (inline) or :identifier (named stream).
    // Inline URIs get prefixed with "//" to reconstruct the full protocol URI.
    let name = if let Some(uri_text) = emit_ann.field_text("emit_uri") {
        format!("//{}", uri_text)
    } else {
        emit_ann
            .field_text("emit_name")
            .unwrap_or_else(|| "default".to_string())
    };

    // Fork: if the emit has a body (predicate continuation), apply it to
    // the CPR. Otherwise, capture the full relation unchanged.
    let emit_expr = if let Some(emit_body) = emit_ann.field("emit_body") {
        handle_continuation(emit_body, cpr.clone(), features)?
    } else {
        cpr.clone()
    };

    let source_location = Some((
        emit_ann.raw_node().start_byte(),
        emit_ann.raw_node().end_byte(),
    ));

    Ok(Some(crate::pipeline::asts::core::EmitSpec {
        name,
        body: emit_expr,
        source_location,
    }))
}

/// Extract a danger gate spec from an annotation.
/// Returns Some(DangerSpec) if this node contains a `danger_annotation`.
fn extract_danger_from_annotation(
    meta_node: CstNode,
) -> Result<Option<crate::pipeline::asts::core::DangerSpec>> {
    // annotation → annotation_body → danger_annotation
    let ann_body = match meta_node.find_child("annotation_body") {
        Some(b) => b,
        None => return Ok(None),
    };
    let danger_ann = match ann_body.find_child("danger_annotation") {
        Some(a) => a,
        None => return Ok(None),
    };

    // Extract the URI path (reuses error_uri_path rule)
    let uri = danger_ann
        .field_text("danger_uri")
        .ok_or_else(|| DelightQLError::parse_error("Danger annotation missing URI path"))?;

    // Extract the toggle state (ON, OFF, ALLOW, or 1-9)
    let state_text = danger_ann
        .field_text("danger_state")
        .ok_or_else(|| DelightQLError::parse_error("Danger annotation missing state toggle"))?;

    let state = match state_text.as_str() {
        "ON" => crate::pipeline::asts::core::DangerState::On,
        "OFF" => crate::pipeline::asts::core::DangerState::Off,
        "ALLOW" => crate::pipeline::asts::core::DangerState::Allow,
        s if s.len() == 1 && s.as_bytes()[0].is_ascii_digit() && s != "0" => {
            crate::pipeline::asts::core::DangerState::Severity(s.parse().unwrap())
        }
        other => {
            return Err(DelightQLError::parse_error(format!(
                "Invalid danger state: '{}'. Expected ON, OFF, ALLOW, or 1-9",
                other
            )));
        }
    };

    let source_location = Some((
        danger_ann.raw_node().start_byte(),
        danger_ann.raw_node().end_byte(),
    ));

    Ok(Some(crate::pipeline::asts::core::DangerSpec {
        uri,
        state,
        source_location,
    }))
}

/// Extract an option spec from an annotation.
/// Returns Some(OptionSpec) if this node contains an `option_annotation`.
fn extract_option_from_annotation(
    meta_node: CstNode,
) -> Result<Option<crate::pipeline::asts::core::OptionSpec>> {
    // annotation → annotation_body → option_annotation
    let ann_body = match meta_node.find_child("annotation_body") {
        Some(b) => b,
        None => return Ok(None),
    };
    let option_ann = match ann_body.find_child("option_annotation") {
        Some(a) => a,
        None => return Ok(None),
    };

    // Extract the URI path (reuses error_uri_path rule)
    let uri = option_ann
        .field_text("option_uri")
        .ok_or_else(|| DelightQLError::parse_error("Option annotation missing URI path"))?;

    // Extract the toggle state (ON, OFF, ALLOW, or 1-9)
    let state_text = option_ann
        .field_text("option_state")
        .ok_or_else(|| DelightQLError::parse_error("Option annotation missing state toggle"))?;

    let state = match state_text.as_str() {
        "ON" => crate::pipeline::asts::core::OptionState::On,
        "OFF" => crate::pipeline::asts::core::OptionState::Off,
        "ALLOW" => crate::pipeline::asts::core::OptionState::Allow,
        s if s.len() == 1 && s.as_bytes()[0].is_ascii_digit() && s != "0" => {
            crate::pipeline::asts::core::OptionState::Severity(s.parse().unwrap())
        }
        other => {
            return Err(DelightQLError::parse_error(format!(
                "Invalid option state: '{}'. Expected ON, OFF, ALLOW, or 1-9",
                other
            )));
        }
    };

    let source_location = Some((
        option_ann.raw_node().start_byte(),
        option_ann.raw_node().end_byte(),
    ));

    Ok(Some(crate::pipeline::asts::core::OptionSpec {
        uri,
        state,
        source_location,
    }))
}

/// Scan an assertion body CST for a piped_invocation that is an assertion
/// view (exists/notexists/forall/equals). Returns the predicate and optional
/// right_operand CST node (for equals with reverse pipe).
/// Bare assertions (no view) default to (Exists, None) in the caller.
fn detect_assertion_view(node: CstNode) -> Option<(AssertionPredicate, Option<CstNode>)> {
    if node.kind() == "piped_invocation" {
        if let Some(name) = node.field_text("function") {
            match name.as_str() {
                "exists" | "∃" => return Some((AssertionPredicate::Exists, None)),
                "notexists" | "∄" => return Some((AssertionPredicate::NotExists, None)),
                "forall" | "∀" => return Some((AssertionPredicate::Forall, None)),
                "equals" | "≡" => {
                    let right = node.field("right_operand");
                    return Some((AssertionPredicate::Equals, right));
                }
                _ => {}
            }
        }
    }
    for child in node.children() {
        if let Some(found) = detect_assertion_view(child) {
            return Some(found);
        }
    }
    None
}

/// Rewrite a forall assertion body by negating its terminal predicates.
///
/// Implements the spec: `, P |> forall(*)` → `, NOT(P) |> notexists(*)`.
///
/// The assertion expression includes both the CPR's filters and the assertion
/// body's filters. Only the assertion body's filters should be negated. We
/// determine which filters belong to the assertion body by comparing the
/// stripped expression to the stripped CPR:
///
/// - If no pipe separates them, the CPR's filters are at the bottom of the
///   terminal filter chain. We strip all terminal filters, subtract the CPR's
///   count, and negate only the assertion body's.
/// - If a pipe was inserted by the assertion body, the CPR's filters are
///   hidden below the pipe. All terminal filters belong to the assertion body.
fn negate_terminal_filters_for_forall(
    expr: RelationalExpression,
    cpr: &RelationalExpression,
) -> Result<RelationalExpression> {
    // Strip all terminal predicate filters from both expression and CPR
    let (expr_conditions, expr_inner) = strip_terminal_pred_filters(expr);
    let (_, cpr_inner) = strip_terminal_pred_filters(cpr.clone());

    // Determine how many terminal filters belong to the assertion body.
    // If expr_inner == cpr_inner, the CPR's filters were part of the
    // terminal chain (no pipe between them). Otherwise, a pipe hides
    // the CPR's filters and all terminal filters are from the assertion body.
    let cpr_terminal_count = count_terminal_pred_filters(cpr);
    let assertion_count = if expr_inner == cpr_inner {
        expr_conditions.len().saturating_sub(cpr_terminal_count)
    } else {
        expr_conditions.len()
    };

    if assertion_count == 0 {
        // forall with no predicate is vacuously true.
        // Add WHERE 1 = 0 (always false) so NOT EXISTS evaluates to TRUE.
        let false_cond = BooleanExpression::Comparison {
            operator: "=".to_string(),
            left: Box::new(DomainExpression::Literal {
                value: LiteralValue::Number("1".to_string()),
                alias: None,
            }),
            right: Box::new(DomainExpression::Literal {
                value: LiteralValue::Number("0".to_string()),
                alias: None,
            }),
        };
        // Reconstruct the original expression, then add the false condition on top
        let mut result = expr_inner;
        for cond in expr_conditions.into_iter().rev() {
            result = RelationalExpression::filter_builder(result)
                .with_condition(SigmaCondition::Predicate(cond))
                .build();
        }
        return Ok(RelationalExpression::filter_builder(result)
            .with_condition(SigmaCondition::Predicate(false_cond))
            .build());
    }

    // Split: assertion body's conditions (outermost) vs CPR's (innermost)
    let to_negate: Vec<_> = expr_conditions[..assertion_count].to_vec();
    let to_keep: Vec<_> = expr_conditions[assertion_count..].to_vec();

    // Rebuild: inner → CPR filters → negated assertion predicate
    let mut result = expr_inner;
    for cond in to_keep.into_iter().rev() {
        result = RelationalExpression::filter_builder(result)
            .with_condition(SigmaCondition::Predicate(cond))
            .build();
    }

    // Conjoin all assertion body conditions: P1 AND P2 AND ...
    let conjunction = to_negate
        .into_iter()
        .reduce(|a, b| BooleanExpression::And {
            left: Box::new(a),
            right: Box::new(b),
        })
        .unwrap(); // Safe: assertion_count > 0

    // Negate: NOT(P1 AND P2 AND ...)
    let negated = BooleanExpression::Not {
        expr: Box::new(conjunction),
    };

    Ok(RelationalExpression::filter_builder(result)
        .with_condition(SigmaCondition::Predicate(negated))
        .build())
}

/// Strip all terminal Predicate-type Filter nodes from the top of an expression.
/// Returns the collected boolean conditions (outer-to-inner order) and the
/// remaining inner expression.
fn strip_terminal_pred_filters(
    mut expr: RelationalExpression,
) -> (Vec<BooleanExpression>, RelationalExpression) {
    let mut conditions = Vec::new();
    loop {
        match expr {
            RelationalExpression::Filter {
                source,
                condition: SigmaCondition::Predicate(bool_expr),
                ..
            } => {
                conditions.push(bool_expr);
                expr = *source;
            }
            other => return (conditions, other),
        }
    }
}

/// Count terminal Predicate-type Filter nodes at the top of an expression.
fn count_terminal_pred_filters(expr: &RelationalExpression) -> usize {
    match expr {
        RelationalExpression::Filter {
            source,
            condition: SigmaCondition::Predicate(_),
            ..
        } => 1 + count_terminal_pred_filters(source),
        _other => 0,
    }
}

// ============================================================================
// Using operator extraction
// ============================================================================

/// Extract ALL Using columns from a continuation chain inductively.
///
/// Walks the entire continuation chain and collects columns from every
/// `.(cols)` operator encountered, accumulating them into a single Vec.
/// Returns the accumulated columns and any remaining non-Using continuation.
///
/// Per STAR-AS-SCOPE-ASSIGNER: multiple `.(cols)` operators accumulate:
///   `users() *.(id).(name)` → USING (id, name)
pub(super) fn extract_using_from_continuation(
    cont: CstNode,
) -> (Option<Vec<String>>, Option<CstNode>) {
    let mut all_columns: Vec<String> = Vec::new();
    extract_using_inductive(cont, &mut all_columns)
}

/// Recursive helper: walk the continuation chain, collecting Using columns.
fn extract_using_inductive<'a>(
    cont: CstNode<'a>,
    accumulated: &mut Vec<String>,
) -> (Option<Vec<String>>, Option<CstNode<'a>>) {
    if let Some(unary) = cont.find_child("unary_operator_expression") {
        if let Some(using_node) = unary.find_child("using_operator") {
            // Extract column names from this .(cols) operator
            if let Some(col_list) = using_node.find_child("using_column_list") {
                for child in col_list.children() {
                    if child.kind() == "identifier" {
                        accumulated.push(crate::pipeline::cst::unstrop(child.text()));
                    }
                }
            }

            // Recurse into nested continuation to find more .(cols)
            if let Some(nested_cont) = unary.find_child("relational_continuation") {
                return extract_using_inductive(nested_cont, accumulated);
            }

            // No more continuations — return all accumulated columns
            let cols = if accumulated.is_empty() {
                None
            } else {
                Some(accumulated.clone())
            };
            return (cols, None);
        }

        // Not a Using operator — check if there's Using deeper in the chain
        // (e.g., * followed by .(cols))
        if let Some(nested_cont) = unary.find_child("relational_continuation") {
            let (cols, remaining) = extract_using_inductive(nested_cont, accumulated);
            if cols.is_some() {
                return (cols, remaining);
            }
        }
    }

    // No Using found at this level
    let cols = if accumulated.is_empty() {
        None
    } else {
        Some(accumulated.clone())
    };
    (cols, Some(cont))
}

/// Apply Using columns to a relation by converting its domain_spec to GlobWithUsing
fn apply_using_columns_to_rel_expr(
    expr: RelationalExpression,
    using_columns: Vec<String>,
) -> RelationalExpression {
    match expr {
        RelationalExpression::Relation(Relation::Ground {
            identifier,
            canonical_name,
            domain_spec,
            alias,
            cpr_schema,
            outer,
            mutation_target,
            passthrough,
            hygienic_injections,
        }) => {
            // Convert domain_spec to GlobWithUsing, or extend existing
            let new_domain_spec = match domain_spec {
                DomainSpec::Bare | DomainSpec::Glob => DomainSpec::GlobWithUsing(using_columns),
                DomainSpec::GlobWithUsing(mut existing) => {
                    // Extend: .(id) followed by .(name) → USING (id, name)
                    existing.extend(using_columns);
                    DomainSpec::GlobWithUsing(existing)
                }
                other => {
                    panic!("catch-all hit in builder_v2/continuation.rs apply_using_to_expr DomainSpec: {:?}", other)
                }
            };
            RelationalExpression::Relation(Relation::Ground {
                identifier,
                canonical_name,
                domain_spec: new_domain_spec,
                alias,
                cpr_schema,
                outer,
                mutation_target,
                passthrough,
                hygienic_injections,
            })
        }
        // For other expression types, panic to surface unexpected variant
        other => {
            panic!("catch-all hit in builder_v2/continuation.rs apply_using_to_expr RelationalExpression: {:?}", other)
        }
    }
}
