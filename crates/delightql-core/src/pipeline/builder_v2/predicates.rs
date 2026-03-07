//! Predicates, comparisons, EXISTS parsing

use super::expressions::*;

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::FeatureCollector;

/// Parse predicate as BooleanExpression (for WHERE/ON/HAVING clauses)
pub(super) fn parse_predicate_as_boolean(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<BooleanExpression> {
    // Check for boolean combination expressions first (higher precedence)
    if let Some(and_expr) = node.find_child("and_expression") {
        let left = and_expr
            .field("left")
            .ok_or_else(|| DelightQLError::parse_error("AND missing left operand"))?;
        let right = and_expr
            .field("right")
            .ok_or_else(|| DelightQLError::parse_error("AND missing right operand"))?;

        return Ok(BooleanExpression::And {
            left: Box::new(parse_predicate_as_boolean(left, features)?),
            right: Box::new(parse_predicate_as_boolean(right, features)?),
        });
    }

    if let Some(or_expr) = node.find_child("or_expression") {
        let left = or_expr
            .field("left")
            .ok_or_else(|| DelightQLError::parse_error("OR missing left operand"))?;
        let right = or_expr
            .field("right")
            .ok_or_else(|| DelightQLError::parse_error("OR missing right operand"))?;

        return Ok(BooleanExpression::Or {
            left: Box::new(parse_predicate_as_boolean(left, features)?),
            right: Box::new(parse_predicate_as_boolean(right, features)?),
        });
    }

    // Handle semicolon as OR (inside parentheses only)
    if let Some(or_expr) = node.find_child("or_expression_with_semicolon") {
        let left = or_expr
            .field("left")
            .ok_or_else(|| DelightQLError::parse_error("OR missing left operand"))?;
        let right = or_expr
            .field("right")
            .ok_or_else(|| DelightQLError::parse_error("OR missing right operand"))?;

        return Ok(BooleanExpression::Or {
            left: Box::new(parse_predicate_as_boolean(left, features)?),
            right: Box::new(parse_predicate_as_boolean(right, features)?),
        });
    }

    // Handle paren_predicate nodes directly (when node IS a paren_predicate)
    if node.kind() == "paren_predicate" {
        // Look for inner predicate or or_expression_with_semicolon
        if let Some(inner) = node.find_child("or_expression_with_semicolon") {
            return parse_predicate_as_boolean(inner, features);
        }
        if let Some(inner) = node.find_child("predicate") {
            return parse_predicate_as_boolean(inner, features);
        }
    }

    // Check for atomic predicate types
    if let Some(atomic) = node.find_child("atomic_predicate") {
        // Check if atomic_predicate contains a paren_predicate
        if let Some(paren_pred) = atomic.find_child("paren_predicate") {
            return parse_predicate_as_boolean(paren_pred, features);
        }
        // Check if atomic_predicate contains a predicate (legacy parenthesized case)
        if let Some(inner_pred) = atomic.find_child("predicate") {
            // Parenthesized predicate case: atomic_predicate -> '(' predicate ')'
            return parse_predicate_as_boolean(inner_pred, features);
        }
        // Otherwise recurse into atomic_predicate for comparison/exists
        return parse_predicate_as_boolean(atomic, features);
    }

    // Check for different predicate types
    if let Some(comparison) = node.find_child("comparison") {
        return parse_comparison_as_boolean(comparison, features);
    }

    if let Some(inner_exists) = node.find_child("inner_exists") {
        return parse_inner_exists_as_boolean(inner_exists, features);
    }

    if let Some(in_rel) = node.find_child("in_relational_predicate") {
        return parse_in_relational_as_boolean(in_rel, features);
    }

    if let Some(in_pred) = node.find_child("in_predicate") {
        return parse_in_predicate_as_boolean(in_pred, features);
    }

    if let Some(bool_lit) = node.find_child("boolean_literal") {
        let value = bool_lit.text() == "true";
        return Ok(BooleanExpression::BooleanLiteral { value });
    }

    if let Some(not_expr) = node.find_child("not_expression") {
        let inner = not_expr
            .field("expr")
            .ok_or_else(|| DelightQLError::parse_error("NOT expression missing inner predicate"))?;
        return Ok(BooleanExpression::Not {
            expr: Box::new(parse_predicate_as_boolean(inner, features)?),
        });
    }

    if let Some(sigma_call) = node.find_child("sigma_call") {
        return parse_sigma_call_as_boolean(sigma_call);
    }

    Err(DelightQLError::parse_error("Unknown predicate type"))
}

/// Parse column spec: glob_spec or column_list
/// Parse predicate (for use in projections - returns DomainExpression)
/// TODO: This should eventually return ProjectionItem that can handle both domains and booleans
pub(super) fn parse_predicate(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<DomainExpression> {
    // Check for boolean combination expressions first
    if node.has_child("and_expression")
        || node.has_child("or_expression")
        || node.has_child("or_expression_with_semicolon")
    {
        // For AND/OR expressions in projections, parse as boolean and wrap
        let bool_expr = parse_predicate_as_boolean(node, features)?;
        return Ok(DomainExpression::Predicate {
            expr: Box::new(bool_expr),
            alias: None,
        });
    }

    // Check for atomic predicate
    if let Some(atomic) = node.find_child("atomic_predicate") {
        // Check if atomic_predicate contains a parenthesized predicate
        if let Some(inner_pred) = atomic.find_child("predicate") {
            // Parenthesized predicate case: atomic_predicate -> '(' predicate ')'
            return parse_predicate(inner_pred, features);
        }
        // Otherwise recurse into atomic_predicate for comparison/exists
        return parse_predicate(atomic, features);
    }

    // Check for different predicate types
    if let Some(comparison) = node.find_child("comparison") {
        return parse_comparison(comparison, features);
    }

    if let Some(inner_exists) = node.find_child("inner_exists") {
        let exists = inner_exists
            .find_child("exists_marker")
            .map(|marker| !marker.has_child("not_exists"))
            .unwrap_or(true);

        let passthrough = inner_exists.has_child("passthrough_separator");

        let (namespace_path, grounding) =
            if let Some(ns_node) = inner_exists.field("namespace_path") {
                super::relations::parse_namespace_qualification(ns_node)?
            } else {
                (NamespacePath::empty(), None)
            };

        let raw_table_name = inner_exists
            .field_text("table")
            .ok_or_else(|| DelightQLError::parse_error("No table name in inner_exists"))?;

        // HO table name substitution
        let table_name = if namespace_path.is_empty() && grounding.is_none() {
            if let Some(ref bindings) = features.ho_bindings {
                if let Some(actual) = bindings.table_params.get(raw_table_name.as_str()) {
                    actual.clone()
                } else {
                    raw_table_name.to_string()
                }
            } else {
                raw_table_name.to_string()
            }
        } else {
            raw_table_name.to_string()
        };

        let identifier = QualifiedName {
            namespace_path: namespace_path.clone(),
            name: table_name.into(),
            grounding: grounding.clone(),
        };

        let alias = inner_exists
            .find_child("table_alias")
            .and_then(|n| n.field_text("name"));

        // HO table expr substitution: if table name matches a table_expr param, use bound expr
        let base = if namespace_path.is_empty() && grounding.is_none() {
            if let Some(ref bindings) = features.ho_bindings {
                if let Some(bound_expr) = bindings.table_expr_params.get(raw_table_name.as_str()) {
                    bound_expr.clone()
                } else {
                    RelationalExpression::Relation(Relation::Ground {
                        identifier: identifier.clone(),
                        canonical_name: PhaseBox::phantom(),
                        domain_spec: DomainSpec::Glob,
                        alias: alias.clone().map(|s| s.into()),
                        outer: false,
                        mutation_target: false,
                        passthrough,
                        cpr_schema: PhaseBox::phantom(),
                        hygienic_injections: Vec::new(),
                    })
                }
            } else {
                RelationalExpression::Relation(Relation::Ground {
                    identifier: identifier.clone(),
                    canonical_name: PhaseBox::phantom(),
                    domain_spec: DomainSpec::Glob,
                    alias: alias.clone().map(|s| s.into()),
                    outer: false,
                    mutation_target: false,
                    passthrough,
                    cpr_schema: PhaseBox::phantom(),
                    hygienic_injections: Vec::new(),
                })
            }
        } else {
            RelationalExpression::Relation(Relation::Ground {
                identifier: identifier.clone(),
                canonical_name: PhaseBox::phantom(),
                domain_spec: DomainSpec::Glob,
                alias: alias.clone().map(|s| s.into()),
                outer: false,
                mutation_target: false,
                passthrough,
                cpr_schema: PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            })
        };

        let continuation_node = inner_exists
            .find_child("relational_continuation")
            .ok_or_else(|| {
                DelightQLError::parse_error(
                    "No continuation in inner_exists - continuation is required",
                )
            })?;

        // Extract USING columns from the continuation (e.g., +orders(*.(status)))
        let (using_cols, remaining_cont) =
            super::continuation::extract_using_from_continuation(continuation_node);
        let using_columns = match using_cols {
            Some(super::continuation::ExtractedUsing::Columns(cols)) => cols,
            Some(super::continuation::ExtractedUsing::All) => {
                return Err(crate::error::DelightQLError::parse_error(
                    ".* (USING all shared columns) is not supported in inner exists predicates; use explicit .(col1, col2) instead"
                ));
            }
            None => Vec::new(),
        };

        let mut dummy_features = FeatureCollector::inheriting_ho_bindings(features);
        let subquery = if let Some(remaining) = remaining_cont {
            super::continuation::handle_continuation(remaining, base, &mut dummy_features)?
        } else {
            // No continuation left after extracting USING — just use base
            base
        };

        let inner_exists_bool = BooleanExpression::InnerExists {
            exists,
            identifier,
            subquery: Box::new(subquery),
            alias,
            using_columns,
        };

        return Ok(DomainExpression::Predicate {
            expr: Box::new(inner_exists_bool),
            alias: None,
        });
    }

    if let Some(in_rel) = node.find_child("in_relational_predicate") {
        let bool_expr = parse_in_relational_as_boolean(in_rel, features)?;
        return Ok(DomainExpression::Predicate {
            expr: Box::new(bool_expr),
            alias: None,
        });
    }

    if let Some(in_pred) = node.find_child("in_predicate") {
        let bool_expr = parse_in_predicate_as_boolean(in_pred, features)?;
        return Ok(DomainExpression::Predicate {
            expr: Box::new(bool_expr),
            alias: None,
        });
    }

    if let Some(bool_lit) = node.find_child("boolean_literal") {
        let value = bool_lit.text() == "true";
        return Ok(DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::BooleanLiteral { value }),
            alias: None,
        });
    }

    if let Some(not_expr) = node.find_child("not_expression") {
        let inner = not_expr
            .field("expr")
            .ok_or_else(|| DelightQLError::parse_error("NOT expression missing inner predicate"))?;
        return Ok(DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::Not {
                expr: Box::new(parse_predicate_as_boolean(inner, features)?),
            }),
            alias: None,
        });
    }

    if let Some(sigma_call) = node.find_child("sigma_call") {
        let bool_expr = parse_sigma_call_as_boolean(sigma_call)?;
        return Ok(DomainExpression::Predicate {
            expr: Box::new(bool_expr),
            alias: None,
        });
    }

    Err(DelightQLError::parse_error("Unknown predicate type"))
}

/// Parse comparison as BooleanExpression
pub(super) fn parse_comparison_as_boolean(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<BooleanExpression> {
    let operator_node = node
        .field("operator")
        .ok_or_else(|| DelightQLError::parse_error("No operator in comparison"))?;
    let operator = operator_node
        .children()
        .next()
        .map(|child| child.kind().to_string())
        .unwrap_or_else(|| operator_node.kind().to_string());

    let left_node = node
        .field("left")
        .ok_or_else(|| DelightQLError::parse_error("No left in comparison"))?;
    let right_node = node
        .field("right")
        .ok_or_else(|| DelightQLError::parse_error("No right in comparison"))?;

    // Check if this is AND/OR (boolean operator) or a comparison
    match operator.as_str() {
        "AND" | "and" | "&&" => {
            // Both sides must be predicates - parse them as boolean expressions
            let left_bool = parse_predicate_as_boolean(left_node, features)?;
            let right_bool = parse_predicate_as_boolean(right_node, features)?;
            Ok(BooleanExpression::And {
                left: Box::new(left_bool),
                right: Box::new(right_bool),
            })
        }
        "OR" | "or" | "||" => {
            // Both sides must be predicates - parse them as boolean expressions
            let left_bool = parse_predicate_as_boolean(left_node, features)?;
            let right_bool = parse_predicate_as_boolean(right_node, features)?;
            Ok(BooleanExpression::Or {
                left: Box::new(left_bool),
                right: Box::new(right_bool),
            })
        }
        _ => {
            // It's a comparison operator - parse operands as domain expressions
            let left = parse_simple_expression(left_node, features)?;
            let right = parse_simple_expression(right_node, features)?;

            // Detect glob-glob comparison (x.* = y.*) → GlobCorrelation
            if let (
                DomainExpression::Projection(ProjectionExpr::Glob {
                    qualifier: Some(ref lq),
                    ..
                }),
                DomainExpression::Projection(ProjectionExpr::Glob {
                    qualifier: Some(ref rq),
                    ..
                }),
            ) = (&left, &right)
            {
                return Ok(BooleanExpression::GlobCorrelation {
                    left: lq.clone(),
                    right: rq.clone(),
                });
            }

            // Detect ordinal-glob comparison (x|*| = y|*|) → OrdinalGlobCorrelation
            if let (
                DomainExpression::ColumnOrdinal(ref lo),
                DomainExpression::ColumnOrdinal(ref ro),
            ) = (&left, &right)
            {
                if lo.get().glob && ro.get().glob {
                    if let (Some(lq), Some(rq)) = (&lo.get().qualifier, &ro.get().qualifier) {
                        return Ok(BooleanExpression::OrdinalGlobCorrelation {
                            left: lq.clone().into(),
                            right: rq.clone().into(),
                        });
                    }
                }
            }

            Ok(BooleanExpression::Comparison {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
    }
}

/// Parse inner exists as BooleanExpression
pub(super) fn parse_inner_exists_as_boolean(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<BooleanExpression> {
    let exists = node
        .find_child("exists_marker")
        .map(|marker| !marker.has_child("not_exists"))
        .unwrap_or(true);

    let passthrough = node.has_child("passthrough_separator");

    let (namespace_path, grounding) = if let Some(ns_node) = node.field("namespace_path") {
        super::relations::parse_namespace_qualification(ns_node)?
    } else {
        (NamespacePath::empty(), None)
    };

    let raw_table_name = node
        .field_text("table")
        .ok_or_else(|| DelightQLError::parse_error("No table name in inner_exists"))?;

    // HO table name substitution
    let table_name = if namespace_path.is_empty() && grounding.is_none() {
        if let Some(ref bindings) = features.ho_bindings {
            bindings
                .table_params
                .get(raw_table_name.as_str())
                .cloned()
                .unwrap_or_else(|| raw_table_name.to_string())
        } else {
            raw_table_name.to_string()
        }
    } else {
        raw_table_name.to_string()
    };

    let identifier = QualifiedName {
        namespace_path: namespace_path.clone(),
        name: table_name.into(),
        grounding: grounding.clone(),
    };

    let alias = node
        .find_child("table_alias")
        .and_then(|n| n.field_text("name"));

    // HO table expr substitution
    let base = if namespace_path.is_empty() && grounding.is_none() {
        if let Some(ref bindings) = features.ho_bindings {
            if let Some(bound_expr) = bindings.table_expr_params.get(raw_table_name.as_str()) {
                bound_expr.clone()
            } else {
                RelationalExpression::Relation(Relation::Ground {
                    identifier: identifier.clone(),
                    canonical_name: PhaseBox::phantom(),
                    domain_spec: DomainSpec::Glob,
                    alias: alias.clone().map(|s| s.into()),
                    outer: false,
                    mutation_target: false,
                    passthrough,
                    cpr_schema: PhaseBox::phantom(),
                    hygienic_injections: Vec::new(),
                })
            }
        } else {
            RelationalExpression::Relation(Relation::Ground {
                identifier: identifier.clone(),
                canonical_name: PhaseBox::phantom(),
                domain_spec: DomainSpec::Glob,
                alias: alias.clone().map(|s| s.into()),
                outer: false,
                mutation_target: false,
                passthrough,
                cpr_schema: PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            })
        }
    } else {
        RelationalExpression::Relation(Relation::Ground {
            identifier: identifier.clone(),
            canonical_name: PhaseBox::phantom(),
            domain_spec: DomainSpec::Glob,
            alias: alias.clone().map(|s| s.into()),
            outer: false,
            mutation_target: false,
            passthrough,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        })
    };

    let continuation_node = node.find_child("relational_continuation").ok_or_else(|| {
        DelightQLError::parse_error("No continuation in inner_exists - continuation is required")
    })?;

    // Extract USING columns from the continuation (e.g., +orders(*.(status)))
    let (using_cols, remaining_cont) =
        super::continuation::extract_using_from_continuation(continuation_node);
    let using_columns = match using_cols {
        Some(super::continuation::ExtractedUsing::Columns(cols)) => cols,
        Some(super::continuation::ExtractedUsing::All) => {
            return Err(crate::error::DelightQLError::parse_error(
                ".* (USING all shared columns) is not supported in inner exists predicates; use explicit .(col1, col2) instead"
            ));
        }
        None => Vec::new(),
    };

    let mut dummy_features = FeatureCollector::inheriting_ho_bindings(features);
    let subquery = if let Some(remaining) = remaining_cont {
        super::continuation::handle_continuation(remaining, base, &mut dummy_features)?
    } else {
        base
    };

    Ok(BooleanExpression::InnerExists {
        exists,
        identifier,
        subquery: Box::new(subquery),
        alias,
        using_columns,
    })
}

/// Parse comparison (for projections - returns DomainExpression)
pub(super) fn parse_comparison(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<DomainExpression> {
    let operator_node = node
        .field("operator")
        .ok_or_else(|| DelightQLError::parse_error("No operator in comparison"))?;
    // The operator field contains a comparison_operator node, get its child
    let operator = operator_node
        .children()
        .next()
        .map(|child| child.kind().to_string())
        .unwrap_or_else(|| operator_node.kind().to_string());

    // Find left and right
    let left_node = node
        .field("left")
        .ok_or_else(|| DelightQLError::parse_error("No left in comparison"))?;
    let right_node = node
        .field("right")
        .ok_or_else(|| DelightQLError::parse_error("No right in comparison"))?;

    let left = parse_simple_expression(left_node, features)?;
    let right = parse_simple_expression(right_node, features)?;

    // Create a boolean comparison expression
    let bool_expr = BooleanExpression::Comparison {
        operator,
        left: Box::new(left),
        right: Box::new(right),
    };

    // For projection contexts, we need to wrap the boolean in DomainExpression::Predicate
    // This models SQL's semantic where booleans become values in SELECT clauses
    Ok(DomainExpression::Predicate {
        expr: Box::new(bool_expr),
        alias: None,
    })
}

/// Parse IN predicate as BooleanExpression
/// Grammar: value IN (set_item1; set_item2; ...)
/// Example: status in ("active"; "pending")
pub(super) fn parse_in_predicate_as_boolean(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<BooleanExpression> {
    // Get the value expression (left side of IN)
    let value_node = node
        .field("value")
        .ok_or_else(|| DelightQLError::parse_error("No value in in_predicate"))?;
    let value_expr = parse_simple_expression(value_node, features)?;

    // Get the set (right side of IN) - semicolon-separated list
    let set_node = node
        .field("set")
        .ok_or_else(|| DelightQLError::parse_error("No set in in_predicate"))?;

    // EPOCH 5: Parse the in_value_list - each child is an in_value_row
    // Each in_value_row can have multiple comma-separated values (for tuple IN)
    let mut set_exprs = Vec::new();
    for child in set_node.children() {
        if child.kind() == "in_value_row" {
            // Collect all values in this row
            let mut row_values = Vec::new();
            for value_node in child.children() {
                if value_node.kind() == "non_binary_domain_expression" {
                    row_values.push(parse_simple_expression(value_node, features)?);
                }
            }

            if row_values.is_empty() {
                return Err(DelightQLError::parse_error("Empty row in IN set"));
            }

            // If single value, add directly; if multiple, wrap in tuple
            if row_values.len() == 1 {
                set_exprs.push(row_values.into_iter().next().unwrap());
            } else {
                // Multi-column row - create tuple
                set_exprs.push(DomainExpression::Tuple {
                    elements: row_values,
                    alias: None,
                });
            }
        }
    }

    if set_exprs.is_empty() {
        return Err(DelightQLError::parse_error("Empty IN set"));
    }

    // Validate arity: if left side is scalar, rows must also be scalar.
    // (2,3,3) creates a Tuple — user probably meant (2;3;3).
    // Tuple IN like (a, b) in (1, 2; 3, 4) is valid — left is also a tuple.
    let left_is_tuple = matches!(&value_expr, DomainExpression::Tuple { .. });
    if !left_is_tuple {
        for expr in &set_exprs {
            if let DomainExpression::Tuple { elements, .. } = expr {
                return Err(DelightQLError::parse_error_categorized(
                    "in",
                    format!(
                        "IN value list has {}-column rows but left side is a single value. \
                         Use semicolons to separate values: a in (2;3;3), not commas: a in (2,3,3)",
                        elements.len()
                    ),
                ));
            }
        }
    }

    // Detect NOT IN vs IN from operator node
    let negated = node
        .field("operator")
        .map(|op| op.has_child("not_in_op") || op.kind() == "not_in_op")
        .unwrap_or(false);

    // Build BooleanExpression::In
    Ok(BooleanExpression::In {
        value: Box::new(value_expr),
        set: set_exprs,
        negated,
    })
}

/// Parse IN relational predicate as BooleanExpression
/// Grammar: value [NOT] IN [namespace.]table( continuation )
/// Example: DepartmentId in department(|> (DepartmentId))
pub(super) fn parse_in_relational_as_boolean(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<BooleanExpression> {
    // Get the value expression (left side of IN)
    let value_node = node
        .field("value")
        .ok_or_else(|| DelightQLError::parse_error("No value in in_relational_predicate"))?;
    let value_expr = parse_simple_expression(value_node, features)?;

    // Detect NOT IN vs IN from operator node
    let negated = node
        .field("operator")
        .map(|op| op.has_child("not_in_op") || op.kind() == "not_in_op")
        .unwrap_or(false);

    // Parse namespace qualification
    let (namespace_path, grounding) = if let Some(ns_node) = node.field("namespace_path") {
        super::relations::parse_namespace_qualification(ns_node)?
    } else {
        (NamespacePath::empty(), None)
    };

    // Get table name (with HO substitution)
    let raw_table_name = node
        .field_text("table")
        .ok_or_else(|| DelightQLError::parse_error("No table name in in_relational_predicate"))?;

    let table_name = if namespace_path.is_empty() && grounding.is_none() {
        if let Some(ref bindings) = features.ho_bindings {
            bindings
                .table_params
                .get(raw_table_name.as_str())
                .cloned()
                .unwrap_or_else(|| raw_table_name.to_string())
        } else {
            raw_table_name.to_string()
        }
    } else {
        raw_table_name.to_string()
    };

    let identifier = QualifiedName {
        namespace_path: namespace_path.clone(),
        name: table_name.into(),
        grounding: grounding.clone(),
    };

    // Build base relation (with HO table expr substitution)
    let base = if namespace_path.is_empty() && grounding.is_none() {
        if let Some(ref bindings) = features.ho_bindings {
            if let Some(bound_expr) = bindings.table_expr_params.get(raw_table_name.as_str()) {
                bound_expr.clone()
            } else {
                RelationalExpression::Relation(Relation::Ground {
                    identifier: identifier.clone(),
                    canonical_name: PhaseBox::phantom(),
                    domain_spec: DomainSpec::Glob,
                    alias: None,
                    outer: false,
                    mutation_target: false,
                    passthrough: false,
                    cpr_schema: PhaseBox::phantom(),
                    hygienic_injections: Vec::new(),
                })
            }
        } else {
            RelationalExpression::Relation(Relation::Ground {
                identifier: identifier.clone(),
                canonical_name: PhaseBox::phantom(),
                domain_spec: DomainSpec::Glob,
                alias: None,
                outer: false,
                mutation_target: false,
                passthrough: false,
                cpr_schema: PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            })
        }
    } else {
        RelationalExpression::Relation(Relation::Ground {
            identifier: identifier.clone(),
            canonical_name: PhaseBox::phantom(),
            domain_spec: DomainSpec::Glob,
            alias: None,
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        })
    };

    // Parse continuation (required)
    let continuation_node = node.find_child("relational_continuation").ok_or_else(|| {
        DelightQLError::parse_error(
            "No continuation in in_relational_predicate - continuation is required",
        )
    })?;

    let mut dummy_features = FeatureCollector::inheriting_ho_bindings(features);
    let subquery =
        super::continuation::handle_continuation(continuation_node, base, &mut dummy_features)?;

    Ok(BooleanExpression::InRelational {
        value: Box::new(value_expr),
        subquery: Box::new(subquery),
        identifier,
        negated,
    })
}

/// Check if a predicate node contains a destructuring operator
pub(super) fn is_destructuring_predicate(node: CstNode) -> bool {
    // predicate -> atomic_predicate -> comparison -> operator
    let comparison = node
        .find_child("atomic_predicate")
        .and_then(|ap| ap.find_child("comparison"))
        .or_else(|| node.find_child("comparison"));

    if let Some(comp) = comparison {
        if let Some(op_node) = comp.field("operator") {
            if let Some(child) = op_node.children().next() {
                let kind = child.kind();
                return kind.starts_with("destructure");
            }
        }
    }
    false
}

/// Parse destructuring predicate as SigmaCondition::Destructure
pub(super) fn parse_destructuring_sigma(node: CstNode) -> Result<SigmaCondition> {
    // predicate -> atomic_predicate -> comparison
    let comparison = node
        .find_child("atomic_predicate")
        .and_then(|ap| ap.find_child("comparison"))
        .or_else(|| node.find_child("comparison"))
        .ok_or_else(|| DelightQLError::parse_error("Expected comparison node for destructuring"))?;

    let left_node = comparison.field("left").ok_or_else(|| {
        DelightQLError::parse_error("Destructuring missing left operand (JSON column)")
    })?;

    let right_node = comparison.field("right").ok_or_else(|| {
        DelightQLError::parse_error("Destructuring missing right operand (pattern)")
    })?;

    let operator_node = comparison
        .field("operator")
        .ok_or_else(|| DelightQLError::parse_error("Destructuring missing operator"))?;

    // Get operator kind
    let operator_kind = operator_node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty operator node"))?
        .kind()
        .to_string();

    // Parse JSON column (left side)
    let mut destr_features = FeatureCollector::new();
    let json_column = parse_simple_expression(left_node, &mut destr_features)?;

    // Parse pattern (right side) - must be a Curly function or MetadataTreeGroup
    let pattern_expr = parse_simple_expression(right_node, &mut destr_features)?;

    let pattern = match pattern_expr {
        DomainExpression::Function(f @ FunctionExpression::Curly { .. }) => f,
        DomainExpression::Function(f @ FunctionExpression::Array { .. }) => f,
        DomainExpression::Function(f @ FunctionExpression::MetadataTreeGroup { .. }) => f,
        _ => {
            return Err(DelightQLError::parse_error(
                "Destructuring pattern must be a Curly function {...}, Array pattern [...], or Metadata Tree Group (key:~> ...)"
            ))
        }
    };

    // Determine mode from operator
    use crate::pipeline::asts::unresolved::DestructureMode;
    let mode = match operator_kind.as_str() {
        "destructure_scalar_op" => DestructureMode::Scalar,
        "destructure_aggregate_op" => DestructureMode::Aggregate,
        _ => {
            return Err(DelightQLError::parse_error(&format!(
                "Unknown destructuring operator: {}",
                operator_kind
            )))
        }
    };

    Ok(SigmaCondition::Destructure {
        json_column: Box::new(json_column),
        pattern: Box::new(pattern),
        mode,
        destructured_schema: PhaseBox::phantom(),
    })
}

/// Parse sigma predicate call as BooleanExpression
/// Syntax: +like(arg1, arg2) or \+like(arg1, arg2)
pub(super) fn parse_sigma_call_as_boolean(node: CstNode) -> Result<BooleanExpression> {
    // Extract EXISTS/NOT EXISTS operator
    let exists = node
        .find_child("exists_marker")
        .map(|marker| !marker.has_child("not_exists"))
        .unwrap_or(true);

    // Extract functor name
    let functor = node
        .field_text("functor")
        .ok_or_else(|| DelightQLError::parse_error("No functor in sigma_call"))?
        .to_string();

    // Extract arguments
    let arguments = if let Some(args_node) = node.find_child("sigma_argument_list") {
        let mut args = Vec::new();
        let mut sigma_features = FeatureCollector::new();
        for child in args_node.children() {
            if child.kind() == "domain_expression" {
                args.push(parse_domain_expression_wrapper(child, &mut sigma_features)?);
            }
        }
        args
    } else {
        Vec::new()
    };

    // Represent sigma calls as SigmaCondition::SigmaCall wrapped in Sigma BooleanExpression
    Ok(BooleanExpression::Sigma {
        condition: Box::new(SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
        }),
    })
}
