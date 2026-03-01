// Provenance population - Post-refiner fixup for CFE parameter tracking
// This walks an already-refined tree and populates provenance fields based on qualifier matching
//
// SIMPLIFIED VERSION: Only handles critical paths (Filter predicates, EXISTS)
// The full tree walk is complex due to Refined AST structure differences

use crate::pipeline::asts::refined;

/// Populate provenance in an already-refined relational tree
/// Focuses on Filter predicates and EXISTS subqueries where parameters appear
pub(super) fn populate_provenance_in_relational(
    expr: &mut refined::RelationalExpression,
    curried_params: &[String],
    regular_params: &[String],
    context_params: &[String],
) {
    use refined::RelationalExpression;

    match expr {
        RelationalExpression::Filter {
            source, condition, ..
        } => {
            populate_provenance_in_relational(
                source,
                curried_params,
                regular_params,
                context_params,
            );
            if let refined::SigmaCondition::Predicate(pred) = condition {
                populate_provenance_in_boolean(
                    pred,
                    curried_params,
                    regular_params,
                    context_params,
                );
            }
        }
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            populate_provenance_in_relational(left, curried_params, regular_params, context_params);
            populate_provenance_in_relational(
                right,
                curried_params,
                regular_params,
                context_params,
            );
            if let Some(condition) = join_condition {
                populate_provenance_in_boolean(
                    condition,
                    curried_params,
                    regular_params,
                    context_params,
                );
            }
        }
        RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                populate_provenance_in_relational(
                    operand,
                    curried_params,
                    regular_params,
                    context_params,
                );
            }
        }
        RelationalExpression::Pipe(pipe_expr) => {
            populate_provenance_in_relational(
                &mut pipe_expr.source,
                curried_params,
                regular_params,
                context_params,
            );
        }
        // Relation: leaf node — no expressions to populate provenance in
        RelationalExpression::Relation(_) => {}
        // ER chains should be consumed before CFE precompilation
        RelationalExpression::ErJoinChain { .. }
        | RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before CFE precompilation")
        }
    }
}

fn populate_provenance_in_domain(
    expr: &mut refined::DomainExpression,
    curried_params: &[String],
    regular_params: &[String],
    context_params: &[String],
) {
    use refined::{DomainExpression, LvarProvenance};

    match expr {
        DomainExpression::Lvar {
            name,
            qualifier,
            provenance,
            ..
        } => {
            // Determine provenance based on parameter list membership
            // NOTE: Resolver doesn't add qualifiers even with fake schema - just validates
            // So we check parameter lists directly, not qualifiers
            let new_provenance = if curried_params.iter().any(|p| name == p.as_str()) {
                log::debug!(
                    "🔧 PROVENANCE FIXER: {} (qual={:?}) → CfeCurriedParameter",
                    name,
                    qualifier
                );
                Some(LvarProvenance::CfeCurriedParameter)
            } else if regular_params.iter().any(|p| name == p.as_str()) {
                log::debug!(
                    "🔧 PROVENANCE FIXER: {} (qual={:?}) → CfeParameter",
                    name,
                    qualifier
                );
                Some(LvarProvenance::CfeParameter)
            } else if context_params.iter().any(|p| name == p.as_str()) {
                log::debug!(
                    "🔧 PROVENANCE FIXER: {} (qual={:?}) → CfeContext",
                    name,
                    qualifier
                );
                Some(LvarProvenance::CfeContext)
            } else {
                log::debug!(
                    "🔧 PROVENANCE FIXER: {} (qual={:?}) → None (real table)",
                    name,
                    qualifier
                );
                None
            };

            *provenance = refined::PhaseBox::new(new_provenance);
        }
        DomainExpression::Predicate { expr, .. } => {
            populate_provenance_in_boolean(expr, curried_params, regular_params, context_params);
        }
        DomainExpression::ScalarSubquery { subquery, .. } => {
            populate_provenance_in_relational(
                subquery,
                curried_params,
                regular_params,
                context_params,
            );
        }
        DomainExpression::Parenthesized { inner, .. } => {
            populate_provenance_in_domain(inner, curried_params, regular_params, context_params);
        }
        DomainExpression::Tuple { elements, .. } => {
            for elem in elements {
                populate_provenance_in_domain(elem, curried_params, regular_params, context_params);
            }
        }
        // Function expressions: recurse into arguments
        DomainExpression::Function(func) => {
            populate_provenance_in_function(func, curried_params, regular_params, context_params);
        }
        // PipedExpression: recurse into value and transforms
        DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            populate_provenance_in_domain(value, curried_params, regular_params, context_params);
            for t in transforms {
                populate_provenance_in_function(t, curried_params, regular_params, context_params);
            }
        }
        // PivotOf: recurse into value and key
        DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            populate_provenance_in_domain(
                value_column,
                curried_params,
                regular_params,
                context_params,
            );
            populate_provenance_in_domain(
                pivot_key,
                curried_params,
                regular_params,
                context_params,
            );
        }
        // Leaf types: no Lvars inside, nothing to do
        DomainExpression::Literal { .. }
        | DomainExpression::Projection(_)
        | DomainExpression::ValuePlaceholder { .. }
        | DomainExpression::NonUnifiyingUnderscore
        | DomainExpression::Substitution(_)
        | DomainExpression::ColumnOrdinal(_) => {}
    }
}

fn populate_provenance_in_boolean(
    expr: &mut refined::BooleanExpression,
    curried_params: &[String],
    regular_params: &[String],
    context_params: &[String],
) {
    use refined::BooleanExpression;

    match expr {
        BooleanExpression::Comparison { left, right, .. } => {
            populate_provenance_in_domain(left, curried_params, regular_params, context_params);
            populate_provenance_in_domain(right, curried_params, regular_params, context_params);
        }
        BooleanExpression::And { left, right } | BooleanExpression::Or { left, right } => {
            populate_provenance_in_boolean(left, curried_params, regular_params, context_params);
            populate_provenance_in_boolean(right, curried_params, regular_params, context_params);
        }
        BooleanExpression::Not { expr } => {
            populate_provenance_in_boolean(expr, curried_params, regular_params, context_params);
        }
        BooleanExpression::InnerExists { subquery, .. } => {
            populate_provenance_in_relational(
                subquery,
                curried_params,
                regular_params,
                context_params,
            );
        }
        BooleanExpression::In { value, set, .. } => {
            populate_provenance_in_domain(value, curried_params, regular_params, context_params);
            for elem in set {
                populate_provenance_in_domain(elem, curried_params, regular_params, context_params);
            }
        }
        // InRelational: subquery + value, recurse into both
        BooleanExpression::InRelational {
            value, subquery, ..
        } => {
            populate_provenance_in_domain(value, curried_params, regular_params, context_params);
            populate_provenance_in_relational(
                subquery,
                curried_params,
                regular_params,
                context_params,
            );
        }
        // Using, BooleanLiteral, Sigma, GlobCorrelation, OrdinalGlobCorrelation:
        // no domain expressions with possible CFE parameters
        BooleanExpression::Using { .. }
        | BooleanExpression::BooleanLiteral { .. }
        | BooleanExpression::Sigma { .. }
        | BooleanExpression::GlobCorrelation { .. }
        | BooleanExpression::OrdinalGlobCorrelation { .. } => {}
    }
}

fn populate_provenance_in_function(
    func: &mut refined::FunctionExpression,
    curried_params: &[String],
    regular_params: &[String],
    context_params: &[String],
) {
    use refined::FunctionExpression;

    match func {
        FunctionExpression::Regular { arguments, .. }
        | FunctionExpression::Curried { arguments, .. }
        | FunctionExpression::Bracket { arguments, .. } => {
            for arg in arguments {
                populate_provenance_in_domain(arg, curried_params, regular_params, context_params);
            }
        }
        FunctionExpression::Infix { left, right, .. } => {
            populate_provenance_in_domain(left, curried_params, regular_params, context_params);
            populate_provenance_in_domain(right, curried_params, regular_params, context_params);
        }
        FunctionExpression::Window {
            arguments,
            partition_by,
            ..
        } => {
            for arg in arguments {
                populate_provenance_in_domain(arg, curried_params, regular_params, context_params);
            }
            for p in partition_by {
                populate_provenance_in_domain(p, curried_params, regular_params, context_params);
            }
        }
        FunctionExpression::Lambda { body, .. } => {
            populate_provenance_in_domain(body, curried_params, regular_params, context_params);
        }
        FunctionExpression::HigherOrder {
            curried_arguments,
            regular_arguments,
            ..
        } => {
            for arg in curried_arguments {
                populate_provenance_in_domain(arg, curried_params, regular_params, context_params);
            }
            for arg in regular_arguments {
                populate_provenance_in_domain(arg, curried_params, regular_params, context_params);
            }
        }
        FunctionExpression::JsonPath { source, path, .. } => {
            populate_provenance_in_domain(source, curried_params, regular_params, context_params);
            populate_provenance_in_domain(path, curried_params, regular_params, context_params);
        }
        // CaseExpression, StringTemplate, Curly, MetadataTreeGroup, Array:
        // deep walk not critical for provenance fixup
        FunctionExpression::CaseExpression { .. }
        | FunctionExpression::StringTemplate { .. }
        | FunctionExpression::Curly { .. }
        | FunctionExpression::MetadataTreeGroup { .. }
        | FunctionExpression::Array { .. } => {}
    }
}
