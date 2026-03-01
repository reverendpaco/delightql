use crate::error::Result;
use crate::pipeline::asts::core::phase_box::{PhaseBox, PhaseBoxable};
use crate::pipeline::{ast_addressed, ast_refined};
use delightql_types::SqlIdentifier;

pub fn address_query(query: ast_refined::Query) -> Result<ast_addressed::Query> {
    let mut addressed = address_query_inner(query)?;
    assign_tree_group_cte_names(&mut addressed);
    Ok(addressed)
}

fn address_query_inner(query: ast_refined::Query) -> Result<ast_addressed::Query> {
    match query {
        ast_refined::Query::WithCtes { ctes, query } => {
            let addressed_ctes = ctes
                .into_iter()
                .map(|cte| {
                    let is_recursive = expression_references_name(&cte.expression, &cte.name);
                    let addressed_expr: ast_addressed::RelationalExpression = cte.expression.into();
                    ast_addressed::CteBinding {
                        expression: addressed_expr,
                        name: cte.name,
                        is_recursive: is_recursive.new(),
                    }
                })
                .collect();
            Ok(ast_addressed::Query::WithCtes {
                ctes: addressed_ctes,
                query: query.into(),
            })
        }
        ast_refined::Query::WithPrecompiledCfes { cfes, query } => {
            Ok(ast_addressed::Query::WithPrecompiledCfes {
                cfes,
                query: Box::new(address_query_inner(*query)?),
            })
        }
        ast_refined::Query::ReplTempTable { query, table_name } => {
            Ok(ast_addressed::Query::ReplTempTable {
                query: Box::new(address_query_inner(*query)?),
                table_name,
            })
        }
        ast_refined::Query::ReplTempView { query, view_name } => {
            Ok(ast_addressed::Query::ReplTempView {
                query: Box::new(address_query_inner(*query)?),
                view_name,
            })
        }
        // Plain relational query — no CTEs, just convert phase.
        // Tree group CTE names are assigned by the caller (assign_tree_group_cte_names).
        ast_refined::Query::Relational(expr) => Ok(ast_addressed::Query::Relational(expr.into())),
        // These are consumed before the refined phase and should never reach the addresser.
        ast_refined::Query::WithCfes { .. } | ast_refined::Query::WithErContext { .. } => {
            unreachable!("WithCfes/WithErContext should be consumed before addressing")
        }
    }
}

// ---------------------------------------------------------------------------
// DQL AST walk: does an expression reference a name via Ground relations?
// ---------------------------------------------------------------------------

fn expression_references_name(expr: &ast_refined::RelationalExpression, name: &str) -> bool {
    match expr {
        ast_refined::RelationalExpression::Relation(rel) => relation_references_name(rel, name),
        ast_refined::RelationalExpression::Join { left, right, .. } => {
            expression_references_name(left, name) || expression_references_name(right, name)
        }
        ast_refined::RelationalExpression::Filter { source, .. } => {
            expression_references_name(source, name)
        }
        ast_refined::RelationalExpression::Pipe(pipe) => {
            expression_references_name(&pipe.source, name)
        }
        ast_refined::RelationalExpression::SetOperation { operands, .. } => operands
            .iter()
            .any(|op| expression_references_name(op, name)),
        ast_refined::RelationalExpression::ErJoinChain { .. }
        | ast_refined::RelationalExpression::ErTransitiveJoin { .. } => false,
    }
}

fn relation_references_name(rel: &ast_refined::Relation, name: &str) -> bool {
    match rel {
        ast_refined::Relation::Ground { identifier, .. } => {
            identifier.name == SqlIdentifier::new(name)
        }
        ast_refined::Relation::ConsultedView { body, .. } => query_references_name(body, name),
        ast_refined::Relation::InnerRelation { pattern, .. } => {
            inner_pattern_references_name(pattern, name)
        }
        ast_refined::Relation::Anonymous { .. }
        | ast_refined::Relation::TVF { .. }
        | ast_refined::Relation::PseudoPredicate { .. } => false,
    }
}

fn query_references_name(query: &ast_refined::Query, name: &str) -> bool {
    match query {
        ast_refined::Query::Relational(expr) => expression_references_name(expr, name),
        ast_refined::Query::WithCtes { ctes, query } => {
            ctes.iter()
                .any(|cte| expression_references_name(&cte.expression, name))
                || expression_references_name(query, name)
        }
        ast_refined::Query::WithPrecompiledCfes { query, .. } => query_references_name(query, name),
        ast_refined::Query::WithCfes { query, .. } => query_references_name(query, name),
        ast_refined::Query::ReplTempTable { query, .. } => query_references_name(query, name),
        ast_refined::Query::ReplTempView { query, .. } => query_references_name(query, name),
        ast_refined::Query::WithErContext { query, .. } => query_references_name(query, name),
    }
}

fn inner_pattern_references_name(pattern: &ast_refined::InnerRelationPattern, name: &str) -> bool {
    match pattern {
        ast_refined::InnerRelationPattern::Indeterminate { subquery, .. }
        | ast_refined::InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. }
        | ast_refined::InnerRelationPattern::CorrelatedScalarJoin { subquery, .. }
        | ast_refined::InnerRelationPattern::CorrelatedGroupJoin { subquery, .. }
        | ast_refined::InnerRelationPattern::CorrelatedWindowJoin { subquery, .. } => {
            expression_references_name(subquery, name)
        }
    }
}

// ---------------------------------------------------------------------------
// Mutable walk: assign CTE names to tree groups (depth-first)
// ---------------------------------------------------------------------------

fn assign_tree_group_cte_names(query: &mut ast_addressed::Query) {
    let mut counter = 0;
    walk_query_for_tree_groups(query, &mut counter);
}

fn walk_query_for_tree_groups(query: &mut ast_addressed::Query, counter: &mut usize) {
    match query {
        ast_addressed::Query::Relational(expr) => {
            walk_relational_for_tree_groups(expr, counter);
        }
        ast_addressed::Query::WithCtes { ctes, query } => {
            for cte in ctes.iter_mut() {
                walk_relational_for_tree_groups(&mut cte.expression, counter);
            }
            walk_relational_for_tree_groups(query, counter);
        }
        ast_addressed::Query::WithPrecompiledCfes { query, .. } => {
            walk_query_for_tree_groups(query, counter);
        }
        ast_addressed::Query::WithCfes { query, .. } => {
            walk_query_for_tree_groups(query, counter);
        }
        ast_addressed::Query::ReplTempTable { query, .. } => {
            walk_query_for_tree_groups(query, counter);
        }
        ast_addressed::Query::ReplTempView { query, .. } => {
            walk_query_for_tree_groups(query, counter);
        }
        ast_addressed::Query::WithErContext { query, .. } => {
            walk_query_for_tree_groups(query, counter);
        }
    }
}

#[stacksafe::stacksafe]
fn walk_relational_for_tree_groups(
    expr: &mut ast_addressed::RelationalExpression,
    counter: &mut usize,
) {
    match expr {
        ast_addressed::RelationalExpression::Relation(_) => {
            // ConsultedView bodies bypass the addresser (known gap from chunk 1).
            // The Option<String> fallback in the transformer handles this.
        }
        ast_addressed::RelationalExpression::Join { left, right, .. } => {
            walk_relational_for_tree_groups(left, counter);
            walk_relational_for_tree_groups(right, counter);
        }
        ast_addressed::RelationalExpression::Filter { source, .. } => {
            walk_relational_for_tree_groups(source, counter);
        }
        ast_addressed::RelationalExpression::Pipe(_) => {
            // Linearize: walk the pipe chain iteratively instead of recursing
            let mut current = expr;
            while let ast_addressed::RelationalExpression::Pipe(pipe) = current {
                walk_operator_for_tree_groups(&mut pipe.operator, counter);
                current = &mut pipe.source;
            }
            walk_relational_for_tree_groups(current, counter);
        }
        ast_addressed::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands.iter_mut() {
                walk_relational_for_tree_groups(operand, counter);
            }
        }
        ast_addressed::RelationalExpression::ErJoinChain { .. }
        | ast_addressed::RelationalExpression::ErTransitiveJoin { .. } => {}
    }
}

fn walk_operator_for_tree_groups(
    op: &mut ast_addressed::UnaryRelationalOperator,
    counter: &mut usize,
) {
    match op {
        ast_addressed::UnaryRelationalOperator::Modulo { spec, .. } => {
            if let ast_addressed::ModuloSpec::GroupBy {
                reducing_by,
                reducing_on,
                ..
            } = spec
            {
                for expr in reducing_by.iter_mut() {
                    walk_domain_for_tree_groups(expr, counter);
                }
                for expr in reducing_on.iter_mut() {
                    walk_domain_for_tree_groups(expr, counter);
                }
            }
        }
        ast_addressed::UnaryRelationalOperator::General { expressions, .. } => {
            for expr in expressions.iter_mut() {
                walk_domain_for_tree_groups(expr, counter);
            }
        }
        ast_addressed::UnaryRelationalOperator::AggregatePipe { aggregations } => {
            for expr in aggregations.iter_mut() {
                walk_domain_for_tree_groups(expr, counter);
            }
        }
        // Operators with expressions but no tree groups in practice:
        // MapCover/EmbedMapCover: function is a regular fn (trim:(), etc), not a curly tree group
        // ProjectOut: column names to exclude — no expressions that could hold tree groups
        // RenameCover: old→new rename specs — no complex expressions
        // TupleOrdering: column refs for ORDER BY
        // Reposition: positional specs
        // Transform: expressions that could theoretically hold tree groups, but
        //   transform covers ($$) use simple column→expression mappings
        ast_addressed::UnaryRelationalOperator::MapCover { .. }
        | ast_addressed::UnaryRelationalOperator::EmbedMapCover { .. }
        | ast_addressed::UnaryRelationalOperator::ProjectOut { .. }
        | ast_addressed::UnaryRelationalOperator::RenameCover { .. }
        | ast_addressed::UnaryRelationalOperator::TupleOrdering { .. }
        | ast_addressed::UnaryRelationalOperator::Reposition { .. }
        | ast_addressed::UnaryRelationalOperator::Transform { .. } => {}
        // Operators with no user expressions at all:
        ast_addressed::UnaryRelationalOperator::MetaIze { .. }
        | ast_addressed::UnaryRelationalOperator::Qualify
        | ast_addressed::UnaryRelationalOperator::Using { .. }
        | ast_addressed::UnaryRelationalOperator::DmlTerminal { .. }
        | ast_addressed::UnaryRelationalOperator::InteriorDrillDown { .. }
        | ast_addressed::UnaryRelationalOperator::NarrowingDestructure { .. } => {}
        // Consumed before refined phase:
        ast_addressed::UnaryRelationalOperator::HoViewApplication { .. }
        | ast_addressed::UnaryRelationalOperator::DirectiveTerminal { .. }
        | ast_addressed::UnaryRelationalOperator::CompanionAccess { .. } => {
            unreachable!(
                "HoViewApplication/DirectiveTerminal/CompanionAccess consumed before addressing"
            )
        }
    }
}

fn walk_domain_for_tree_groups(expr: &mut ast_addressed::DomainExpression, counter: &mut usize) {
    match expr {
        ast_addressed::DomainExpression::Function(func) => {
            walk_function_for_tree_groups(func, counter);
        }
        ast_addressed::DomainExpression::PipedExpression { transforms, .. } => {
            for transform in transforms.iter_mut() {
                walk_function_for_tree_groups(transform, counter);
            }
        }
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            walk_domain_for_tree_groups(inner, counter);
        }
        // Leaf domain expressions: no nested function expressions, no tree groups possible.
        ast_addressed::DomainExpression::Lvar { .. }
        | ast_addressed::DomainExpression::Literal { .. }
        | ast_addressed::DomainExpression::Projection(_)
        | ast_addressed::DomainExpression::NonUnifiyingUnderscore
        | ast_addressed::DomainExpression::ValuePlaceholder { .. }
        | ast_addressed::DomainExpression::Substitution(_)
        | ast_addressed::DomainExpression::ColumnOrdinal(_)
        | ast_addressed::DomainExpression::PivotOf { .. } => {}
        // Predicate: contains BooleanExpression. Tree groups in boolean context are
        // not a supported pattern, so no walk needed.
        ast_addressed::DomainExpression::Predicate { .. } => {}
        // Tuple: contains sub-expressions, but tuple elements are simple values
        // (used for multi-column IN). No tree groups.
        ast_addressed::DomainExpression::Tuple { .. } => {}
        // ScalarSubquery: contains a RelationalExpression that CAN contain tree groups.
        // e.g., orders:(, user_id = id ~> {total, date}) — the {total, date} is a tree group.
        // A scalar subquery returning a JSON object via tree group is valid DQL.
        ast_addressed::DomainExpression::ScalarSubquery { subquery, .. } => {
            walk_relational_for_tree_groups(subquery, counter);
        }
    }
}

fn walk_function_for_tree_groups(
    func: &mut ast_addressed::FunctionExpression,
    counter: &mut usize,
) {
    match func {
        ast_addressed::FunctionExpression::Curly {
            members,
            cte_requirements,
            ..
        } => {
            // Depth-first: walk nested members (inner tree groups) FIRST
            for member in members.iter_mut() {
                if let ast_addressed::CurlyMember::KeyValue { value, .. } = member {
                    walk_domain_for_tree_groups(value, counter);
                }
            }
            // Then assign name to THIS level (if it has cte_requirements)
            if let Some(req) = cte_requirements {
                let name = format!("_tg_{}", counter);
                *counter += 1;
                req.cte_name = PhaseBox::from_cte_name(Some(name));
            }
        }
        ast_addressed::FunctionExpression::MetadataTreeGroup {
            constructor,
            cte_requirements,
            ..
        } => {
            // Depth-first: walk the constructor chain FIRST
            walk_function_for_tree_groups(constructor, counter);
            // Then assign name to THIS level
            if let Some(req) = cte_requirements {
                let name = format!("_tg_{}", counter);
                *counter += 1;
                req.cte_name = PhaseBox::from_cte_name(Some(name));
            }
        }
        ast_addressed::FunctionExpression::Regular { arguments, .. } => {
            for arg in arguments.iter_mut() {
                walk_domain_for_tree_groups(arg, counter);
            }
        }
        ast_addressed::FunctionExpression::Bracket { arguments, .. } => {
            for arg in arguments.iter_mut() {
                walk_domain_for_tree_groups(arg, counter);
            }
        }
        // Function variants whose arguments could contain tree groups:
        ast_addressed::FunctionExpression::Curried { arguments, .. } => {
            for arg in arguments.iter_mut() {
                walk_domain_for_tree_groups(arg, counter);
            }
        }
        ast_addressed::FunctionExpression::HigherOrder {
            curried_arguments,
            regular_arguments,
            ..
        } => {
            for arg in curried_arguments.iter_mut() {
                walk_domain_for_tree_groups(arg, counter);
            }
            for arg in regular_arguments.iter_mut() {
                walk_domain_for_tree_groups(arg, counter);
            }
        }
        ast_addressed::FunctionExpression::Window { arguments, .. } => {
            for arg in arguments.iter_mut() {
                walk_domain_for_tree_groups(arg, counter);
            }
        }
        ast_addressed::FunctionExpression::CaseExpression { arms, .. } => {
            for arm in arms.iter_mut() {
                match arm {
                    ast_addressed::CaseArm::Simple { result, .. }
                    | ast_addressed::CaseArm::CurriedSimple { result, .. }
                    | ast_addressed::CaseArm::Searched { result, .. }
                    | ast_addressed::CaseArm::Default { result } => {
                        walk_domain_for_tree_groups(result, counter);
                    }
                }
            }
        }
        ast_addressed::FunctionExpression::Lambda { body, .. } => {
            walk_domain_for_tree_groups(body, counter);
        }
        ast_addressed::FunctionExpression::Infix { left, right, .. } => {
            walk_domain_for_tree_groups(left, counter);
            walk_domain_for_tree_groups(right, counter);
        }
        // Leaf-like: no sub-expressions that could hold tree groups
        ast_addressed::FunctionExpression::StringTemplate { .. }
        | ast_addressed::FunctionExpression::Array { .. }
        | ast_addressed::FunctionExpression::JsonPath { .. } => {}
    }
}
