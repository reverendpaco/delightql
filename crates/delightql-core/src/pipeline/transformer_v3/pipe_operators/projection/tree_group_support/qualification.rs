// Column qualification helpers for tree group expressions after JOINs

use crate::pipeline::asts::addressed as ast_addressed;
use std::collections::HashSet;

/// Qualify an expression's column references with a table name
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn qualify_expression_with_table(
    expr: ast_addressed::DomainExpression,
    table_name: &str,
) -> ast_addressed::DomainExpression {
    match expr {
        ast_addressed::DomainExpression::Lvar {
            name,
            namespace_path,
            alias,
            ..
        } => ast_addressed::DomainExpression::Lvar {
            name,
            qualifier: Some(table_name.into()),
            namespace_path,
            alias,
            provenance: ast_addressed::PhaseBox::phantom(),
        },
        _ => expr,
    }
}

/// Recursively qualify all base table column references in an expression
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn qualify_base_table_references(
    expr: ast_addressed::DomainExpression,
    table_name: &str,
) -> ast_addressed::DomainExpression {
    qualify_base_table_references_inner(expr, table_name, &std::collections::HashSet::new())
}

/// Helper with CTE name tracking
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn qualify_base_table_references_inner(
    expr: ast_addressed::DomainExpression,
    table_name: &str,
    cte_names: &std::collections::HashSet<String>,
) -> ast_addressed::DomainExpression {
    match expr {
        ast_addressed::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: _,
        } => {
            let preserve_qualifier = qualifier
                .as_ref()
                .map(|q| cte_names.contains(q.as_str()))
                .unwrap_or(false);
            if preserve_qualifier {
                ast_addressed::DomainExpression::Lvar {
                    name,
                    qualifier,
                    namespace_path,
                    alias,
                    provenance: ast_addressed::PhaseBox::phantom(),
                }
            } else {
                ast_addressed::DomainExpression::Lvar {
                    name,
                    qualifier: Some(table_name.into()),
                    namespace_path,
                    alias,
                    provenance: ast_addressed::PhaseBox::phantom(),
                }
            }
        }
        ast_addressed::DomainExpression::Function(func) => {
            ast_addressed::DomainExpression::Function(qualify_function_expression_inner(
                func, table_name, cte_names,
            ))
        }
        ast_addressed::DomainExpression::Tuple { elements, alias } => {
            ast_addressed::DomainExpression::Tuple {
                elements: elements
                    .into_iter()
                    .map(|elem| qualify_base_table_references_inner(elem, table_name, cte_names))
                    .collect(),
                alias,
            }
        }
        ast_addressed::DomainExpression::Parenthesized { inner, alias } => {
            ast_addressed::DomainExpression::Parenthesized {
                inner: Box::new(qualify_base_table_references_inner(
                    *inner, table_name, cte_names,
                )),
                alias,
            }
        }
        _ => expr,
    }
}

fn qualify_function_expression_inner(
    func: ast_addressed::FunctionExpression,
    table_name: &str,
    cte_names: &std::collections::HashSet<String>,
) -> ast_addressed::FunctionExpression {
    match func {
        ast_addressed::FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => ast_addressed::FunctionExpression::Regular {
            name,
            namespace,
            arguments: arguments
                .into_iter()
                .map(|arg| qualify_base_table_references_inner(arg, table_name, cte_names))
                .collect(),
            alias,
            conditioned_on,
        },
        ast_addressed::FunctionExpression::Curly {
            members,
            inner_grouping_keys,
            cte_requirements,
            alias,
        } => ast_addressed::FunctionExpression::Curly {
            members: members
                .into_iter()
                .map(|member| qualify_curly_member_inner(member, table_name, cte_names))
                .collect(),
            inner_grouping_keys,
            cte_requirements,
            alias,
        },
        ast_addressed::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            keys_only,
            cte_requirements,
            alias,
        } => {
            let preserve_key_qualifier = key_qualifier
                .as_ref()
                .map(|q| cte_names.contains(q.as_str()))
                .unwrap_or(false);
            ast_addressed::FunctionExpression::MetadataTreeGroup {
                key_column,
                key_qualifier: if preserve_key_qualifier {
                    key_qualifier
                } else {
                    Some(table_name.into())
                },
                key_schema,
                constructor: Box::new(qualify_function_expression_inner(
                    *constructor,
                    table_name,
                    cte_names,
                )),
                keys_only,
                cte_requirements,
                alias,
            }
        }
        _ => func,
    }
}

fn qualify_curly_member_inner(
    member: ast_addressed::CurlyMember,
    table_name: &str,
    cte_names: &std::collections::HashSet<String>,
) -> ast_addressed::CurlyMember {
    match member {
        ast_addressed::CurlyMember::Shorthand {
            column,
            qualifier,
            schema,
        } => {
            let preserve_qualifier = qualifier
                .as_ref()
                .map(|q| cte_names.contains(q.as_str()))
                .unwrap_or(false);
            ast_addressed::CurlyMember::Shorthand {
                column,
                qualifier: if preserve_qualifier {
                    qualifier
                } else {
                    Some(table_name.into())
                },
                schema,
            }
        }
        ast_addressed::CurlyMember::KeyValue {
            key,
            nested_reduction,
            value,
        } => ast_addressed::CurlyMember::KeyValue {
            key,
            nested_reduction,
            value: Box::new(qualify_base_table_references_inner(
                *value, table_name, cte_names,
            )),
        },
        // PATH FIRST-CLASS: Epoch 5 - PathLiteral handling
        ast_addressed::CurlyMember::PathLiteral { path, alias } => {
            ast_addressed::CurlyMember::PathLiteral {
                path: Box::new(qualify_base_table_references_inner(
                    *path, table_name, cte_names,
                )),
                alias,
            }
        }
        ast_addressed::CurlyMember::Comparison { .. } => member,
        // TG-ERGONOMIC-INDUCTOR: These should have been expanded by resolver
        ast_addressed::CurlyMember::Glob
        | ast_addressed::CurlyMember::Pattern { .. }
        | ast_addressed::CurlyMember::OrdinalRange { .. } => {
            panic!(
                "Glob/Pattern/OrdinalRange in curly member should have been expanded by resolver"
            )
        }
        // Placeholder is only valid in destructuring, pass through unchanged
        ast_addressed::CurlyMember::Placeholder => member,
    }
}
/// Qualify SQL AST column references with base table name
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn qualify_sql_expression(
    expr: crate::pipeline::sql_ast_v3::DomainExpression,
    table_name: &str,
    cte_names: &std::collections::HashSet<String>,
) -> crate::pipeline::sql_ast_v3::DomainExpression {
    use crate::pipeline::sql_ast_v3::DomainExpression;
    use crate::pipeline::transformer_v3::QualifierScope;

    match expr {
        DomainExpression::Column {
            name, qualifier, ..
        } => {
            let preserve_qualifier = qualifier
                .as_ref()
                .map(|q| cte_names.iter().any(|c| c == q.table_name()))
                .unwrap_or(false);

            if preserve_qualifier {
                DomainExpression::Column { name, qualifier }
            } else {
                DomainExpression::Column {
                    name,
                    qualifier: Some(QualifierScope::structural(table_name)),
                }
            }
        }
        DomainExpression::Binary { left, op, right } => DomainExpression::Binary {
            left: Box::new(qualify_sql_expression(*left, table_name, cte_names)),
            op,
            right: Box::new(qualify_sql_expression(*right, table_name, cte_names)),
        },
        DomainExpression::Unary { op, expr } => DomainExpression::Unary {
            op,
            expr: Box::new(qualify_sql_expression(*expr, table_name, cte_names)),
        },
        DomainExpression::Function {
            name,
            args,
            distinct,
        } => DomainExpression::Function {
            name,
            args: args
                .into_iter()
                .map(|arg| qualify_sql_expression(arg, table_name, cte_names))
                .collect(),
            distinct,
        },
        DomainExpression::Parens(inner) => DomainExpression::Parens(Box::new(
            qualify_sql_expression(*inner, table_name, cte_names),
        )),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => DomainExpression::Case {
            expr: expr.map(|e| Box::new(qualify_sql_expression(*e, table_name, cte_names))),
            when_clauses: when_clauses
                .into_iter()
                .map(|wc| {
                    crate::pipeline::sql_ast_v3::WhenClause::new(
                        qualify_sql_expression(wc.when().clone(), table_name, cte_names),
                        qualify_sql_expression(wc.then().clone(), table_name, cte_names),
                    )
                })
                .collect(),
            else_clause: else_clause
                .map(|e| Box::new(qualify_sql_expression(*e, table_name, cte_names))),
        },
        DomainExpression::InList { expr, not, values } => DomainExpression::InList {
            expr: Box::new(qualify_sql_expression(*expr, table_name, cte_names)),
            not,
            values: values
                .into_iter()
                .map(|v| qualify_sql_expression(v, table_name, cte_names))
                .collect(),
        },
        DomainExpression::WindowFunction {
            name,
            args,
            partition_by,
            order_by,
            frame,
        } => DomainExpression::WindowFunction {
            name,
            args: args
                .into_iter()
                .map(|arg| qualify_sql_expression(arg, table_name, cte_names))
                .collect(),
            partition_by: partition_by
                .into_iter()
                .map(|p| qualify_sql_expression(p, table_name, cte_names))
                .collect(),
            order_by: order_by
                .into_iter()
                .map(|(e, dir)| (qualify_sql_expression(e, table_name, cte_names), dir))
                .collect(),
            frame: frame.map(|f| qualify_window_frame(f, table_name, cte_names)),
        },
        DomainExpression::InSubquery {
            expr: e,
            not,
            query,
        } => DomainExpression::InSubquery {
            expr: Box::new(qualify_sql_expression(*e, table_name, cte_names)),
            not,
            query,
        },
        DomainExpression::Literal(_)
        | DomainExpression::Star
        | DomainExpression::Exists { .. }
        | DomainExpression::Subquery(_)
        | DomainExpression::RawSql(_) => expr,
    }
}

/// Qualify window frame expressions
fn qualify_window_frame(
    frame: crate::pipeline::sql_ast_v3::SqlWindowFrame,
    table_name: &str,
    cte_names: &HashSet<String>,
) -> crate::pipeline::sql_ast_v3::SqlWindowFrame {
    use crate::pipeline::sql_ast_v3::{SqlFrameBound, SqlWindowFrame};

    let start = match frame.start {
        SqlFrameBound::Unbounded => SqlFrameBound::Unbounded,
        SqlFrameBound::CurrentRow => SqlFrameBound::CurrentRow,
        SqlFrameBound::Preceding(expr) => SqlFrameBound::Preceding(Box::new(
            qualify_sql_expression(*expr, table_name, cte_names),
        )),
        SqlFrameBound::Following(expr) => SqlFrameBound::Following(Box::new(
            qualify_sql_expression(*expr, table_name, cte_names),
        )),
    };

    let end = match frame.end {
        SqlFrameBound::Unbounded => SqlFrameBound::Unbounded,
        SqlFrameBound::CurrentRow => SqlFrameBound::CurrentRow,
        SqlFrameBound::Preceding(expr) => SqlFrameBound::Preceding(Box::new(
            qualify_sql_expression(*expr, table_name, cte_names),
        )),
        SqlFrameBound::Following(expr) => SqlFrameBound::Following(Box::new(
            qualify_sql_expression(*expr, table_name, cte_names),
        )),
    };

    SqlWindowFrame {
        mode: frame.mode,
        start,
        end,
    }
}
