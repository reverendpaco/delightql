use crate::pipeline::asts::core::expressions::boolean::BooleanExpression;
use crate::pipeline::asts::core::expressions::domain::DomainExpression;
use crate::pipeline::asts::core::expressions::functions::{CaseArm, FunctionExpression};
use crate::pipeline::asts::core::expressions::pipes::SigmaCondition;
use crate::pipeline::asts::core::{LiteralValue, Resolved};
use crate::pipeline::sql_ast_v3::BinaryOperator;
use crate::pipeline::sql_ast_v3::DomainExpression as SqlExpression;
use crate::pipeline::sql_ast_v3::WhenClause;
use crate::Result;

use super::asts::{ColumnDef, CreateTableDef, DdlConstraint, DdlDefault};
use super::sql_ast::{SqlColumnDef, SqlCreateTable, SqlDefaultClause, SqlTableConstraint};

/// Transform a resolved DDL AST into a SQL DDL AST.
pub fn transform(def: CreateTableDef<Resolved>) -> Result<SqlCreateTable> {
    let mut sql_columns = Vec::with_capacity(def.columns.len());
    let mut table_constraints: Vec<SqlTableConstraint> = Vec::new();

    for col in &def.columns {
        let (sql_col, extra_constraints) = transform_column(col)?;
        sql_columns.push(sql_col);
        table_constraints.extend(extra_constraints);
    }

    // Table-level constraints from the DDL AST
    for tc in &def.table_constraints {
        table_constraints.push(transform_table_constraint(tc)?);
    }

    Ok(SqlCreateTable {
        name: def.name,
        temp: def.temp,
        columns: sql_columns,
        table_constraints,
    })
}

/// Transform a single column definition, returning the SQL column def and any
/// constraints that need to be promoted to table-level (composite PK/UNIQUE, FK).
fn transform_column(col: &ColumnDef<Resolved>) -> Result<(SqlColumnDef, Vec<SqlTableConstraint>)> {
    let mut not_null = false;
    let mut primary_key = false;
    let mut unique = false;
    let mut check = None;
    let mut extra_constraints = Vec::new();

    for c in &col.constraints {
        match c {
            DdlConstraint::PrimaryKey { columns: None } => {
                primary_key = true;
            }
            DdlConstraint::PrimaryKey {
                columns: Some(cols),
            } => {
                extra_constraints.push(SqlTableConstraint::PrimaryKey {
                    _name: None,
                    columns: cols.clone(),
                });
            }
            DdlConstraint::Unique { columns: None } => {
                unique = true;
            }
            DdlConstraint::Unique {
                columns: Some(cols),
            } => {
                extra_constraints.push(SqlTableConstraint::Unique {
                    _name: None,
                    columns: cols.clone(),
                });
            }
            DdlConstraint::NotNull => {
                not_null = true;
            }
            DdlConstraint::Check { expr } => {
                check = Some(transform_ddl_expression(expr.clone(), &col.name)?);
            }
            DdlConstraint::ForeignKey { table, columns } => {
                extra_constraints.push(SqlTableConstraint::ForeignKey {
                    _name: None,
                    columns: vec![col.name.clone()],
                    ref_table: table.clone(),
                    ref_columns: columns.clone(),
                });
            }
        }
    }

    let default = col
        .default
        .as_ref()
        .map(|d| transform_default(d, &col.name))
        .transpose()?;

    Ok((
        SqlColumnDef {
            name: col.name.clone(),
            col_type: col.col_type.clone(),
            not_null,
            primary_key,
            unique,
            check,
            default,
        },
        extra_constraints,
    ))
}

fn transform_default(default: &DdlDefault<Resolved>, col_name: &str) -> Result<SqlDefaultClause> {
    match default {
        DdlDefault::Value { expr } => {
            let sql_expr = transform_ddl_expression(expr.clone(), col_name)?;
            Ok(SqlDefaultClause::Expression(sql_expr))
        }
        DdlDefault::Generated { expr, kind } => {
            let sql_expr = transform_ddl_expression(expr.clone(), col_name)?;
            Ok(SqlDefaultClause::Generated {
                expr: sql_expr,
                kind: kind.clone(),
            })
        }
    }
}

fn transform_table_constraint(c: &DdlConstraint<Resolved>) -> Result<SqlTableConstraint> {
    match c {
        DdlConstraint::PrimaryKey {
            columns: Some(cols),
        } => Ok(SqlTableConstraint::PrimaryKey {
            _name: None,
            columns: cols.clone(),
        }),
        DdlConstraint::PrimaryKey { columns: None } => Ok(SqlTableConstraint::PrimaryKey {
            _name: None,
            columns: vec![],
        }),
        DdlConstraint::Unique {
            columns: Some(cols),
        } => Ok(SqlTableConstraint::Unique {
            _name: None,
            columns: cols.clone(),
        }),
        DdlConstraint::Unique { columns: None } => Ok(SqlTableConstraint::Unique {
            _name: None,
            columns: vec![],
        }),
        DdlConstraint::Check { expr } => {
            let sql_expr = transform_ddl_expression(expr.clone(), "")?;
            Ok(SqlTableConstraint::Check {
                _name: None,
                expr: sql_expr,
            })
        }
        DdlConstraint::NotNull => {
            // Table-level NOT NULL doesn't make sense; shouldn't reach here
            Err(crate::DelightQLError::transpilation_error(
                "NotNull constraint at table level is invalid",
                "ddl_pipeline::transformer",
            ))
        }
        DdlConstraint::ForeignKey { table, columns } => Ok(SqlTableConstraint::ForeignKey {
            _name: None,
            columns: columns.clone(),
            ref_table: table.clone(),
            ref_columns: columns.clone(),
        }),
    }
}

/// Convert a resolved DDL domain expression into a SQL expression.
///
/// This handles the small subset of expressions that appear in DDL (CHECK/DEFAULT):
/// Lvar, Literal, ValuePlaceholder, Function(Regular), Function(Infix),
/// Predicate(Comparison), Parenthesized.
///
/// ValuePlaceholder (`@`) is substituted with the column name.
fn transform_ddl_expression(
    expr: DomainExpression<Resolved>,
    column_name: &str,
) -> Result<SqlExpression> {
    match expr {
        DomainExpression::Lvar { name, .. } => Ok(SqlExpression::Column {
            name: name.to_string(),
            qualifier: None,
        }),
        DomainExpression::Literal { value, .. } => Ok(SqlExpression::Literal(value)),
        DomainExpression::ValuePlaceholder { .. } => Ok(SqlExpression::Column {
            name: column_name.to_string(),
            qualifier: None,
        }),
        DomainExpression::Function(func) => transform_ddl_function(func, column_name),
        DomainExpression::Predicate { expr: pred, .. } => {
            transform_ddl_predicate(*pred, column_name)
        }
        DomainExpression::Parenthesized { inner, .. } => {
            let inner_sql = transform_ddl_expression(*inner, column_name)?;
            Ok(SqlExpression::Parens(Box::new(inner_sql)))
        }
        other => Err(crate::DelightQLError::transpilation_error(
            format!(
                "Unsupported DDL expression variant: {:?}",
                std::mem::discriminant(&other)
            ),
            "ddl_pipeline::transformer",
        )),
    }
}

fn transform_ddl_function(
    func: FunctionExpression<Resolved>,
    column_name: &str,
) -> Result<SqlExpression> {
    match func {
        FunctionExpression::Regular {
            name, arguments, ..
        } => {
            let args = arguments
                .into_iter()
                .map(|a| transform_ddl_expression(a, column_name))
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlExpression::Function {
                name: name.to_string(),
                args,
                distinct: false,
            })
        }
        FunctionExpression::Infix {
            operator,
            left,
            right,
            ..
        } => {
            let left_sql = transform_ddl_expression(*left, column_name)?;
            let right_sql = transform_ddl_expression(*right, column_name)?;
            let op = match operator.as_str() {
                "add" => BinaryOperator::Add,
                "subtract" => BinaryOperator::Subtract,
                "multiply" => BinaryOperator::Multiply,
                "divide" => BinaryOperator::Divide,
                "modulo" => BinaryOperator::Modulo,
                "concat" => BinaryOperator::Concatenate,
                other => {
                    return Err(crate::DelightQLError::transpilation_error(
                        format!("Unknown infix operator in DDL: {other}"),
                        "ddl_pipeline::transformer",
                    ))
                }
            };
            Ok(SqlExpression::Binary {
                left: Box::new(left_sql),
                op,
                right: Box::new(right_sql),
            })
        }
        FunctionExpression::CaseExpression { arms, .. } => {
            let mut when_clauses = Vec::new();
            let mut else_clause = None;
            let mut case_expr = None;

            for arm in arms {
                match arm {
                    CaseArm::Simple {
                        test_expr,
                        value,
                        result,
                    } => {
                        if case_expr.is_none() {
                            case_expr =
                                Some(Box::new(transform_ddl_expression(*test_expr, column_name)?));
                        }
                        when_clauses.push(WhenClause::new(
                            SqlExpression::Literal(value),
                            transform_ddl_expression(*result, column_name)?,
                        ));
                    }
                    CaseArm::Searched { condition, result } => {
                        when_clauses.push(WhenClause::new(
                            transform_ddl_predicate(*condition, column_name)?,
                            transform_ddl_expression(*result, column_name)?,
                        ));
                    }
                    CaseArm::Default { result } => {
                        else_clause =
                            Some(Box::new(transform_ddl_expression(*result, column_name)?));
                    }
                    CaseArm::CurriedSimple { .. } => {
                        return Err(crate::DelightQLError::transpilation_error(
                            "Curried CASE not supported in DDL context",
                            "ddl_pipeline::transformer",
                        ));
                    }
                }
            }

            Ok(SqlExpression::Case {
                expr: case_expr,
                when_clauses,
                else_clause,
            })
        }
        other => Err(crate::DelightQLError::transpilation_error(
            format!(
                "Unsupported DDL function variant: {:?}",
                std::mem::discriminant(&other)
            ),
            "ddl_pipeline::transformer",
        )),
    }
}

fn transform_ddl_predicate(
    pred: BooleanExpression<Resolved>,
    column_name: &str,
) -> Result<SqlExpression> {
    match pred {
        BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => {
            let left_sql = transform_ddl_expression(*left, column_name)?;
            let right_sql = transform_ddl_expression(*right, column_name)?;
            let op = match operator.as_str() {
                "traditional_eq" | "null_safe_eq" => BinaryOperator::Equal,
                "traditional_ne" => BinaryOperator::NotEqual,
                "null_safe_ne" => BinaryOperator::IsDistinctFrom,
                "less_than" => BinaryOperator::LessThan,
                "greater_than" => BinaryOperator::GreaterThan,
                "less_than_eq" => BinaryOperator::LessThanOrEqual,
                "greater_than_eq" => BinaryOperator::GreaterThanOrEqual,
                other => {
                    return Err(crate::DelightQLError::transpilation_error(
                        format!("Unknown comparison operator in DDL: {other}"),
                        "ddl_pipeline::transformer",
                    ))
                }
            };
            Ok(SqlExpression::Binary {
                left: Box::new(left_sql),
                op,
                right: Box::new(right_sql),
            })
        }
        BooleanExpression::And { left, right } => {
            let left_sql = transform_ddl_predicate(*left, column_name)?;
            let right_sql = transform_ddl_predicate(*right, column_name)?;
            Ok(SqlExpression::Binary {
                left: Box::new(left_sql),
                op: BinaryOperator::And,
                right: Box::new(right_sql),
            })
        }
        BooleanExpression::Or { left, right } => {
            let left_sql = transform_ddl_predicate(*left, column_name)?;
            let right_sql = transform_ddl_predicate(*right, column_name)?;
            Ok(SqlExpression::Binary {
                left: Box::new(left_sql),
                op: BinaryOperator::Or,
                right: Box::new(right_sql),
            })
        }
        BooleanExpression::Not { expr } => {
            let inner = transform_ddl_predicate(*expr, column_name)?;
            Ok(SqlExpression::Unary {
                op: crate::pipeline::sql_ast_v3::UnaryOperator::Not,
                expr: Box::new(inner),
            })
        }
        BooleanExpression::BooleanLiteral { value } => {
            if value {
                Ok(SqlExpression::Parens(Box::new(SqlExpression::Binary {
                    left: Box::new(SqlExpression::Literal(LiteralValue::Number("1".into()))),
                    op: BinaryOperator::Equal,
                    right: Box::new(SqlExpression::Literal(LiteralValue::Number("1".into()))),
                })))
            } else {
                Ok(SqlExpression::Parens(Box::new(SqlExpression::Binary {
                    left: Box::new(SqlExpression::Literal(LiteralValue::Number("1".into()))),
                    op: BinaryOperator::Equal,
                    right: Box::new(SqlExpression::Literal(LiteralValue::Number("0".into()))),
                })))
            }
        }
        BooleanExpression::In {
            value,
            set,
            negated,
        } => {
            let expr = transform_ddl_expression(*value, column_name)?;
            let values = set
                .into_iter()
                .map(|v| transform_ddl_expression(v, column_name))
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlExpression::InList {
                expr: Box::new(expr),
                not: negated,
                values,
            })
        }
        BooleanExpression::Sigma { condition } => match *condition {
            SigmaCondition::SigmaCall {
                functor,
                arguments,
                exists,
            } => match functor.as_str() {
                "like" => {
                    if arguments.len() != 2 {
                        return Err(crate::DelightQLError::transpilation_error(
                            format!(
                                "DDL LIKE requires exactly 2 arguments, got {}",
                                arguments.len()
                            ),
                            "ddl_pipeline::transformer",
                        ));
                    }
                    let mut args = arguments.into_iter();
                    let value = transform_ddl_expression(args.next().unwrap(), column_name)?;
                    let pattern = transform_ddl_expression(args.next().unwrap(), column_name)?;
                    let op = if exists {
                        BinaryOperator::Like
                    } else {
                        BinaryOperator::NotLike
                    };
                    Ok(SqlExpression::Binary {
                        left: Box::new(value),
                        op,
                        right: Box::new(pattern),
                    })
                }
                other => Err(crate::DelightQLError::transpilation_error(
                    format!("Unsupported sigma predicate in DDL: {other}"),
                    "ddl_pipeline::transformer",
                )),
            },
            other => Err(crate::DelightQLError::transpilation_error(
                format!(
                    "Unsupported sigma condition variant in DDL: {:?}",
                    std::mem::discriminant(&other)
                ),
                "ddl_pipeline::transformer",
            )),
        },
        other => Err(crate::DelightQLError::transpilation_error(
            format!(
                "Unsupported DDL boolean expression variant: {:?}",
                std::mem::discriminant(&other)
            ),
            "ddl_pipeline::transformer",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_pipeline::asts::{ColumnDef, CreateTableDef, DdlConstraint, DdlDefault};
    use crate::pipeline::asts::core::expressions::pipes::SigmaCondition;
    use crate::pipeline::asts::core::LiteralValue;

    fn lit_num(n: &str) -> DomainExpression<Resolved> {
        DomainExpression::Literal {
            value: LiteralValue::Number(n.to_string()),
            alias: None,
        }
    }

    fn lit_str(s: &str) -> DomainExpression<Resolved> {
        DomainExpression::Literal {
            value: LiteralValue::String(s.to_string()),
            alias: None,
        }
    }

    fn value_placeholder() -> DomainExpression<Resolved> {
        DomainExpression::ValuePlaceholder { alias: None }
    }

    fn simple_col(name: &str, col_type: &str) -> ColumnDef<Resolved> {
        ColumnDef {
            name: name.to_string(),
            col_type: col_type.to_string(),
            constraints: vec![],
            default: None,
        }
    }

    #[test]
    fn test_pk_none_to_primary_key_true() {
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "id".into(),
                col_type: "INTEGER".into(),
                constraints: vec![DdlConstraint::PrimaryKey { columns: None }],
                default: None,
            }],
            table_constraints: vec![],
        };
        let result = transform(def).unwrap();
        assert!(result.columns[0].primary_key);
    }

    #[test]
    fn test_pk_some_to_table_constraint() {
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![simple_col("a", "INTEGER"), simple_col("b", "TEXT")],
            table_constraints: vec![DdlConstraint::PrimaryKey {
                columns: Some(vec!["a".into(), "b".into()]),
            }],
        };
        let result = transform(def).unwrap();
        assert!(matches!(
            &result.table_constraints[0],
            SqlTableConstraint::PrimaryKey { columns, .. } if columns.len() == 2
        ));
    }

    #[test]
    fn test_not_null_to_flag() {
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "name".into(),
                col_type: "TEXT".into(),
                constraints: vec![DdlConstraint::NotNull],
                default: None,
            }],
            table_constraints: vec![],
        };
        let result = transform(def).unwrap();
        assert!(result.columns[0].not_null);
    }

    #[test]
    fn test_check_with_value_placeholder_substituted() {
        // @ > 0 → "age" > 0
        let check_expr = DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::Comparison {
                operator: "greater_than".to_string(),
                left: Box::new(value_placeholder()),
                right: Box::new(lit_num("0")),
            }),
            alias: None,
        };
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "age".into(),
                col_type: "INTEGER".into(),
                constraints: vec![DdlConstraint::Check { expr: check_expr }],
                default: None,
            }],
            table_constraints: vec![],
        };
        let result = transform(def).unwrap();
        let check = result.columns[0].check.as_ref().unwrap();
        // Should be Binary { Column("age"), GreaterThan, Literal(0) }
        match check {
            SqlExpression::Binary { left, op, right } => {
                assert!(
                    matches!(left.as_ref(), SqlExpression::Column { name, .. } if name == "age")
                );
                assert_eq!(*op, BinaryOperator::GreaterThan);
                assert!(
                    matches!(right.as_ref(), SqlExpression::Literal(LiteralValue::Number(n)) if n == "0")
                );
            }
            other => panic!("Expected Binary, got: {:?}", other),
        }
    }

    #[test]
    fn test_default_literal() {
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "count".into(),
                col_type: "INTEGER".into(),
                constraints: vec![],
                default: Some(DdlDefault::Value {
                    expr: lit_num("42"),
                }),
            }],
            table_constraints: vec![],
        };
        let result = transform(def).unwrap();
        assert!(matches!(
            &result.columns[0].default,
            Some(SqlDefaultClause::Expression(SqlExpression::Literal(LiteralValue::Number(n)))) if n == "42"
        ));
    }

    // === BooleanLiteral ===

    #[test]
    fn test_boolean_literal_true() {
        let expr = DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::BooleanLiteral { value: true }),
            alias: None,
        };
        let result = transform_ddl_expression(expr, "col").unwrap();
        // Should be (1 = 1)
        match result {
            SqlExpression::Parens(inner) => match *inner {
                SqlExpression::Binary { left, op, right } => {
                    assert_eq!(op, BinaryOperator::Equal);
                    assert!(
                        matches!(*left, SqlExpression::Literal(LiteralValue::Number(ref n)) if n == "1")
                    );
                    assert!(
                        matches!(*right, SqlExpression::Literal(LiteralValue::Number(ref n)) if n == "1")
                    );
                }
                other => panic!("Expected Binary inside Parens, got: {:?}", other),
            },
            other => panic!("Expected Parens, got: {:?}", other),
        }
    }

    #[test]
    fn test_boolean_literal_false() {
        let expr = DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::BooleanLiteral { value: false }),
            alias: None,
        };
        let result = transform_ddl_expression(expr, "col").unwrap();
        // Should be (1 = 0)
        match result {
            SqlExpression::Parens(inner) => match *inner {
                SqlExpression::Binary { left, op, right } => {
                    assert_eq!(op, BinaryOperator::Equal);
                    assert!(
                        matches!(*left, SqlExpression::Literal(LiteralValue::Number(ref n)) if n == "1")
                    );
                    assert!(
                        matches!(*right, SqlExpression::Literal(LiteralValue::Number(ref n)) if n == "0")
                    );
                }
                other => panic!("Expected Binary inside Parens, got: {:?}", other),
            },
            other => panic!("Expected Parens, got: {:?}", other),
        }
    }

    // === IN list ===

    #[test]
    fn test_in_list() {
        let expr = DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::In {
                value: Box::new(value_placeholder()),
                set: vec![lit_num("1"), lit_num("2"), lit_num("3")],
                negated: false,
            }),
            alias: None,
        };
        let result = transform_ddl_expression(expr, "status").unwrap();
        match result {
            SqlExpression::InList {
                expr, not, values, ..
            } => {
                assert!(!not);
                assert!(
                    matches!(*expr, SqlExpression::Column { ref name, .. } if name == "status")
                );
                assert_eq!(values.len(), 3);
            }
            other => panic!("Expected InList, got: {:?}", other),
        }
    }

    #[test]
    fn test_not_in_list() {
        let expr = DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::In {
                value: Box::new(value_placeholder()),
                set: vec![lit_num("1"), lit_num("2")],
                negated: true,
            }),
            alias: None,
        };
        let result = transform_ddl_expression(expr, "status").unwrap();
        match result {
            SqlExpression::InList { not, .. } => assert!(not),
            other => panic!("Expected InList, got: {:?}", other),
        }
    }

    // === Sigma / LIKE ===

    #[test]
    fn test_like_sigma() {
        let expr = DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::Sigma {
                condition: Box::new(SigmaCondition::SigmaCall {
                    functor: "like".to_string(),
                    arguments: vec![value_placeholder(), lit_str("%abc")],
                    exists: true,
                }),
            }),
            alias: None,
        };
        let result = transform_ddl_expression(expr, "name").unwrap();
        match result {
            SqlExpression::Binary { left, op, right } => {
                assert_eq!(op, BinaryOperator::Like);
                assert!(matches!(*left, SqlExpression::Column { ref name, .. } if name == "name"));
                assert!(
                    matches!(*right, SqlExpression::Literal(LiteralValue::String(ref s)) if s == "%abc")
                );
            }
            other => panic!("Expected Binary LIKE, got: {:?}", other),
        }
    }

    #[test]
    fn test_not_like_sigma() {
        let expr = DomainExpression::Predicate {
            expr: Box::new(BooleanExpression::Sigma {
                condition: Box::new(SigmaCondition::SigmaCall {
                    functor: "like".to_string(),
                    arguments: vec![value_placeholder(), lit_str("%test%")],
                    exists: false,
                }),
            }),
            alias: None,
        };
        let result = transform_ddl_expression(expr, "name").unwrap();
        match result {
            SqlExpression::Binary { op, .. } => assert_eq!(op, BinaryOperator::NotLike),
            other => panic!("Expected Binary NotLike, got: {:?}", other),
        }
    }

    // === CaseExpression ===

    #[test]
    fn test_case_searched() {
        use crate::pipeline::asts::core::expressions::functions::CaseArm;
        let case_expr = DomainExpression::Function(FunctionExpression::CaseExpression {
            arms: vec![
                CaseArm::Searched {
                    condition: Box::new(BooleanExpression::Comparison {
                        operator: "greater_than".to_string(),
                        left: Box::new(value_placeholder()),
                        right: Box::new(lit_num("0")),
                    }),
                    result: Box::new(lit_str("positive")),
                },
                CaseArm::Default {
                    result: Box::new(lit_str("non-positive")),
                },
            ],
            alias: None,
        });
        let result = transform_ddl_expression(case_expr, "val").unwrap();
        match result {
            SqlExpression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                assert!(expr.is_none());
                assert_eq!(when_clauses.len(), 1);
                assert!(else_clause.is_some());
            }
            other => panic!("Expected Case, got: {:?}", other),
        }
    }

    #[test]
    fn test_case_simple() {
        use crate::pipeline::asts::core::expressions::functions::CaseArm;
        let case_expr = DomainExpression::Function(FunctionExpression::CaseExpression {
            arms: vec![
                CaseArm::Simple {
                    test_expr: Box::new(value_placeholder()),
                    value: LiteralValue::Number("1".to_string()),
                    result: Box::new(lit_str("one")),
                },
                CaseArm::Simple {
                    test_expr: Box::new(value_placeholder()),
                    value: LiteralValue::Number("2".to_string()),
                    result: Box::new(lit_str("two")),
                },
                CaseArm::Default {
                    result: Box::new(lit_str("other")),
                },
            ],
            alias: None,
        });
        let result = transform_ddl_expression(case_expr, "code").unwrap();
        match result {
            SqlExpression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                assert!(expr.is_some());
                assert_eq!(when_clauses.len(), 2);
                assert!(else_clause.is_some());
            }
            other => panic!("Expected Case, got: {:?}", other),
        }
    }
}
