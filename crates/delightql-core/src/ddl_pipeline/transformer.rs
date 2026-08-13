// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::asts::core::expressions::domain::DomainExpression;
use crate::pipeline::asts::core::expressions::functions::FunctionApplication;
use crate::pipeline::asts::core::expressions::truth::TruthExpression;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Resolved;
#[cfg(test)]
use crate::pipeline::asts::vocabulary::Vec1;
use crate::pipeline::sql_ast::BinaryOperator;
use crate::pipeline::sql_ast::DomainExpression as SqlExpression;
use crate::pipeline::sql_ast::WhenClause;
use crate::Result;

use super::asts::{ColumnDef, CreateTableDef, DdlConstraint, DdlDefault};
use super::sql_ast::{SqlColumnDef, SqlCreateTable, SqlDefaultClause, SqlTableConstraint};
use crate::pipeline::asts::core::{Comparison, Membership, SigmaApplication};
use crate::pipeline::asts::core::{NamedReference, Reference};
#[cfg(test)]
use crate::pipeline::asts::core::{Polarity, Probe, ValueRow};

/// Transform a resolved DDL AST into a SQL DDL AST.
pub fn transform(
    def: CreateTableDef<Resolved>,
    identities: &crate::names::Registry,
) -> Result<SqlCreateTable> {
    let mut sql_columns = Vec::with_capacity(def.columns.len());
    let mut table_constraints: Vec<SqlTableConstraint> = Vec::new();

    for col in &def.columns {
        let (sql_col, extra_constraints) = transform_column(col, identities)?;
        sql_columns.push(sql_col);
        table_constraints.extend(extra_constraints);
    }

    // Table-level constraints from the DDL AST
    for tc in &def.table_constraints {
        table_constraints.push(transform_table_constraint(tc, identities)?);
    }

    Ok(SqlCreateTable {
        table: def.name,
        temp: def.temp,
        columns: sql_columns,
        table_constraints,
    })
}

/// Transform a single column definition, returning the SQL column def and any
/// constraints that need to be promoted to table-level (composite PK/UNIQUE, FK).
fn transform_column(
    col: &ColumnDef<Resolved>,
    identities: &crate::names::Registry,
) -> Result<(SqlColumnDef, Vec<SqlTableConstraint>)> {
    let mut not_null = false;
    let mut primary_key = false;
    let mut unique = false;
    let mut checks = Vec::new();
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
                    columns: cols.clone(),
                });
            }
            DdlConstraint::NotNull => {
                not_null = true;
            }
            DdlConstraint::Check { expr } => {
                checks.push(transform_ddl_predicate(
                    expr.clone(),
                    Some(col.name),
                    identities,
                )?);
            }
            DdlConstraint::ForeignKey { table, columns } => {
                extra_constraints.push(SqlTableConstraint::ForeignKey {
                    columns: vec![col.name],
                    ref_table: *table,
                    ref_columns: columns.clone(),
                });
            }
        }
    }

    let default = col
        .default
        .as_ref()
        .map(|d| transform_default(d, col.name, identities))
        .transpose()?;

    Ok((
        SqlColumnDef {
            column: col.name,
            col_type: col.col_type.clone(),
            not_null,
            primary_key,
            unique,
            checks,
            default,
        },
        extra_constraints,
    ))
}

fn transform_default(
    default: &DdlDefault<Resolved>,
    column: crate::names::ColId,
    identities: &crate::names::Registry,
) -> Result<SqlDefaultClause> {
    match default {
        DdlDefault::Value { expr } => {
            let sql_expr = transform_ddl_expression(expr.clone(), Some(column), identities)?;
            Ok(SqlDefaultClause::Expression(sql_expr))
        }
        DdlDefault::Generated { expr, kind } => {
            let sql_expr = transform_ddl_expression(expr.clone(), Some(column), identities)?;
            Ok(SqlDefaultClause::Generated {
                expr: sql_expr,
                kind: kind.clone(),
            })
        }
    }
}

fn transform_table_constraint(
    c: &DdlConstraint<Resolved>,
    identities: &crate::names::Registry,
) -> Result<SqlTableConstraint> {
    match c {
        DdlConstraint::PrimaryKey {
            columns: Some(cols),
        } if !cols.is_empty() => Ok(SqlTableConstraint::PrimaryKey {
            columns: cols.clone(),
        }),
        DdlConstraint::PrimaryKey { .. } => {
            Err(crate::DelightQLError::validation_error_categorized(
                "imprint/manifest/table_constraint_columns",
                "A table-level primary key must name at least one column",
                "use \"%%(column, ...)\" on the \"_\" constraint row",
            ))
        }
        DdlConstraint::Unique {
            columns: Some(cols),
        } if !cols.is_empty() => Ok(SqlTableConstraint::Unique {
            columns: cols.clone(),
        }),
        DdlConstraint::Unique { .. } => Err(crate::DelightQLError::validation_error_categorized(
            "imprint/manifest/table_constraint_columns",
            "A table-level unique constraint must name at least one column",
            "use \"%(column, ...)\" on the \"_\" constraint row",
        )),
        DdlConstraint::Check { expr } => {
            let sql_expr = transform_ddl_predicate(expr.clone(), None, identities)?;
            Ok(SqlTableConstraint::Check { expr: sql_expr })
        }
        DdlConstraint::NotNull => {
            // Table-level NOT NULL doesn't make sense; shouldn't reach here
            Err(crate::DelightQLError::transpilation_error(
                "NotNull constraint at table level is invalid",
                "ddl_pipeline::transformer",
            ))
        }
        DdlConstraint::ForeignKey { table, columns } => {
            let ref_columns = columns
                .iter()
                .enumerate()
                .map(|(position, column)| {
                    identities.mint_column(
                        *table,
                        crate::names::ColumnOrigin::Bound {
                            position: position as u32,
                        },
                        identities.published(*column),
                        crate::names::Addressing::Published,
                        crate::names::ValueFacts::default(),
                    )
                })
                .collect();
            Ok(SqlTableConstraint::ForeignKey {
                columns: columns.clone(),
                ref_table: *table,
                ref_columns,
            })
        }
    }
}

/// Lower an n-ary logical composition, left-associating the SQL binary
/// operator the target actually has.
///
/// The AST is n-ary because associativity makes nesting meaningless; the
/// SQL AST's operator is binary, so the shape is rebuilt HERE and nowhere
/// earlier.
fn n_ary_ddl_predicate(
    parts: crate::pipeline::asts::vocabulary::Vec2<TruthExpression<Resolved>>,
    op: BinaryOperator,
    column: Option<crate::names::ColId>,
    identities: &crate::names::Registry,
) -> Result<SqlExpression> {
    let (first, rest) = parts.into_head_tail();
    let mut combined = transform_ddl_predicate(first, column, identities)?;
    for part in rest {
        combined = SqlExpression::Binary {
            left: Box::new(combined),
            op: op.clone(),
            right: Box::new(transform_ddl_predicate(part, column, identities)?),
        };
    }
    Ok(combined)
}

/// Convert a resolved DDL domain expression into a SQL expression.
///
/// This handles the small subset of expressions that appear in DDL (CHECK/DEFAULT):
/// Lvar, Literal, Function(Regular), Function(Infix). A CHECK's
/// body is a TRUTH and takes `transform_ddl_predicate` instead.
///
/// `@` is already the column: the DDL resolver bound it there.
fn transform_ddl_expression(
    expr: DomainExpression<Resolved>,
    column: Option<crate::names::ColId>,
    identities: &crate::names::Registry,
) -> Result<SqlExpression> {
    match expr {
        DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
            column,
            ..
        }))) => Ok(SqlExpression::Column(column)),
        DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Ground(value),
        ) => Ok(SqlExpression::Literal(value)),
        DomainExpression::Application(func) => transform_ddl_function(func, column, identities),
        other => Err(crate::DelightQLError::transpilation_error(
            format!(
                "Unsupported DDL expression variant: {:?}",
                std::mem::discriminant(&other)
            ),
            "ddl_pipeline::transformer",
        )),
    }
}

/// A DDL argument's value, lowered from the EXACT carrier it stands in. A
/// crossed argument is a truth read as a value, and it is spelled as the
/// predicate it is rather than converted back into a value that holds one.
fn transform_ddl_argument(
    value: crate::pipeline::asts::core::ArgumentValue<Resolved>,
    column: Option<crate::names::ColId>,
    identities: &crate::names::Registry,
) -> Result<SqlExpression> {
    use crate::pipeline::asts::core::ArgumentValue;
    match value {
        ArgumentValue::Domain { value, .. } => transform_ddl_expression(value, column, identities),
        ArgumentValue::Truth(crossing) => {
            transform_ddl_predicate(crossing.into_truth(), column, identities)
        }
    }
}

fn transform_ddl_function(
    func: FunctionApplication<Resolved>,
    column: Option<crate::names::ColId>,
    identities: &crate::names::Registry,
) -> Result<SqlExpression> {
    match func {
        crate::pipeline::asts::core::FunctionApplication::Standard(application) => {
            let call = application.call.into_inner();
            let name = {
                let mut name = String::new();
                identities
                    .write_function(call.callee, &mut crate::names::sink::Teaching(&mut name))
                    .map_err(|error| {
                        crate::DelightQLError::transpilation_error(
                            format!("cannot render DDL callable: {error:?}"),
                            "ddl_pipeline::transformer",
                        )
                    })?;
                name
            };
            // A DDL scalar call carries the scalar stratum by type; a
            // relational argument is unrepresentable here.
            let args = match call.arguments {
                crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => members,
                crate::pipeline::asts::core::operators::CallArguments::None => Vec::new(),
                crate::pipeline::asts::core::operators::CallArguments::HigherOrder(_) => {
                    return Err(crate::DelightQLError::transpilation_error(
                        "DDL scalar call cannot contain a relational argument",
                        "ddl_pipeline::transformer",
                    ))
                }
            }
            .into_iter()
            .map(|member| match member {
                crate::pipeline::asts::core::operators::ScalarArgument::Value(value) => {
                    transform_ddl_argument(value, column, identities)
                }
                crate::pipeline::asts::core::operators::ScalarArgument::Spread(_)
                | crate::pipeline::asts::core::operators::ScalarArgument::Star => {
                    Err(crate::DelightQLError::transpilation_error(
                        "a DDL scalar call cannot contain an enumerating argument",
                        "ddl_pipeline::transformer",
                    ))
                }
                crate::pipeline::asts::core::operators::ScalarArgument::Callable(_) => {
                    Err(crate::DelightQLError::transpilation_error(
                        "a DDL scalar call cannot contain a callable argument",
                        "ddl_pipeline::transformer",
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;
            Ok(SqlExpression::Function {
                name: name.into(),
                args,
                distinct: false,
            })
        }
        crate::pipeline::asts::core::FunctionApplication::Infix(infix) => {
            let left_sql = transform_ddl_expression(*infix.left, column, identities)?;
            let right_sql = transform_ddl_expression(*infix.right, column, identities)?;
            let op = match infix.operator {
                crate::pipeline::asts::vocabulary::BinOp::Add => BinaryOperator::Add,
                crate::pipeline::asts::vocabulary::BinOp::Sub => BinaryOperator::Subtract,
                crate::pipeline::asts::vocabulary::BinOp::Mul => BinaryOperator::Multiply,
                crate::pipeline::asts::vocabulary::BinOp::Div => BinaryOperator::Divide,
                crate::pipeline::asts::vocabulary::BinOp::Mod => BinaryOperator::Modulo,
                crate::pipeline::asts::vocabulary::BinOp::Concat => BinaryOperator::Concatenate,
            };
            Ok(SqlExpression::Binary {
                left: Box::new(left_sql),
                op,
                right: Box::new(right_sql),
            })
        }
        crate::pipeline::asts::core::FunctionApplication::Case(case) => {
            let mut when_clauses = Vec::new();
            let mut else_clause = None;
            let mut case_expr = None;

            let default = match case {
                crate::pipeline::asts::core::CaseExpression::Anchored {
                    anchor,
                    arms,
                    default,
                } => {
                    case_expr = Some(Box::new(transform_ddl_expression(
                        *anchor, column, identities,
                    )?));
                    for arm in arms.into_vec() {
                        when_clauses.push(WhenClause::new(
                            SqlExpression::Literal(arm.term),
                            transform_ddl_expression(*arm.result, column, identities)?,
                        ));
                    }
                    default
                }
                crate::pipeline::asts::core::CaseExpression::Searched { arms, default } => {
                    for arm in arms.into_vec() {
                        when_clauses.push(WhenClause::new(
                            transform_ddl_predicate(*arm.condition, column, identities)?,
                            transform_ddl_expression(*arm.result, column, identities)?,
                        ));
                    }
                    default
                }
            };
            if let Some(result) = default {
                else_clause = Some(Box::new(transform_ddl_expression(
                    *result, column, identities,
                )?));
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
    pred: TruthExpression<Resolved>,
    column: Option<crate::names::ColId>,
    identities: &crate::names::Registry,
) -> Result<SqlExpression> {
    match pred {
        TruthExpression::Comparison(Comparison {
            operator,
            left,
            right,
        }) => {
            let left_sql = transform_ddl_expression(*left, column, identities)?;
            let right_sql = transform_ddl_expression(*right, column, identities)?;
            let op = match operator {
                crate::pipeline::asts::vocabulary::CmpOp::Equal => BinaryOperator::Equal,
                crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual => BinaryOperator::IsNotDistinctFrom,
                crate::pipeline::asts::vocabulary::CmpOp::NotEqual => BinaryOperator::NotEqual,
                crate::pipeline::asts::vocabulary::CmpOp::NullSafeNotEqual => BinaryOperator::IsDistinctFrom,
                crate::pipeline::asts::vocabulary::CmpOp::LessThan => BinaryOperator::LessThan,
                crate::pipeline::asts::vocabulary::CmpOp::GreaterThan => BinaryOperator::GreaterThan,
                crate::pipeline::asts::vocabulary::CmpOp::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
                crate::pipeline::asts::vocabulary::CmpOp::GreaterThanOrEqual => {
                    BinaryOperator::GreaterThanOrEqual
                }
            };
            Ok(SqlExpression::Binary {
                left: Box::new(left_sql),
                op,
                right: Box::new(right_sql),
            })
        }
        TruthExpression::Conjunction(parts) => {
            n_ary_ddl_predicate(*parts, BinaryOperator::And, column, identities)
        }
        TruthExpression::Disjunction(parts) => {
            n_ary_ddl_predicate(*parts, BinaryOperator::Or, column, identities)
        }
        TruthExpression::Not { expr } => {
            let inner = transform_ddl_predicate(*expr, column, identities)?;
            Ok(SqlExpression::Unary {
                op: crate::pipeline::sql_ast::UnaryOperator::Not,
                expr: Box::new(inner),
            })
        }
        TruthExpression::Membership(Membership {
            probe,
            rows,
            negated,
            ..
        }) => {
            // A CHECK IS NOT A DIFFERENT TRUTH LANGUAGE. Literal membership
            // is null-safe here as everywhere (equality-law rows 5-6): OR
            // across candidate rows, AND across corresponding components,
            // `IS NOT DISTINCT FROM` per pair. SQL `IN` is never emitted —
            // it answers unknown on a null probe, which is the case the
            // author reached for membership to handle.
            let probe_width = probe.width();
            let probes = probe
                .into_values()
                .try_map(|p| transform_ddl_expression(p, column, identities))?;
            let candidates = rows.try_map(|row| -> Result<_> {
                let row_width = row.width();
                // The zip pairs each probe component with its own candidate
                // component and REFUSES on a width mismatch instead of
                // stopping at the shorter side, which would silently narrow
                // the test rather than name the error.
                let pairs = probes.clone().zip_exact(row.0).ok_or_else(|| {
                    crate::error::DelightQLError::validation_error_categorized(
                        "membership/arity",
                        format!(
                            "membership candidate has {} value(s) but the probe has {}",
                            row_width, probe_width
                        ),
                        "every candidate must match the probe's width",
                    )
                })?;
                Ok(pairs
                    .try_map(|(probe, value)| -> Result<_> {
                        Ok(SqlExpression::Binary {
                            left: Box::new(probe),
                            op: BinaryOperator::IsNotDistinctFrom,
                            right: Box::new(transform_ddl_expression(value, column, identities)?),
                        })
                    })?
                    .reduce(|left, right| SqlExpression::Binary {
                        left: Box::new(left),
                        op: BinaryOperator::And,
                        right: Box::new(right),
                    }))
            })?;
            // Both reductions are total: a candidate has at least one value
            // and a membership has at least one candidate, so there is no
            // empty set for this lowering to give a meaning to.
            let membership = candidates.reduce(|left, right| SqlExpression::Binary {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            });
            Ok(if negated {
                SqlExpression::Unary {
                    op: crate::pipeline::sql_ast::UnaryOperator::Not,
                    expr: Box::new(membership),
                }
            } else {
                membership
            })
        }
        // A DDL constraint's sigma application is a BIN predicate: a DQL
        // truth rule has no catalog to be fetched from at imprint time, and
        // the resolver that would expand one does not run here.
        TruthExpression::Sigma(SigmaApplication {
            proof: crate::pipeline::asts::core::NamedProof::Body(_),
            ..
        }) => Err(crate::DelightQLError::transpilation_error(
            "a DDL constraint observes a built-in predicate; a truth rule's body has no \
             lowering here",
            "ddl_pipeline::transformer",
        )),
        TruthExpression::Sigma(SigmaApplication {
            proof: crate::pipeline::asts::core::NamedProof::Call(call),
            polarity,
        }) => {
            let call = call.into_inner();
            let name = {
                let mut name = String::new();
                identities
                    .write_function(call.callee, &mut crate::names::sink::Teaching(&mut name))
                    .map_err(|error| {
                        crate::DelightQLError::transpilation_error(
                            format!("cannot render DDL sigma callable: {error:?}"),
                            "ddl_pipeline::transformer",
                        )
                    })?;
                name
            };
            let arguments: Vec<_> = match call.arguments {
                crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => members
                    .into_iter()
                    .filter_map(|member| match member {
                        crate::pipeline::asts::core::operators::ScalarArgument::Value(
                            expression,
                        ) => Some(expression),
                        _ => None,
                    })
                    .collect(),
                crate::pipeline::asts::core::operators::CallArguments::None
                | crate::pipeline::asts::core::operators::CallArguments::HigherOrder(_) => {
                    Vec::new()
                }
            };
            match name.as_str() {
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
                    let value = transform_ddl_argument(
                        args.next().expect("two arguments, counted above"),
                        column,
                        identities,
                    )?;
                    let pattern = transform_ddl_argument(
                        args.next().expect("two arguments, counted above"),
                        column,
                        identities,
                    )?;
                    // The atom is lowered UNOBSERVED and the polarity wraps
                    // it, exactly as on the query road. `NOT LIKE` is Kleene
                    // negation: on a null operand it is UNKNOWN, which a SQL
                    // CHECK accepts — so both polarities would admit the same
                    // row instead of equipartitioning it.
                    Ok(SqlExpression::Observation {
                        expr: Box::new(SqlExpression::Binary {
                            left: Box::new(value),
                            op: BinaryOperator::Like,
                            right: Box::new(pattern),
                        }),
                        positive: polarity.is_positive(),
                    })
                }
                other => Err(crate::DelightQLError::transpilation_error(
                    format!("Unsupported sigma predicate in DDL: {other}"),
                    "ddl_pipeline::transformer",
                )),
            }
        }
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
    use crate::pipeline::asts::core::{LiteralValue, Unresolved};

    fn lit_num(n: &str) -> DomainExpression<Unresolved> {
        DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(
            LiteralValue::Number(n.to_string()),
        ))
    }

    fn lit_str(s: &str) -> DomainExpression<Unresolved> {
        DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(
            LiteralValue::String(s.to_string()),
        ))
    }

    fn value_placeholder() -> DomainExpression<Unresolved> {
        DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Open(
            crate::pipeline::asts::core::DomainHole::CompositionInput,
        ))
    }

    fn simple_col(name: &str, col_type: &str) -> ColumnDef<Unresolved> {
        ColumnDef {
            name: name.to_string(),
            col_type: col_type.to_string(),
            constraints: vec![],
            default: None,
        }
    }

    fn transform_def(def: CreateTableDef<Unresolved>) -> SqlCreateTable {
        let (resolved, identities) = crate::ddl_pipeline::resolver::resolve(def).unwrap();
        transform(resolved, &identities).unwrap()
    }

    /// A CHECK body's lowering, read off the column constraint it is.
    fn transform_check(expr: TruthExpression<Unresolved>, column: &str) -> SqlExpression {
        let result = transform_def(CreateTableDef {
            name: "t".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: column.to_string(),
                col_type: "TEXT".to_string(),
                constraints: vec![DdlConstraint::Check { expr }],
                default: None,
            }],
            table_constraints: vec![],
        });
        result.columns[0].checks.first().expect("one check").clone()
    }

    fn transform_expr(expr: DomainExpression<Unresolved>, column: &str) -> SqlExpression {
        let result = transform_def(CreateTableDef {
            name: "t".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: column.to_string(),
                col_type: "TEXT".to_string(),
                constraints: vec![],
                default: Some(DdlDefault::Value { expr }),
            }],
            table_constraints: vec![],
        });
        match result.columns[0].default.clone().unwrap() {
            SqlDefaultClause::Expression(expr) => expr,
            SqlDefaultClause::Generated { .. } => unreachable!(),
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
        let result = transform_def(def);
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
        let result = transform_def(def);
        assert!(matches!(
            &result.table_constraints[0],
            SqlTableConstraint::PrimaryKey { columns, .. } if columns.len() == 2
        ));
    }

    #[test]
    fn table_constraints_without_columns_refuse_by_kind() {
        let registry = crate::names::Registry::new(&[]);
        let cases: [(DdlConstraint<Resolved>, &str); 4] = [
            (
                DdlConstraint::PrimaryKey { columns: None },
                "table-level primary key",
            ),
            (
                DdlConstraint::PrimaryKey {
                    columns: Some(vec![]),
                },
                "table-level primary key",
            ),
            (
                DdlConstraint::Unique { columns: None },
                "table-level unique constraint",
            ),
            (
                DdlConstraint::Unique {
                    columns: Some(vec![]),
                },
                "table-level unique constraint",
            ),
        ];

        for (constraint, kind) in cases {
            let error = transform_table_constraint(&constraint, &registry).unwrap_err();
            assert_eq!(
                error.error_uri(),
                "delightql-error://imprint/manifest/table_constraint_columns"
            );
            assert!(
                error.to_string().contains(kind),
                "error should identify {kind}: {error}"
            );
        }
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
        let result = transform_def(def);
        assert!(result.columns[0].not_null);
    }

    #[test]
    fn test_check_with_value_placeholder_substituted() {
        // @ > 0 → "age" > 0
        let check_expr = TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::GreaterThan,
            left: Box::new(value_placeholder()),
            right: Box::new(lit_num("0")),
        });
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
        let result = transform_def(def);
        let check = result.columns[0].checks.first().unwrap();
        // Should be Binary { Column("age"), GreaterThan, Literal(0) }
        match check {
            SqlExpression::Binary { left, op, right } => {
                assert!(matches!(left.as_ref(), SqlExpression::Column(_)));
                assert_eq!(*op, BinaryOperator::GreaterThan);
                assert!(
                    matches!(right.as_ref(), SqlExpression::Literal(LiteralValue::Number(n)) if n == "0")
                );
            }
            other => panic!("Expected Binary, got: {:?}", other),
        }
    }

    #[test]
    fn multiple_checks_remain_distinct_column_constraints() {
        let comparison = |operator: &str, value: &str| DdlConstraint::Check {
            expr: TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::from_name(operator)
                    .expect("DDL test comparison uses a closed CmpOp name"),
                left: Box::new(value_placeholder()),
                right: Box::new(lit_num(value)),
            }),
        };
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "age".into(),
                col_type: "INTEGER".into(),
                constraints: vec![
                    comparison("greater_than", "0"),
                    comparison("less_than", "100"),
                ],
                default: None,
            }],
            table_constraints: vec![],
        };

        let result = transform_def(def);

        assert_eq!(result.columns[0].checks.len(), 2);
        assert!(matches!(
            &result.columns[0].checks[0],
            SqlExpression::Binary {
                op: BinaryOperator::GreaterThan,
                ..
            }
        ));
        assert!(matches!(
            &result.columns[0].checks[1],
            SqlExpression::Binary {
                op: BinaryOperator::LessThan,
                ..
            }
        ));
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
        let result = transform_def(def);
        assert!(matches!(
            &result.columns[0].default,
            Some(SqlDefaultClause::Expression(SqlExpression::Literal(LiteralValue::Number(n)))) if n == "42"
        ));
    }

    #[test]
    fn test_in_list() {
        let expr = TruthExpression::Membership(Membership {
            probe: Probe::Value(Box::new(value_placeholder())),
            rows: Vec1::with_tail(
                ValueRow(Vec1::new(lit_num("1"))),
                vec![
                    ValueRow(Vec1::new(lit_num("2"))),
                    ValueRow(Vec1::new(lit_num("3"))),
                ],
            ),
            negated: false,
            source: crate::pipeline::asts::core::MembershipSource::In,
        });
        // A CHECK IS NOT A DIFFERENT TRUTH LANGUAGE: membership is null-safe
        // here as everywhere, so it lowers to OR over `IS NOT DISTINCT FROM`
        // and never to SQL `IN`, which answers unknown on a null probe.
        let rendered = format!("{:?}", transform_check(expr, "status"));
        assert!(
            !rendered.contains("InList"),
            "a DDL membership must not lower to SQL IN: {rendered}"
        );
        assert_eq!(rendered.matches("IsNotDistinctFrom").count(), 3);
        assert_eq!(rendered.matches("Or").count(), 2);
    }

    #[test]
    fn test_not_in_list() {
        let expr = TruthExpression::Membership(Membership {
            probe: Probe::Value(Box::new(value_placeholder())),
            rows: Vec1::with_tail(
                ValueRow(Vec1::new(lit_num("1"))),
                vec![ValueRow(Vec1::new(lit_num("2")))],
            ),
            negated: true,
            source: crate::pipeline::asts::core::MembershipSource::In,
        });
        let rendered = format!("{:?}", transform_check(expr, "status"));
        assert!(!rendered.contains("InList"));
        // Two-valued by construction, so the negation is a safe NOT.
        assert!(rendered.contains("Not"));
        assert_eq!(rendered.matches("IsNotDistinctFrom").count(), 2);
    }

    // === Sigma / LIKE ===

    #[test]
    fn test_like_sigma() {
        let expr = TruthExpression::Sigma(SigmaApplication {
            polarity: Polarity::Positive,
            proof: crate::pipeline::asts::core::NamedProof::Call(
                crate::pipeline::asts::core::PureCall::from_inner(
                    crate::pipeline::asts::core::FunctorCall::scalar(
                        crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                            &std::rc::Rc::new(crate::names::Registry::new(&[])),
                            crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                            "like",
                        ),
                        vec![value_placeholder(), lit_str("%abc")],
                    ),
                ),
            ),
        });
        // THE ATOM IS OBSERVED, not negated. A CHECK accepts UNKNOWN, so a
        // bare `LIKE` would admit the null this positive observation must
        // refuse — the collapse is the whole difference.
        let result = transform_check(expr, "name");
        let SqlExpression::Observation { expr, positive } = result else {
            panic!("Expected an observation, got: {result:?}");
        };
        assert!(positive);
        match *expr {
            SqlExpression::Binary { left, op, right } => {
                assert_eq!(op, BinaryOperator::Like);
                assert!(matches!(*left, SqlExpression::Column(_)));
                assert!(
                    matches!(*right, SqlExpression::Literal(LiteralValue::String(ref s)) if s == "%abc")
                );
            }
            other => panic!("Expected Binary LIKE, got: {:?}", other),
        }
    }

    #[test]
    fn test_not_like_sigma() {
        let expr = TruthExpression::Sigma(SigmaApplication {
            polarity: Polarity::Negative,
            proof: crate::pipeline::asts::core::NamedProof::Call(
                crate::pipeline::asts::core::PureCall::from_inner(
                    crate::pipeline::asts::core::FunctorCall::scalar(
                        crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                            &std::rc::Rc::new(crate::names::Registry::new(&[])),
                            crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                            "like",
                        ),
                        vec![value_placeholder(), lit_str("%test%")],
                    ),
                ),
            ),
        });
        // And the negative one observes the SAME atom. `NOT LIKE` is Kleene
        // negation: it keeps UNKNOWN, so both polarities would admit a null
        // row instead of one of them claiming it.
        let result = transform_check(expr, "name");
        let SqlExpression::Observation { expr, positive } = result else {
            panic!("Expected an observation, got: {result:?}");
        };
        assert!(!positive);
        match *expr {
            SqlExpression::Binary { op, .. } => assert_eq!(op, BinaryOperator::Like),
            other => panic!("Expected Binary NotLike, got: {:?}", other),
        }
    }

    // === CaseExpression ===

    #[test]
    fn test_case_searched() {
        let case_expr =
            DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Case(
                crate::pipeline::asts::core::CaseExpression::Searched {
                    arms: crate::pipeline::asts::vocabulary::Vec1::new(
                        crate::pipeline::asts::core::SearchedArm {
                            condition: Box::new(TruthExpression::Comparison(Comparison {
                                operator: crate::pipeline::asts::vocabulary::CmpOp::GreaterThan,
                                left: Box::new(value_placeholder()),
                                right: Box::new(lit_num("0")),
                            })),
                            result: Box::new(lit_str("positive")),
                        },
                    ),
                    default: Some(Box::new(lit_str("non-positive"))),
                },
            ));
        let result = transform_expr(case_expr, "val");
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
        let case_expr =
            DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Case(
                crate::pipeline::asts::core::CaseExpression::Anchored {
                    anchor: Box::new(value_placeholder()),
                    arms: crate::pipeline::asts::vocabulary::Vec1::with_tail(
                        crate::pipeline::asts::core::MatchArm {
                            term: LiteralValue::Number("1".to_string()),
                            result: Box::new(lit_str("one")),
                        },
                        vec![crate::pipeline::asts::core::MatchArm {
                            term: LiteralValue::Number("2".to_string()),
                            result: Box::new(lit_str("two")),
                        }],
                    ),
                    default: Some(Box::new(lit_str("other"))),
                },
            ));
        let result = transform_expr(case_expr, "code");
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
