// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::asts::core::expressions::domain::DomainExpression;
use crate::pipeline::asts::core::expressions::functions::FunctionApplication;
use crate::pipeline::asts::core::expressions::truth::TruthExpression;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Resolved;
use crate::pipeline::asts::core::TruthConsumer;
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
                checks.push(transform_check(expr.clone(), Some(col.name), identities)?);
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
            let sql_expr = transform_check(expr.clone(), None, identities)?;
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
                .map(|(_position, column)| {
                    identities.sql_column(
                        *table,
                        identities.published(*column),
                        crate::names::Addressing::Published,
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
    consumer: TruthConsumer,
) -> Result<SqlExpression> {
    let (first, rest) = parts.into_head_tail();
    let mut combined = transform_ddl_predicate(first, column, identities, consumer)?;
    for part in rest {
        combined = SqlExpression::Binary {
            left: Box::new(combined),
            op: op.clone(),
            right: Box::new(transform_ddl_predicate(part, column, identities, consumer)?),
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
        }))) => Ok(SqlExpression::Column(column.column())),
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

/// A DDL argument's value. DISTINCT is the argument's own data and is not a
/// DDL concern; the value lowers through the ordinary scalar road.
fn transform_ddl_argument(
    value: crate::pipeline::asts::core::ArgumentValue<Resolved>,
    column: Option<crate::names::ColId>,
    identities: &crate::names::Registry,
) -> Result<SqlExpression> {
    transform_ddl_expression(value.value, column, identities)
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
        crate::pipeline::asts::core::FunctionApplication::Case(case) => match case {
            // THE SAME MATCH LAW AS EVERY OTHER ROAD. A null arm is not dead
            // code in DelightQL, so the shape the target can express is
            // decided by the arms and by one lowering, not once per road.
            crate::pipeline::asts::core::CaseExpression::Anchored {
                anchor,
                arms,
                default,
            } => {
                let subject = transform_ddl_expression(*anchor, column, identities)?;
                let asked = arms
                    .into_vec()
                    .into_iter()
                    .map(|arm| {
                        Ok((
                            arm.term,
                            transform_ddl_expression(*arm.result, column, identities)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = default
                    .map(|result| transform_ddl_expression(*result, column, identities))
                    .transpose()?;
                SqlExpression::anchored_case(subject, asked, else_expr)
            }
            crate::pipeline::asts::core::CaseExpression::Searched { arms, default } => {
                let mut when_clauses = Vec::new();
                for arm in arms.into_vec() {
                    when_clauses.push(WhenClause::new(
                        // A CASE ARM'S GUARD IS A FILTER: it fires exactly
                        // where its truth is TRUE, whatever consumes the
                        // value the arm produces.
                        transform_ddl_predicate(
                            *arm.condition,
                            column,
                            identities,
                            TruthConsumer::Filter,
                        )?,
                        transform_ddl_expression(*arm.result, column, identities)?,
                    ));
                }
                let else_clause = default
                    .map(|result| transform_ddl_expression(*result, column, identities))
                    .transpose()?
                    .map(Box::new);
                Ok(SqlExpression::Case {
                    expr: None,
                    when_clauses,
                    else_clause,
                })
            }
        },
        // THE CROSSING lowers as the truth it is, READ AS A VALUE: the DDL
        // predicate road spells it, told that nothing observes it here, and
        // nothing converts it back.
        FunctionApplication::Crossed(crossing) => transform_ddl_predicate(
            crossing.into_truth(),
            column,
            identities,
            TruthConsumer::Value,
        ),
        other => Err(crate::DelightQLError::transpilation_error(
            format!(
                "Unsupported DDL function variant: {:?}",
                std::mem::discriminant(&other)
            ),
            "ddl_pipeline::transformer",
        )),
    }
}

/// A CHECK's body, lowered for the consumer a database CHECK is: the row is
/// refused exactly when the constrained truth is FALSE, so TRUE and UNKNOWN
/// alike satisfy it. This is the ONE entrance a column or table constraint
/// takes; the consumer is named here and never rediscovered downstream.
fn transform_check(
    pred: TruthExpression<Resolved>,
    column: Option<crate::names::ColId>,
    identities: &crate::names::Registry,
) -> Result<SqlExpression> {
    transform_ddl_predicate(pred, column, identities, TruthConsumer::Constraint)
}

fn transform_ddl_predicate(
    pred: TruthExpression<Resolved>,
    column: Option<crate::names::ColId>,
    identities: &crate::names::Registry,
    consumer: TruthConsumer,
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
                crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual => {
                    BinaryOperator::IsNotDistinctFrom
                }
                crate::pipeline::asts::vocabulary::CmpOp::NotEqual => BinaryOperator::NotEqual,
                crate::pipeline::asts::vocabulary::CmpOp::NullSafeNotEqual => {
                    BinaryOperator::IsDistinctFrom
                }
                crate::pipeline::asts::vocabulary::CmpOp::LessThan => BinaryOperator::LessThan,
                crate::pipeline::asts::vocabulary::CmpOp::GreaterThan => {
                    BinaryOperator::GreaterThan
                }
                crate::pipeline::asts::vocabulary::CmpOp::LessThanOrEqual => {
                    BinaryOperator::LessThanOrEqual
                }
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
            n_ary_ddl_predicate(*parts, BinaryOperator::And, column, identities, consumer)
        }
        TruthExpression::Disjunction(parts) => {
            n_ary_ddl_predicate(*parts, BinaryOperator::Or, column, identities, consumer)
        }
        TruthExpression::Not { expr } => {
            let inner = transform_ddl_predicate(*expr, column, identities, consumer)?;
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
        // THE SAME PREDICATE IDENTITY AS THE QUERY ROAD: the call lowers to
        // the predicate rewrite the generator resolves through the bin
        // registry, so `like`, `sql_eq` and `sql_ne` have one lowering. The
        // atom is lowered UNOBSERVED and the CONSUMER decides whether the
        // positive proof is collapsed: only a filter partitions its input,
        // and a CHECK refuses exactly what it can call FALSE. Negative
        // polarity is already the two-valued "not proven TRUE" and is spelled
        // `IS NOT TRUE` for every consumer — never the target's own `NOT`.
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
            let namespace = identities
                .function_namespace(call.callee)
                .into_iter()
                .map(|part| {
                    let mut text = String::new();
                    identities.write(part, &mut crate::names::sink::Teaching(&mut text));
                    text
                })
                .collect::<Vec<_>>();
            let args = match call.arguments {
                crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => members
                    .into_iter()
                    .filter_map(|member| match member {
                        crate::pipeline::asts::core::operators::ScalarArgument::Value(
                            expression,
                        ) => Some(transform_ddl_argument(expression, column, identities)),
                        _ => None,
                    })
                    .collect::<Result<Vec<_>>>()?,
                crate::pipeline::asts::core::operators::CallArguments::None
                | crate::pipeline::asts::core::operators::CallArguments::HigherOrder(_) => {
                    Vec::new()
                }
            };
            let proof = SqlExpression::PredicateRewrite {
                name,
                namespace,
                args,
                negated: false,
            };
            Ok(
                if polarity.is_positive() && !consumer.observes_positive_proof() {
                    proof
                } else {
                    SqlExpression::Observation {
                        expr: Box::new(proof),
                        positive: polarity.is_positive(),
                    }
                },
            )
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
        // A CHECK CARRIES THE POSITIVE PROOF UNOBSERVED. The constraint
        // refuses exactly what it can call FALSE, and an UNKNOWN `LIKE`
        // against a null is not FALSE — collapsing it with `IS TRUE` would
        // make the constraint refuse a row SQL's own CHECK rule admits, and
        // would silently imply the NOT NULL nobody wrote. The atom itself is
        // the predicate identity the query road carries: the generator
        // renders it through the selected bin entity, not a DDL-local arm.
        let result = transform_check(expr, "name");
        match result {
            SqlExpression::PredicateRewrite {
                name,
                namespace,
                args,
                negated,
            } => {
                assert_eq!(name, "like");
                assert!(namespace.is_empty());
                assert!(!negated);
                assert!(matches!(args[0], SqlExpression::Column(_)));
                assert!(
                    matches!(args[1], SqlExpression::Literal(LiteralValue::String(ref s)) if s == "%abc")
                );
            }
            other => panic!("Expected the bare like predicate rewrite, got: {:?}", other),
        }
    }

    /// THE CONSUMER, NOT THE PREDICATE, DECIDES. The same positive proof in
    /// a filtering position IS collapsed: a filter admits only TRUE, which is
    /// what makes the two polarities equipartition its input. Nothing about
    /// `like` changed between the two — only who consumed it.
    #[test]
    fn a_filter_observes_the_positive_proof_the_check_carries() {
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
        let identities = crate::names::Registry::new(&[]);
        let (resolved, identities) = crate::ddl_pipeline::resolver::resolve(CreateTableDef {
            name: "t".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: "name".to_string(),
                col_type: "TEXT".to_string(),
                constraints: vec![DdlConstraint::Check { expr }],
                default: None,
            }],
            table_constraints: vec![],
        })
        .map(|(resolved, minted)| {
            let _ = &identities;
            (resolved, minted)
        })
        .unwrap();
        let DdlConstraint::Check { expr } = resolved.columns[0].constraints[0].clone() else {
            panic!("the fixture writes one check")
        };
        let observed = transform_ddl_predicate(
            expr,
            Some(resolved.columns[0].name),
            &identities,
            TruthConsumer::Filter,
        )
        .unwrap();
        assert!(
            matches!(observed, SqlExpression::Observation { positive: true, .. }),
            "a filtering position collapses the proof it admits: {observed:?}"
        );
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
        // NEGATIVE POLARITY IS ALREADY TWO-VALUED — "not proven TRUE" — so
        // every consumer spells it the same way, `IS NOT TRUE`. It is not the
        // target's `NOT`: `NOT LIKE` is Kleene negation and keeps UNKNOWN,
        // which would answer the wrong question about a null row.
        let result = transform_check(expr, "name");
        let SqlExpression::Observation { expr, positive } = result else {
            panic!("Expected an observation, got: {result:?}");
        };
        assert!(!positive);
        match *expr {
            SqlExpression::PredicateRewrite { name, negated, .. } => {
                assert_eq!(name, "like");
                assert!(!negated);
            }
            other => panic!("Expected the like predicate rewrite, got: {:?}", other),
        }
    }

    /// The SQL comparison predicates take the same road as `like`: one
    /// predicate rewrite carrying the selected identity, its qualifier
    /// included, and — in a CHECK — carried without an observation. The
    /// CHECK law is GENERIC: nothing here reads the callee's name.
    #[test]
    fn test_sql_eq_sigma_keeps_its_qualified_identity() {
        let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
        let callee = crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
            &registry,
            crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
            "sql_eq",
        );
        let expr = TruthExpression::Sigma(SigmaApplication {
            polarity: Polarity::Positive,
            proof: crate::pipeline::asts::core::NamedProof::Call(
                crate::pipeline::asts::core::PureCall::from_inner(
                    crate::pipeline::asts::core::FunctorCall::scalar(
                        callee,
                        vec![value_placeholder(), lit_num("5")],
                    ),
                ),
            ),
        });
        let result = transform_check(expr, "state");
        match result {
            SqlExpression::PredicateRewrite { name, args, .. } => {
                assert_eq!(name, "sql_eq");
                assert_eq!(args.len(), 2);
            }
            other => panic!("Expected the sql_eq predicate rewrite, got: {:?}", other),
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

    /// A null match arm makes anchored matching observably different from
    /// SQL simple CASE. DQL null matches null, so lowering must switch to a
    /// searched case whose arm uses null-safe equality.
    #[test]
    fn an_anchored_null_arm_uses_null_safe_matching() {
        let case_expr =
            DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Case(
                crate::pipeline::asts::core::CaseExpression::Anchored {
                    anchor: Box::new(value_placeholder()),
                    arms: crate::pipeline::asts::vocabulary::Vec1::new(
                        crate::pipeline::asts::core::MatchArm {
                            term: LiteralValue::Null,
                            result: Box::new(lit_str("null-arm")),
                        },
                    ),
                    default: Some(Box::new(lit_str("other"))),
                },
            ));

        let result = transform_expr(case_expr, "code");
        let SqlExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } = result
        else {
            panic!("expected an anchored match to remain a case")
        };
        assert!(expr.is_none(), "a null arm cannot use SQL simple CASE");
        assert_eq!(when_clauses.len(), 1);
        assert!(else_clause.is_some());
        assert!(matches!(
            when_clauses[0].when(),
            SqlExpression::Binary {
                op: BinaryOperator::IsNotDistinctFrom,
                ..
            }
        ));
    }
}
