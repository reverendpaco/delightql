use std::collections::HashMap;

use delightql_types::schema::{ColumnInfo, DatabaseSchema};
use delightql_types::SqlIdentifier;

use crate::pipeline::ast_resolved;
use crate::pipeline::asts::core::expressions::boolean::BooleanExpression;
use crate::pipeline::asts::core::expressions::domain::DomainExpression;
use crate::pipeline::asts::core::metadata::{FqTable, NamespacePath, TableName};
use crate::pipeline::asts::core::provenance::ColumnProvenance;
use crate::pipeline::asts::core::{LiteralValue, PhaseBox, Resolved, Unresolved};
use crate::pipeline::resolver::resolving::resolve_domain_expr_with_full_context_and_system;
use crate::Result;

use super::asts::{ColumnDef, CreateTableDef, DdlConstraint, DdlDefault};

/// Validate column references and resolve phase markers.
///
/// - Resolves each `DomainExpression<Unresolved>` to `DomainExpression<Resolved>`
///   using the DQL resolver, validating Lvar references against the table's columns.
/// - Pattern-matches `@ != null` / `@ IS NOT NULL` in Check constraints → NotNull.
/// - Validates composite PK/UNIQUE column names exist in the table's column list.
pub fn resolve(def: CreateTableDef<Unresolved>) -> Result<CreateTableDef<Resolved>> {
    let available = build_available(&def.columns);

    let mut resolved_columns = Vec::with_capacity(def.columns.len());
    for col in def.columns {
        let constraints = resolve_constraints(col.constraints, &available)?;
        let default = resolve_default(col.default, &available)?;
        resolved_columns.push(ColumnDef {
            name: col.name,
            col_type: col.col_type,
            constraints,
            default,
        });
    }

    // Validate and resolve table-level constraints
    let table_constraints = resolve_constraints(def.table_constraints, &available)?;

    // Validate composite key columns exist
    let col_names: Vec<&str> = resolved_columns.iter().map(|c| c.name.as_str()).collect();
    validate_composite_keys(&resolved_columns, &table_constraints, &col_names)?;

    Ok(CreateTableDef {
        name: def.name,
        temp: def.temp,
        columns: resolved_columns,
        table_constraints,
    })
}

/// Build synthetic `ColumnMetadata` for each column so the DQL resolver
/// can validate Lvar references within DDL expressions.
fn build_available(columns: &[ColumnDef<Unresolved>]) -> Vec<ast_resolved::ColumnMetadata> {
    columns
        .iter()
        .enumerate()
        .map(|(i, col)| ast_resolved::ColumnMetadata {
            info: ColumnProvenance::from_table_column(
                SqlIdentifier::from(col.name.as_str()),
                TableName::Fresh,
                false,
            ),
            fq_table: FqTable {
                parents_path: NamespacePath::empty(),
                name: TableName::Fresh,
                backend_schema: PhaseBox::from_optional_schema(None),
            },
            table_position: Some(i),
            has_user_name: true,
            needs_hygienic_alias: false,
            needs_sql_rename: false,
            interior_schema: None,
        })
        .collect()
}

/// Empty schema — DDL expressions don't reference external tables.
struct EmptySchema;

impl DatabaseSchema for EmptySchema {
    fn get_table_columns(&self, _: Option<&str>, _: &str) -> Option<Vec<ColumnInfo>> {
        None
    }
    fn table_exists(&self, _: Option<&str>, _: &str) -> bool {
        false
    }
}

fn resolve_constraints(
    constraints: Vec<DdlConstraint<Unresolved>>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<Vec<DdlConstraint<Resolved>>> {
    let mut result = Vec::with_capacity(constraints.len());
    for c in constraints {
        match c {
            DdlConstraint::PrimaryKey { columns } => {
                result.push(DdlConstraint::PrimaryKey { columns });
            }
            DdlConstraint::Unique { columns } => {
                result.push(DdlConstraint::Unique { columns });
            }
            DdlConstraint::NotNull => {
                result.push(DdlConstraint::NotNull);
            }
            DdlConstraint::ForeignKey { table, columns } => {
                result.push(DdlConstraint::ForeignKey { table, columns });
            }
            DdlConstraint::Check { expr } => {
                // Check for NotNull pattern before resolving
                if is_not_null_pattern(&expr) {
                    result.push(DdlConstraint::NotNull);
                } else {
                    let resolved = resolve_expr(expr, available)?;
                    result.push(DdlConstraint::Check { expr: resolved });
                }
            }
        }
    }
    Ok(result)
}

fn resolve_default(
    default: Option<DdlDefault<Unresolved>>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<Option<DdlDefault<Resolved>>> {
    match default {
        None => Ok(None),
        Some(DdlDefault::Value { expr }) => {
            let resolved = resolve_expr(expr, available)?;
            Ok(Some(DdlDefault::Value { expr: resolved }))
        }
        Some(DdlDefault::Generated { expr, kind }) => {
            let resolved = resolve_expr(expr, available)?;
            Ok(Some(DdlDefault::Generated {
                expr: resolved,
                kind,
            }))
        }
    }
}

fn resolve_expr(
    expr: DomainExpression<Unresolved>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<DomainExpression<Resolved>> {
    // Intercept In predicates: the DQL resolver desugars In into InnerExists
    // (anonymous table), which is a query-time construct. In DDL context we keep
    // In as a simple value list — no subqueries.
    if let DomainExpression::Predicate {
        expr: ref pred,
        ref alias,
    } = expr
    {
        if let BooleanExpression::In {
            ref value,
            ref set,
            ref negated,
        } = **pred
        {
            let resolved_value = resolve_expr((**value).clone(), available)?;
            let resolved_set = set
                .iter()
                .map(|e| resolve_expr(e.clone(), available))
                .collect::<Result<Vec<_>>>()?;
            return Ok(DomainExpression::Predicate {
                expr: Box::new(BooleanExpression::In {
                    value: Box::new(resolved_value),
                    set: resolved_set,
                    negated: *negated,
                }),
                alias: alias.clone(),
            });
        }
    }

    let schema = EmptySchema;
    let mut cte_context = HashMap::new();
    resolve_domain_expr_with_full_context_and_system(
        expr,
        available,
        &schema,
        &mut cte_context,
        false,
        None,
        None,
    )
}

/// Pattern-match `@ != null` → promote to NotNull.
///
/// The builder produces:
///   `Predicate(Comparison { operator: "traditional_ne", left: ValuePlaceholder, right: Literal(Null) })`
/// for `@ != null`.
fn is_not_null_pattern(expr: &DomainExpression<Unresolved>) -> bool {
    match expr {
        DomainExpression::Predicate { expr: pred, .. } => match pred.as_ref() {
            BooleanExpression::Comparison {
                operator,
                left,
                right,
            } => {
                let op = operator.as_str();
                let ne = op == "traditional_ne" || op == "null_safe_ne";
                if ne && is_value_placeholder(left) && is_null_literal(right) {
                    return true;
                }
                if ne && is_null_literal(left) && is_value_placeholder(right) {
                    return true;
                }
                false
            }
            _ => false,
        },
        _ => false,
    }
}

fn is_value_placeholder(expr: &DomainExpression<Unresolved>) -> bool {
    matches!(expr, DomainExpression::ValuePlaceholder { .. })
}

fn is_null_literal(expr: &DomainExpression<Unresolved>) -> bool {
    matches!(
        expr,
        DomainExpression::Literal {
            value: LiteralValue::Null,
            ..
        }
    )
}

/// Validate that composite PK/UNIQUE constraint columns exist in the table.
fn validate_composite_keys(
    _columns: &[ColumnDef<Resolved>],
    constraints: &[DdlConstraint<Resolved>],
    col_names: &[&str],
) -> Result<()> {
    for c in constraints {
        match c {
            DdlConstraint::PrimaryKey {
                columns: Some(cols),
            }
            | DdlConstraint::Unique {
                columns: Some(cols),
            } => {
                for col in cols {
                    if !col_names.contains(&col.as_str()) {
                        return Err(crate::DelightQLError::validation_error(
                            format!("Composite key references unknown column '{col}'"),
                            "ddl_pipeline::resolver",
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_column(name: &str, col_type: &str) -> ColumnDef<Unresolved> {
        ColumnDef {
            name: name.to_string(),
            col_type: col_type.to_string(),
            constraints: vec![],
            default: None,
        }
    }

    #[test]
    fn test_bare_pk_passes_through() {
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
        let resolved = resolve(def).unwrap();
        assert!(matches!(
            &resolved.columns[0].constraints[0],
            DdlConstraint::PrimaryKey { columns: None }
        ));
    }

    #[test]
    fn test_bare_unique_passes_through() {
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "email".into(),
                col_type: "TEXT".into(),
                constraints: vec![DdlConstraint::Unique { columns: None }],
                default: None,
            }],
            table_constraints: vec![],
        };
        let resolved = resolve(def).unwrap();
        assert!(matches!(
            &resolved.columns[0].constraints[0],
            DdlConstraint::Unique { columns: None }
        ));
    }

    #[test]
    fn test_check_with_valid_column_resolves() {
        use crate::ddl_pipeline::builder;
        // length:(name) > 3 — references column "name" which exists
        let check = builder::build_constraint("length:(name) > 3").unwrap();
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![
                simple_column("id", "INTEGER"),
                ColumnDef {
                    name: "name".into(),
                    col_type: "TEXT".into(),
                    constraints: vec![check],
                    default: None,
                },
            ],
            table_constraints: vec![],
        };
        let resolved = resolve(def).unwrap();
        assert!(matches!(
            &resolved.columns[1].constraints[0],
            DdlConstraint::Check { .. }
        ));
    }

    #[test]
    fn test_check_with_invalid_column_errors() {
        use crate::ddl_pipeline::builder;
        // length:(nonexistent) > 3 — column doesn't exist
        let check = builder::build_constraint("length:(nonexistent) > 3").unwrap();
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "name".into(),
                col_type: "TEXT".into(),
                constraints: vec![check],
                default: None,
            }],
            table_constraints: vec![],
        };
        assert!(resolve(def).is_err());
    }

    #[test]
    fn test_not_null_pattern_match() {
        use crate::ddl_pipeline::builder;
        // @ != null should be promoted to NotNull
        let check = builder::build_constraint("@ != null").unwrap();
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "name".into(),
                col_type: "TEXT".into(),
                constraints: vec![check],
                default: None,
            }],
            table_constraints: vec![],
        };
        let resolved = resolve(def).unwrap();
        assert!(matches!(
            &resolved.columns[0].constraints[0],
            DdlConstraint::NotNull
        ));
    }

    #[test]
    fn test_composite_pk_with_invalid_column_errors() {
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![simple_column("a", "INTEGER"), simple_column("b", "TEXT")],
            table_constraints: vec![DdlConstraint::PrimaryKey {
                columns: Some(vec!["a".into(), "nonexistent".into()]),
            }],
        };
        assert!(resolve(def).is_err());
    }

    #[test]
    fn test_composite_pk_with_valid_columns() {
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![simple_column("a", "INTEGER"), simple_column("b", "TEXT")],
            table_constraints: vec![DdlConstraint::PrimaryKey {
                columns: Some(vec!["a".into(), "b".into()]),
            }],
        };
        let resolved = resolve(def).unwrap();
        assert!(matches!(
            &resolved.table_constraints[0],
            DdlConstraint::PrimaryKey { columns: Some(cols) } if cols.len() == 2
        ));
    }

    #[test]
    fn test_value_placeholder_passes_through() {
        use crate::ddl_pipeline::builder;
        // @ > 0 — value placeholder should pass through resolution unchanged
        let check = builder::build_constraint("@ > 0").unwrap();
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "age".into(),
                col_type: "INTEGER".into(),
                constraints: vec![check],
                default: None,
            }],
            table_constraints: vec![],
        };
        let resolved = resolve(def).unwrap();
        assert!(matches!(
            &resolved.columns[0].constraints[0],
            DdlConstraint::Check { .. }
        ));
    }

    #[test]
    fn test_default_literal_resolves() {
        use crate::ddl_pipeline::builder;
        let default = builder::build_default("42").unwrap();
        let def = CreateTableDef {
            name: "t".into(),
            temp: false,
            columns: vec![ColumnDef {
                name: "count".into(),
                col_type: "INTEGER".into(),
                constraints: vec![],
                default: Some(default),
            }],
            table_constraints: vec![],
        };
        let resolved = resolve(def).unwrap();
        assert!(matches!(
            &resolved.columns[0].default,
            Some(DdlDefault::Value { .. })
        ));
    }
}
