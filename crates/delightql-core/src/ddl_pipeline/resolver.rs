// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use delightql_types::schema::{ColumnInfo, DatabaseSchema};

use crate::pipeline::ast_resolved;
use crate::pipeline::asts::core::expressions::domain::DomainExpression;
use crate::pipeline::asts::core::expressions::truth::TruthExpression;
use crate::pipeline::asts::core::{LiteralValue, Resolved, Unresolved};
use crate::pipeline::resolver::resolving::{
    resolve_domain_expr_via_registry, resolve_truth_via_registry,
};
use crate::Result;

use super::asts::{ColumnDef, CreateTableDef, DdlConstraint, DdlDefault};
use crate::pipeline::asts::core::{Comparison, Membership};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::core::{Probe, ValueRow};

/// Validate column references and resolve phase markers.
///
/// - Resolves each `DomainExpression<Unresolved>` to `DomainExpression<Resolved>`
///   using the DQL resolver, validating Lvar references against the table's columns.
/// - Pattern-matches `@ != null` / `@ IS NOT NULL` in Check constraints → NotNull.
/// - Validates composite PK/UNIQUE column names exist in the table's column list.
pub fn resolve(
    def: CreateTableDef<Unresolved>,
) -> Result<(CreateTableDef<Resolved>, crate::relation::Planning)> {
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let (table, available) = build_available(&def.columns, &def.name, &identities)?;

    let mut resolved_columns = Vec::with_capacity(def.columns.len());
    for (col, metadata) in def.columns.into_iter().zip(&available) {
        let subject = Some(col.name.as_str());
        let constraints =
            resolve_constraints(col.constraints, &available, &identities, false, subject)?;
        let default = resolve_default(col.default, &available, &identities, subject)?;
        resolved_columns.push(ColumnDef {
            name: metadata.identity(),
            col_type: col.col_type,
            constraints,
            default,
        });
    }

    let table_constraints =
        resolve_constraints(def.table_constraints, &available, &identities, true, None)?;

    Ok((
        CreateTableDef {
            name: table,
            temp: def.temp,
            columns: resolved_columns,
            table_constraints,
        },
        identities,
    ))
}

/// Build synthetic `ColumnMetadata` for each column so the DQL resolver
/// can validate Lvar references within DDL expressions.
fn build_available(
    columns: &[ColumnDef<Unresolved>],
    table_name: &str,
    identities: &crate::relation::Planning,
) -> Result<(crate::names::ScopeId, Vec<ast_resolved::ColumnMetadata>)> {
    let table_spelling = identities.intern(table_name, false);
    let entity = identities.mint_entity(table_spelling);
    let slots = columns
        .iter()
        .enumerate()
        .map(|(position, column)| crate::relation::form::SourceSlot {
            position: position as u32,
            named: Some(identities.intern(column.name.as_str(), false)),
            declared_type: Some(column.col_type.clone()),
        })
        .collect::<Vec<_>>();
    let relation = identities
        .authority()
        .derive(crate::relation::RelForm::Source(
            crate::relation::form::SourceSpec {
                origin: crate::relation::form::SourceOrigin::Catalog { entity },
                slots: &slots,
                answers_to: Some(table_spelling),
            },
        ))?;
    let available = crate::relation::published_ports(identities, &relation)?
        .into_iter()
        .map(|port| ast_resolved::ColumnMetadata::new(port.column()))
        .collect();
    Ok((relation.scope(), available))
}

/// Empty schema — DDL expressions don't reference external tables.
struct EmptySchema;

impl DatabaseSchema for EmptySchema {
    fn get_table_columns(
        &self,
        _: Option<&str>,
        _: &str,
    ) -> crate::Result<Option<Vec<ColumnInfo>>> {
        Ok(None)
    }
    fn table_exists(&self, _: Option<&str>, _: &str) -> crate::Result<bool> {
        Ok(false)
    }
}

fn resolve_constraints(
    constraints: Vec<DdlConstraint<Unresolved>>,
    available: &[ast_resolved::ColumnMetadata],
    identities: &crate::relation::Planning,
    table_level: bool,
    subject: Option<&str>,
) -> Result<Vec<DdlConstraint<Resolved>>> {
    let mut result = Vec::with_capacity(constraints.len());
    for c in constraints {
        match c {
            DdlConstraint::PrimaryKey { columns } => {
                result.push(DdlConstraint::PrimaryKey {
                    columns: columns
                        .map(|names| resolve_local_columns(names, available, identities))
                        .transpose()?,
                });
            }
            DdlConstraint::Unique { columns } => {
                result.push(DdlConstraint::Unique {
                    columns: columns
                        .map(|names| resolve_local_columns(names, available, identities))
                        .transpose()?,
                });
            }
            DdlConstraint::NotNull => {
                result.push(DdlConstraint::NotNull);
            }
            DdlConstraint::ForeignKey { table, columns } => {
                if table_level {
                    return Err(crate::DelightQLError::validation_error_categorized(
                        "imprint/manifest/table_foreign_key",
                        "A table-level foreign key cannot distinguish its local columns from its referenced columns",
                        "attach a one-column foreign key to its local schema column, for example \
                         (\"local_column\", \"+parent(remote_column)\", \"fk_name\"); \
                         composite foreign keys require a dedicated syntax",
                    ));
                }
                let table_spelling = identities.intern(&table, false);
                let entity = identities.mint_entity(table_spelling);
                let ref_table = identities.resolved_access_scope(entity, table_spelling);
                let columns = columns
                    .into_iter()
                    .enumerate()
                    .map(|(_position, name)| {
                        let spelling = identities.intern(&name, false);
                        identities.sql_column(
                            ref_table,
                            Some(spelling),
                            crate::names::Addressing::Published,
                        )
                    })
                    .collect();
                result.push(DdlConstraint::ForeignKey {
                    table: ref_table,
                    columns,
                });
            }
            DdlConstraint::Check { expr } => {
                // Check for NotNull pattern before resolving
                if is_not_null_pattern(&expr) {
                    result.push(DdlConstraint::NotNull);
                } else {
                    let resolved = resolve_truth(expr, available, identities, subject)?;
                    result.push(DdlConstraint::Check { expr: resolved });
                }
            }
        }
    }
    Ok(result)
}

fn resolve_local_columns(
    names: Vec<String>,
    available: &[ast_resolved::ColumnMetadata],
    identities: &crate::relation::Planning,
) -> Result<Vec<crate::names::ColId>> {
    names
        .into_iter()
        .map(|name| {
            let spelling = identities.intern(&name, false);
            let symbol = identities.canonical(spelling);
            let matches: Vec<_> = available
                .iter()
                .map(ast_resolved::ColumnMetadata::identity)
                .filter(|column| identities.published_sym(*column) == Some(symbol))
                .collect();
            match matches.as_slice() {
                [column] => Ok(*column),
                [] => Err(crate::DelightQLError::validation_error(
                    format!("Constraint references unknown column '{name}'"),
                    "ddl_pipeline::resolver",
                )),
                _ => Err(crate::DelightQLError::validation_error(
                    format!("Constraint references ambiguous column '{name}'"),
                    "ddl_pipeline::resolver",
                )),
            }
        })
        .collect()
}

fn resolve_default(
    default: Option<DdlDefault<Unresolved>>,
    available: &[ast_resolved::ColumnMetadata],
    identities: &crate::relation::Planning,
    subject: Option<&str>,
) -> Result<Option<DdlDefault<Resolved>>> {
    match default {
        None => Ok(None),
        Some(DdlDefault::Value { expr }) => {
            let resolved = resolve_expr(expr, available, identities, subject)?;
            Ok(Some(DdlDefault::Value { expr: resolved }))
        }
        Some(DdlDefault::Generated { expr, kind }) => {
            let resolved = resolve_expr(expr, available, identities, subject)?;
            Ok(Some(DdlDefault::Generated {
                expr: resolved,
                kind,
            }))
        }
    }
}

/// Resolve a DDL expression.
///
/// `subject` is the column the expression is attached to, and it is what `@`
/// MEANS here: a CHECK or DEFAULT written on a column refers to that column's
/// value. It is resolved to the column at this boundary, so nothing carries a
/// DDL self-reference past the pipeline that knows what it names — a
/// table-level expression has no subject, and `@` in one refuses.
/// Replace `@` with a reference to the column the expression is written on.
///
/// In a column's CHECK or DEFAULT, `@` MEANS that column's value, and the DDL
/// text is where its name comes from — nothing is read back out of the
/// registry. What leaves here is an ordinary authored reference, so the DDL
/// self-reference is a form that exists only in front of this boundary.
/// A table-level expression has no subject, and `@` in one refuses.
fn name_the_subject_in_truth(
    expr: TruthExpression<Unresolved>,
    subject: Option<&str>,
) -> Result<TruthExpression<Unresolved>> {
    crate::pipeline::ast_transform::AstTransform::transform_boolean(
        &mut NameSubject { subject },
        expr,
    )
}

fn name_the_subject(
    expr: DomainExpression<Unresolved>,
    subject: Option<&str>,
) -> Result<DomainExpression<Unresolved>> {
    crate::pipeline::ast_transform::AstTransform::transform_domain(
        &mut NameSubject { subject },
        expr,
    )
}

/// The one walk that spends `@`, shared by the two body categories a DDL
/// column carries: a DEFAULT is a value, a CHECK is a truth.
struct NameSubject<'a> {
    subject: Option<&'a str>,
}

impl crate::pipeline::ast_transform::AstTransform<Unresolved, Unresolved> for NameSubject<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

    fn transform_domain(
        &mut self,
        expression: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Unresolved>> {
        match expression {
            DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Open(
                    crate::pipeline::asts::core::DomainHole::CompositionInput,
                ),
            ) => {
                let Some(subject) = self.subject else {
                    return Err(crate::DelightQLError::transpilation_error(
                        "A table-level DDL expression cannot use the value placeholder",
                        "ddl_pipeline::resolver",
                    ));
                };
                Ok(DomainExpression::Reference(Reference::Named(
                    NamedReference(crate::pipeline::asts::core::AuthoredColumn {
                        name: subject.into(),
                        qualifier: None,
                        namespace_path: crate::pipeline::asts::core::NamespacePath::empty(),
                    }),
                )))
            }
            other => crate::pipeline::ast_transform::walk_transform_domain(self, other),
        }
    }
}

fn resolve_expr(
    expr: DomainExpression<Unresolved>,
    available: &[ast_resolved::ColumnMetadata],
    identities: &crate::relation::Planning,
    subject: Option<&str>,
) -> Result<DomainExpression<Resolved>> {
    let expr = name_the_subject(expr, subject)?;
    let schema = EmptySchema;
    let mut registry = crate::resolution::ResolverCore::new(&schema, &identities);
    resolve_domain_expr_via_registry(expr, &mut registry, available, false)
}

/// Resolve a DDL CHECK's body, which is a TRUTH.
///
/// Membership is intercepted: the DQL resolver desugars `in` into an
/// anonymous-table inner exists, which is a query-time construct, and a
/// constraint has no query to put one in. Here it stays a list of values,
/// each resolved on its own.
fn resolve_truth(
    expr: TruthExpression<Unresolved>,
    available: &[ast_resolved::ColumnMetadata],
    identities: &crate::relation::Planning,
    subject: Option<&str>,
) -> Result<TruthExpression<Resolved>> {
    let expr = name_the_subject_in_truth(expr, subject)?;
    if let TruthExpression::Membership(Membership {
        probe,
        rows,
        negated,
        source,
    }) = expr
    {
        let resolved_probe = match probe {
            Probe::Value(value) => Probe::Value(Box::new(resolve_expr(
                *value, available, identities, subject,
            )?)),
            Probe::Row(values) => {
                Probe::Row(values.try_map(|v| resolve_expr(v, available, identities, subject))?)
            }
        };
        let resolved_rows = rows.try_map(|row| -> Result<_> {
            Ok(ValueRow(row.0.try_map(|e| {
                resolve_expr(e, available, identities, subject)
            })?))
        })?;
        return Ok(TruthExpression::Membership(Membership {
            probe: resolved_probe,
            rows: resolved_rows,
            negated,
            source,
        }));
    }

    let schema = EmptySchema;
    let mut registry = crate::resolution::ResolverCore::new(&schema, &identities);
    resolve_truth_via_registry(expr, &mut registry, available)
}

/// Pattern-match the null-safe `@ != null` → promote to NotNull.
///
/// The builder produces
///   `Comparison { operator: "null_safe_ne", left: @, right: Literal(Null) }`
/// for `@ != null`.
///
/// SQL inequality (`+sql_ne(@, null)`) is a sigma application, not this
/// comparison, and stays a CHECK: it is the null-safe operator alone that
/// promotes.
fn is_not_null_pattern(expr: &TruthExpression<Unresolved>) -> bool {
    match expr {
        TruthExpression::Comparison(Comparison {
            operator,
            left,
            right,
        }) => {
            let null_safe_ne =
                *operator == crate::pipeline::asts::vocabulary::CmpOp::NullSafeNotEqual;
            if null_safe_ne && is_value_placeholder(left) && is_null_literal(right) {
                return true;
            }
            if null_safe_ne && is_null_literal(left) && is_value_placeholder(right) {
                return true;
            }
            false
        }
        _ => false,
    }
}

fn is_value_placeholder(expr: &DomainExpression<Unresolved>) -> bool {
    matches!(
        expr,
        DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Open(
            crate::pipeline::asts::core::DomainHole::CompositionInput
        ))
    )
}

fn is_null_literal(expr: &DomainExpression<Unresolved>) -> bool {
    matches!(
        expr,
        DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(
            LiteralValue::Null
        ))
    )
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
        let resolved = resolve(def).unwrap().0;
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
        let resolved = resolve(def).unwrap().0;
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
        let resolved = resolve(def).unwrap().0;
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
        let resolved = resolve(def).unwrap().0;
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
        let resolved = resolve(def).unwrap().0;
        assert!(matches!(
            &resolved.table_constraints[0],
            DdlConstraint::PrimaryKey { columns: Some(cols) } if cols.len() == 2
        ));
    }

    #[test]
    fn table_level_foreign_key_refuses_ambiguous_column_roles() {
        let foreign_key =
            crate::ddl_pipeline::builder::build_constraint("+parents(parent_code)").unwrap();
        let def = CreateTableDef {
            name: "children".into(),
            temp: false,
            columns: vec![
                simple_column("id", "INTEGER"),
                simple_column("parent_code", "TEXT"),
            ],
            table_constraints: vec![foreign_key],
        };

        let error = match resolve(def) {
            Ok(_) => panic!("table-level foreign key should refuse"),
            Err(error) => error,
        };
        assert_eq!(
            error.error_uri(),
            "delightql-error://imprint/manifest/table_foreign_key"
        );
        assert!(error.to_string().contains("local columns"));
        assert!(error.to_string().contains("referenced columns"));
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
        let resolved = resolve(def).unwrap().0;
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
        let resolved = resolve(def).unwrap().0;
        assert!(matches!(
            &resolved.columns[0].default,
            Some(DdlDefault::Value { .. })
        ));
    }
}
