// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::generator::{GeneratorError, SqlGenerator};

use super::sql_ast::{SqlColumnDef, SqlCreateTable, SqlDefaultClause, SqlTableConstraint};

/// Generate a SQL CREATE TABLE string from a SQL DDL AST.
///
/// The bin registry is the same one the query generator consults: a CHECK's
/// sigma predicate is rendered by the entity the resolver selected.
pub fn generate(
    table: &SqlCreateTable,
    identities: &crate::names::Registry,
    bin_registry: std::sync::Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>,
) -> crate::Result<String> {
    let mut collector = crate::pipeline::sql_ast::names::NameCollector::new(identities);
    collector.scope(table.table);
    for column in &table.columns {
        collector.column(column.column);
        for check in &column.checks {
            collector.expression(check);
        }
        if let Some(default) = &column.default {
            match default {
                SqlDefaultClause::Expression(expression)
                | SqlDefaultClause::Generated {
                    expr: expression, ..
                } => collector.expression(expression),
            }
        }
    }
    for constraint in &table.table_constraints {
        match constraint {
            SqlTableConstraint::PrimaryKey { columns } | SqlTableConstraint::Unique { columns } => {
                for column in columns {
                    collector.column(*column);
                }
            }
            SqlTableConstraint::Check { expr } => collector.expression(expr),
            SqlTableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
            } => {
                for column in columns {
                    collector.column(*column);
                }
                collector.scope(*ref_table);
                for column in ref_columns {
                    collector.column(*column);
                }
            }
        }
    }
    let bundle =
        crate::names::Bundle::gather(vec![collector.finish()]).reserve_authored(identities);
    let baptised = crate::names::baptise(identities, &bundle).map_err(|error| {
        crate::error::DelightQLError::parse_error(format!("DDL naming failed: {error:?}"))
    })?;
    let gen = SqlGenerator::new(&baptised).with_bin_registry(bin_registry);
    let mut sql = String::new();

    // CREATE [TEMP ]TABLE "name"
    sql.push_str("CREATE ");
    if table.temp {
        sql.push_str("TEMP ");
    }
    sql.push_str("TABLE ");
    gen.write_quoted_scope(&mut sql, table.table)
        .map_err(generator_error)?;

    sql.push_str(" (\n");

    // Column definitions + table constraints
    let mut parts: Vec<String> = Vec::new();

    for col in &table.columns {
        parts.push(generate_column(&gen, table.table, col).map_err(generator_error)?);
    }

    for tc in &table.table_constraints {
        parts.push(generate_table_constraint(&gen, table.table, tc).map_err(generator_error)?);
    }

    sql.push_str(&parts.join(",\n"));
    sql.push_str("\n)");

    Ok(sql)
}

fn generator_error(error: GeneratorError) -> crate::error::DelightQLError {
    error.into_delightql_error("DDL SQL generation error")
}

fn generate_column(
    gen: &SqlGenerator<'_, '_>,
    at: crate::names::ScopeId,
    col: &SqlColumnDef,
) -> Result<String, GeneratorError> {
    let mut s = String::new();
    s.push_str("  ");
    gen.write_quoted_column(&mut s, col.column)?;
    s.push(' ');
    s.push_str(&col.col_type);

    if col.primary_key {
        s.push_str(" PRIMARY KEY");
    }
    if col.not_null {
        s.push_str(" NOT NULL");
    }
    if col.unique {
        s.push_str(" UNIQUE");
    }

    if let Some(ref default) = col.default {
        match default {
            SqlDefaultClause::Expression(expr) => {
                s.push_str(" DEFAULT ");
                // SQLite requires parentheses around non-literal defaults (e.g., function calls).
                // Always wrap in parens for safety — SQLite accepts DEFAULT (42) and DEFAULT ('x') too.
                let needs_parens =
                    !matches!(expr, crate::pipeline::sql_ast::DomainExpression::Literal(_));
                if needs_parens {
                    s.push('(');
                }
                s.push_str(&gen.render_ddl_expression(expr, at)?);
                if needs_parens {
                    s.push(')');
                }
            }
            SqlDefaultClause::Generated { expr, kind } => {
                s.push_str(" GENERATED ALWAYS AS (");
                s.push_str(&gen.render_ddl_expression(expr, at)?);
                s.push(')');
                match kind {
                    super::asts::GeneratedKind::Virtual => s.push_str(" VIRTUAL"),
                    super::asts::GeneratedKind::Stored => s.push_str(" STORED"),
                }
            }
        }
    }

    for check_expr in &col.checks {
        s.push_str(" CHECK(");
        s.push_str(&gen.render_ddl_expression(check_expr, at)?);
        s.push(')');
    }

    Ok(s)
}

fn generate_table_constraint(
    gen: &SqlGenerator<'_, '_>,
    at: crate::names::ScopeId,
    tc: &SqlTableConstraint,
) -> Result<String, GeneratorError> {
    let mut s = String::new();
    s.push_str("  ");

    match tc {
        SqlTableConstraint::PrimaryKey { columns, .. } => {
            s.push_str("PRIMARY KEY(");
            write_column_list(gen, &mut s, columns)?;
            s.push(')');
        }
        SqlTableConstraint::Unique { columns, .. } => {
            s.push_str("UNIQUE(");
            write_column_list(gen, &mut s, columns)?;
            s.push(')');
        }
        SqlTableConstraint::Check { expr, .. } => {
            s.push_str("CHECK(");
            s.push_str(&gen.render_ddl_expression(expr, at)?);
            s.push(')');
        }
        SqlTableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            ..
        } => {
            s.push_str("FOREIGN KEY(");
            write_column_list(gen, &mut s, columns)?;
            s.push_str(") REFERENCES ");
            gen.write_quoted_scope(&mut s, *ref_table)?;
            s.push('(');
            write_column_list(gen, &mut s, ref_columns)?;
            s.push(')');
        }
    }

    Ok(s)
}

fn write_column_list(
    gen: &SqlGenerator<'_, '_>,
    sql: &mut String,
    columns: &[crate::names::ColId],
) -> Result<(), GeneratorError> {
    for (position, column) in columns.iter().enumerate() {
        if position > 0 {
            sql.push_str(", ");
        }
        gen.write_quoted_column(sql, *column)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_pipeline::sql_ast::{
        SqlColumnDef, SqlCreateTable, SqlDefaultClause, SqlTableConstraint,
    };
    use crate::names::{Addressing, Registry, ScopeId};

    /// The registry the DDL generator renders CHECK sigma predicates through.
    fn bin_registry() -> std::sync::Arc<crate::bin_cartridge::registry::BinCartridgeRegistry> {
        let mut registry = crate::bin_cartridge::registry::BinCartridgeRegistry::new();
        registry.register_cartridge(crate::bin_cartridge::prelude::create_prelude_cartridge());
        registry
            .register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());
        std::sync::Arc::new(registry)
    }
    use crate::pipeline::asts::core::LiteralValue;
    use crate::pipeline::sql_ast::{BinaryOperator, DomainExpression as SqlExpression};

    fn table(registry: &crate::names::Registry, name: &str) -> ScopeId {
        let spelling = registry.intern(name, false);
        registry.anonymous_scope(Some(spelling))
    }

    fn simple_col(
        registry: &crate::names::Registry,
        table: ScopeId,
        name: &str,
        col_type: &str,
        _position: u32,
    ) -> SqlColumnDef {
        let spelling = registry.intern(name, false);
        let column = registry.sql_column(table, Some(spelling), Addressing::Published);
        SqlColumnDef {
            column,
            col_type: col_type.to_string(),
            not_null: false,
            primary_key: false,
            unique: false,
            checks: Vec::new(),
            default: None,
        }
    }

    #[test]
    fn test_simple_pk_column() {
        let registry = Registry::new(&[]);
        let table_id = table(&registry, "users");
        let table = SqlCreateTable {
            table: table_id,
            temp: false,
            columns: vec![SqlColumnDef {
                primary_key: true,
                ..simple_col(&registry, table_id, "id", "INTEGER", 0)
            }],
            table_constraints: vec![],
        };
        let sql = generate(&table, &registry, bin_registry()).unwrap();
        assert!(sql.contains("CREATE TABLE \"users\""));
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY"));
    }

    #[test]
    fn test_not_null_with_default() {
        let registry = Registry::new(&[]);
        let table_id = table(&registry, "items");
        let table = SqlCreateTable {
            table: table_id,
            temp: false,
            columns: vec![SqlColumnDef {
                not_null: true,
                default: Some(SqlDefaultClause::Expression(SqlExpression::Literal(
                    LiteralValue::Number("42".into()),
                ))),
                ..simple_col(&registry, table_id, "count", "INTEGER", 0)
            }],
            table_constraints: vec![],
        };
        let sql = generate(&table, &registry, bin_registry()).unwrap();
        assert!(sql.contains("NOT NULL"));
        assert!(sql.contains("DEFAULT 42"));
    }

    #[test]
    fn test_check_constraint() {
        let registry = Registry::new(&[]);
        let table_id = table(&registry, "t");
        let mut age = simple_col(&registry, table_id, "age", "INTEGER", 0);
        age.checks.push(SqlExpression::Binary {
            left: Box::new(SqlExpression::Column(age.column)),
            op: BinaryOperator::GreaterThan,
            right: Box::new(SqlExpression::Literal(LiteralValue::Number("0".into()))),
        });
        let table = SqlCreateTable {
            table: table_id,
            temp: false,
            columns: vec![age],
            table_constraints: vec![],
        };
        let sql = generate(&table, &registry, bin_registry()).unwrap();
        assert!(sql.contains("CHECK(age > 0)"));
    }

    #[test]
    fn test_composite_pk() {
        let registry = Registry::new(&[]);
        let table_id = table(&registry, "t");
        let a = simple_col(&registry, table_id, "a", "INTEGER", 0);
        let b = simple_col(&registry, table_id, "b", "TEXT", 1);
        let table = SqlCreateTable {
            table: table_id,
            temp: false,
            columns: vec![a.clone(), b.clone()],
            table_constraints: vec![SqlTableConstraint::PrimaryKey {
                columns: vec![a.column, b.column],
            }],
        };
        let sql = generate(&table, &registry, bin_registry()).unwrap();
        assert!(sql.contains("PRIMARY KEY(\"a\", \"b\")"));
    }

    #[test]
    fn test_temp_table() {
        let registry = Registry::new(&[]);
        let table_id = table(&registry, "tmp");
        let table = SqlCreateTable {
            table: table_id,
            temp: true,
            columns: vec![simple_col(&registry, table_id, "x", "TEXT", 0)],
            table_constraints: vec![],
        };
        let sql = generate(&table, &registry, bin_registry()).unwrap();
        assert!(sql.starts_with("CREATE TEMP TABLE"));
    }

    #[test]
    fn test_foreign_key() {
        let registry = Registry::new(&[]);
        let orders = table(&registry, "orders");
        let users = table(&registry, "users");
        let user_id = simple_col(&registry, orders, "user_id", "INTEGER", 0);
        let id = simple_col(&registry, users, "id", "INTEGER", 0);
        let table = SqlCreateTable {
            table: orders,
            temp: false,
            columns: vec![user_id.clone()],
            table_constraints: vec![SqlTableConstraint::ForeignKey {
                columns: vec![user_id.column],
                ref_table: users,
                ref_columns: vec![id.column],
            }],
        };
        let sql = generate(&table, &registry, bin_registry()).unwrap();
        assert!(sql.contains("FOREIGN KEY(\"user_id\") REFERENCES \"users\"(\"id\")"));
    }

    #[test]
    fn test_unique_column() {
        let registry = Registry::new(&[]);
        let table_id = table(&registry, "t");
        let table = SqlCreateTable {
            table: table_id,
            temp: false,
            columns: vec![SqlColumnDef {
                unique: true,
                ..simple_col(&registry, table_id, "email", "TEXT", 0)
            }],
            table_constraints: vec![],
        };
        let sql = generate(&table, &registry, bin_registry()).unwrap();
        assert!(sql.contains("UNIQUE"));
    }
}
