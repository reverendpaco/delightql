use crate::pipeline::generator_v3::SqlGenerator;

use super::sql_ast::{SqlColumnDef, SqlCreateTable, SqlDefaultClause, SqlTableConstraint};

/// Generate a SQL CREATE TABLE string from a SQL DDL AST.
pub fn generate(table: &SqlCreateTable) -> String {
    let gen = SqlGenerator::new();
    let mut sql = String::new();

    // CREATE [TEMP ]TABLE "name"
    sql.push_str("CREATE ");
    if table.temp {
        sql.push_str("TEMP ");
    }
    sql.push_str("TABLE ");
    write_quoted(&mut sql, &table.name);

    sql.push_str(" (\n");

    // Column definitions + table constraints
    let mut parts: Vec<String> = Vec::new();

    for col in &table.columns {
        parts.push(generate_column(&gen, col));
    }

    for tc in &table.table_constraints {
        parts.push(generate_table_constraint(&gen, tc));
    }

    sql.push_str(&parts.join(",\n"));
    sql.push_str("\n)");

    sql
}

fn generate_column(gen: &SqlGenerator, col: &SqlColumnDef) -> String {
    let mut s = String::new();
    s.push_str("  ");
    write_quoted(&mut s, &col.name);
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
                let needs_parens = !matches!(
                    expr,
                    crate::pipeline::sql_ast_v3::DomainExpression::Literal(_)
                );
                if needs_parens {
                    s.push('(');
                }
                match gen.render_expression(expr) {
                    Ok(rendered) => s.push_str(&rendered),
                    Err(_) => s.push_str("/* ERROR */"),
                }
                if needs_parens {
                    s.push(')');
                }
            }
            SqlDefaultClause::Generated { expr, kind } => {
                s.push_str(" GENERATED ALWAYS AS (");
                match gen.render_expression(expr) {
                    Ok(rendered) => s.push_str(&rendered),
                    Err(_) => s.push_str("/* ERROR */"),
                }
                s.push(')');
                match kind {
                    super::asts::GeneratedKind::Virtual => s.push_str(" VIRTUAL"),
                    super::asts::GeneratedKind::Stored => s.push_str(" STORED"),
                }
            }
        }
    }

    if let Some(ref check_expr) = col.check {
        s.push_str(" CHECK(");
        match gen.render_expression(check_expr) {
            Ok(rendered) => s.push_str(&rendered),
            Err(_) => s.push_str("/* ERROR */"),
        }
        s.push(')');
    }

    s
}

fn generate_table_constraint(gen: &SqlGenerator, tc: &SqlTableConstraint) -> String {
    let mut s = String::new();
    s.push_str("  ");

    match tc {
        SqlTableConstraint::PrimaryKey { columns, .. } => {
            s.push_str("PRIMARY KEY(");
            s.push_str(&quote_column_list(columns));
            s.push(')');
        }
        SqlTableConstraint::Unique { columns, .. } => {
            s.push_str("UNIQUE(");
            s.push_str(&quote_column_list(columns));
            s.push(')');
        }
        SqlTableConstraint::Check { expr, .. } => {
            s.push_str("CHECK(");
            match gen.render_expression(expr) {
                Ok(rendered) => s.push_str(&rendered),
                Err(_) => s.push_str("/* ERROR */"),
            }
            s.push(')');
        }
        SqlTableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            ..
        } => {
            s.push_str("FOREIGN KEY(");
            s.push_str(&quote_column_list(columns));
            s.push_str(") REFERENCES ");
            write_quoted(&mut s, ref_table);
            s.push('(');
            s.push_str(&quote_column_list(ref_columns));
            s.push(')');
        }
    }

    s
}

fn write_quoted(s: &mut String, name: &str) {
    s.push('"');
    s.push_str(name);
    s.push('"');
}

fn quote_column_list(cols: &[String]) -> String {
    cols.iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_pipeline::asts::GeneratedKind;
    use crate::ddl_pipeline::sql_ast::{
        SqlColumnDef, SqlCreateTable, SqlDefaultClause, SqlTableConstraint,
    };
    use crate::pipeline::asts::core::LiteralValue;
    use crate::pipeline::sql_ast_v3::{BinaryOperator, DomainExpression as SqlExpression};

    fn simple_col(name: &str, col_type: &str) -> SqlColumnDef {
        SqlColumnDef {
            name: name.to_string(),
            col_type: col_type.to_string(),
            not_null: false,
            primary_key: false,
            unique: false,
            check: None,
            default: None,
        }
    }

    #[test]
    fn test_simple_pk_column() {
        let table = SqlCreateTable {
            name: "users".into(),
            temp: false,
            columns: vec![SqlColumnDef {
                primary_key: true,
                ..simple_col("id", "INTEGER")
            }],
            table_constraints: vec![],
        };
        let sql = generate(&table);
        assert!(sql.contains("CREATE TABLE \"users\""));
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY"));
    }

    #[test]
    fn test_not_null_with_default() {
        let table = SqlCreateTable {
            name: "items".into(),
            temp: false,
            columns: vec![SqlColumnDef {
                not_null: true,
                default: Some(SqlDefaultClause::Expression(SqlExpression::Literal(
                    LiteralValue::Number("42".into()),
                ))),
                ..simple_col("count", "INTEGER")
            }],
            table_constraints: vec![],
        };
        let sql = generate(&table);
        assert!(sql.contains("NOT NULL"));
        assert!(sql.contains("DEFAULT 42"));
    }

    #[test]
    fn test_check_constraint() {
        let table = SqlCreateTable {
            name: "t".into(),
            temp: false,
            columns: vec![SqlColumnDef {
                check: Some(SqlExpression::Binary {
                    left: Box::new(SqlExpression::Column {
                        name: "age".into(),
                        qualifier: None,
                    }),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(SqlExpression::Literal(LiteralValue::Number("0".into()))),
                }),
                ..simple_col("age", "INTEGER")
            }],
            table_constraints: vec![],
        };
        let sql = generate(&table);
        assert!(sql.contains("CHECK(age > 0)"));
    }

    #[test]
    fn test_composite_pk() {
        let table = SqlCreateTable {
            name: "t".into(),
            temp: false,
            columns: vec![simple_col("a", "INTEGER"), simple_col("b", "TEXT")],
            table_constraints: vec![SqlTableConstraint::PrimaryKey {
                _name: None,
                columns: vec!["a".into(), "b".into()],
            }],
        };
        let sql = generate(&table);
        assert!(sql.contains("PRIMARY KEY(\"a\", \"b\")"));
    }

    #[test]
    fn test_temp_table() {
        let table = SqlCreateTable {
            name: "tmp".into(),
            temp: true,
            columns: vec![simple_col("x", "TEXT")],
            table_constraints: vec![],
        };
        let sql = generate(&table);
        assert!(sql.starts_with("CREATE TEMP TABLE"));
    }

    #[test]
    fn test_foreign_key() {
        let table = SqlCreateTable {
            name: "orders".into(),
            temp: false,
            columns: vec![simple_col("user_id", "INTEGER")],
            table_constraints: vec![SqlTableConstraint::ForeignKey {
                _name: None,
                columns: vec!["user_id".into()],
                ref_table: "users".into(),
                ref_columns: vec!["id".into()],
            }],
        };
        let sql = generate(&table);
        assert!(sql.contains("FOREIGN KEY(\"user_id\") REFERENCES \"users\"(\"id\")"));
    }

    #[test]
    fn test_unique_column() {
        let table = SqlCreateTable {
            name: "t".into(),
            temp: false,
            columns: vec![SqlColumnDef {
                unique: true,
                ..simple_col("email", "TEXT")
            }],
            table_constraints: vec![],
        };
        let sql = generate(&table);
        assert!(sql.contains("UNIQUE"));
    }
}
