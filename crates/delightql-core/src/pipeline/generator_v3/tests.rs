// Integration tests for SQL Generator V3
//
// These tests manually build SQL AST V3 structures and verify the generated SQL.
// This validates our generator before the transformer V3 is implemented.

use crate::pipeline::ast_refined::LiteralValue;
use crate::pipeline::generator_v3::{SqlDialect, SqlGenerator};
use crate::pipeline::sql_ast_v3::*;
use crate::pipeline::transformer_v3::QualifierMint;

#[test]
fn test_simple_select_star() {
    let generator = SqlGenerator::new();

    // Build: SELECT * FROM users
    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_table("users")
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert_eq!(sql.trim(), "SELECT * FROM users");
}

#[test]
fn test_select_with_alias() {
    let generator = SqlGenerator::new();

    // Build: SELECT * FROM users AS u
    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_table_with_alias("users", "u")
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert_eq!(sql.trim(), "SELECT * FROM users AS u");
}

#[test]
fn test_specific_columns() {
    let generator = SqlGenerator::new();

    // Build: SELECT id, name FROM users
    let select = SelectStatement::builder()
        .select(SelectItem::expression(DomainExpression::column("id")))
        .select(SelectItem::expression(DomainExpression::column("name")))
        .from_table("users")
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert_eq!(sql.trim(), "SELECT id, name FROM users");
}

#[test]
fn test_qualified_columns() {
    let generator = SqlGenerator::new();

    // Build: SELECT u.id, u.name FROM users AS u
    let select = SelectStatement::builder()
        .select(SelectItem::expression(DomainExpression::with_qualifier(
            ColumnQualifier::table("u", &QualifierMint::for_test()),
            "id",
        )))
        .select(SelectItem::expression(DomainExpression::with_qualifier(
            ColumnQualifier::table("u", &QualifierMint::for_test()),
            "name",
        )))
        .from_table_with_alias("users", "u")
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert_eq!(sql.trim(), "SELECT u.id, u.name FROM users AS u");
}

#[test]
fn test_column_aliases() {
    let generator = SqlGenerator::new();

    // Build: SELECT user_id AS id, full_name AS name FROM users
    let select = SelectStatement::builder()
        .select(SelectItem::expression_with_alias(
            DomainExpression::column("user_id"),
            "id",
        ))
        .select(SelectItem::expression_with_alias(
            DomainExpression::column("full_name"),
            "name",
        ))
        .from_table("users")
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert_eq!(
        sql.trim(),
        "SELECT user_id AS id, full_name AS name FROM users"
    );
}

#[test]
fn test_simple_where() {
    let generator = SqlGenerator::new();

    // Build: SELECT * FROM users WHERE age > 18
    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_table("users")
        .where_clause(DomainExpression::Binary {
            left: Box::new(DomainExpression::column("age")),
            op: BinaryOperator::GreaterThan,
            right: Box::new(DomainExpression::literal(LiteralValue::Number(
                "18".to_string(),
            ))),
        })
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("SELECT * FROM users"));
    assert!(sql.contains("WHERE age > 18"));
}

#[test]
fn test_and_conditions() {
    let generator = SqlGenerator::new();

    // Build: SELECT * FROM users WHERE age > 18 AND status = 'active'
    let conditions = vec![
        DomainExpression::Binary {
            left: Box::new(DomainExpression::column("age")),
            op: BinaryOperator::GreaterThan,
            right: Box::new(DomainExpression::literal(LiteralValue::Number(
                "18".to_string(),
            ))),
        },
        DomainExpression::Binary {
            left: Box::new(DomainExpression::column("status")),
            op: BinaryOperator::Equal,
            right: Box::new(DomainExpression::literal(LiteralValue::String(
                "active".to_string(),
            ))),
        },
    ];

    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_table("users")
        .where_clause(DomainExpression::and(conditions))
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("age > 18"));
    assert!(sql.contains("AND"));
    assert!(sql.contains("status = 'active'"));
}

#[test]
fn test_inner_join() {
    let generator = SqlGenerator::new();

    // Build: SELECT u.*, o.* FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id
    let join = TableExpression::inner_join(
        TableExpression::table_with_alias("users", "u"),
        TableExpression::table_with_alias("orders", "o"),
        DomainExpression::eq(
            DomainExpression::with_qualifier(
                ColumnQualifier::table("u", &QualifierMint::for_test()),
                "id",
            ),
            DomainExpression::with_qualifier(
                ColumnQualifier::table("o", &QualifierMint::for_test()),
                "user_id",
            ),
        ),
    );

    let select = SelectStatement::builder()
        .select(SelectItem::qualified_star("u"))
        .select(SelectItem::qualified_star("o"))
        .from_tables(vec![join])
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("u.*, o.*"));
    assert!(sql.contains("users AS u"));
    assert!(sql.contains("INNER JOIN"));
    assert!(sql.contains("orders AS o"));
    assert!(sql.contains("ON u.id = o.user_id"));
}

#[test]
fn test_group_by_with_aggregates() {
    let generator = SqlGenerator::new();

    // Build: SELECT country, COUNT(*) AS user_count FROM users GROUP BY country
    let select = SelectStatement::builder()
        .select(SelectItem::expression(DomainExpression::column("country")))
        .select(SelectItem::expression_with_alias(
            DomainExpression::function("COUNT", vec![DomainExpression::star()]),
            "user_count",
        ))
        .from_table("users")
        .group_by(vec![DomainExpression::column("country")])
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("SELECT country, COUNT(*) AS user_count"));
    assert!(sql.contains("GROUP BY country"));
}

#[test]
fn test_group_by_with_having() {
    let generator = SqlGenerator::new();

    // Build: SELECT country, COUNT(*) AS cnt FROM users GROUP BY country HAVING COUNT(*) > 10
    let select = SelectStatement::builder()
        .select(SelectItem::expression(DomainExpression::column("country")))
        .select(SelectItem::expression_with_alias(
            DomainExpression::function("COUNT", vec![DomainExpression::star()]),
            "cnt",
        ))
        .from_table("users")
        .group_by(vec![DomainExpression::column("country")])
        .having(DomainExpression::Binary {
            left: Box::new(DomainExpression::function(
                "COUNT",
                vec![DomainExpression::star()],
            )),
            op: BinaryOperator::GreaterThan,
            right: Box::new(DomainExpression::literal(LiteralValue::Number(
                "10".to_string(),
            ))),
        })
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("GROUP BY country"));
    assert!(sql.contains("HAVING COUNT(*) > 10"));
}

#[test]
fn test_subquery_in_from() {
    let generator = SqlGenerator::new();

    // Build inner query: SELECT age * 2 AS double_age FROM users
    let inner = SelectStatement::builder()
        .select(SelectItem::expression_with_alias(
            DomainExpression::Binary {
                left: Box::new(DomainExpression::column("age")),
                op: BinaryOperator::Multiply,
                right: Box::new(DomainExpression::literal(LiteralValue::Number(
                    "2".to_string(),
                ))),
            },
            "double_age",
        ))
        .from_table("users")
        .build()
        .unwrap();

    // Build outer query: SELECT * FROM (...) AS t1 WHERE double_age > 50
    let outer = SelectStatement::builder()
        .select(SelectItem::star())
        .from_subquery(QueryExpression::Select(Box::new(inner)), "t1")
        .where_clause(DomainExpression::Binary {
            left: Box::new(DomainExpression::column("double_age")),
            op: BinaryOperator::GreaterThan,
            right: Box::new(DomainExpression::literal(LiteralValue::Number(
                "50".to_string(),
            ))),
        })
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(outer)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("SELECT * FROM"));
    assert!(sql.contains("SELECT age * 2 AS double_age FROM users"));
    assert!(sql.contains(") AS t1"));
    assert!(sql.contains("WHERE double_age > 50"));
}

#[test]
fn test_single_cte() {
    let generator = SqlGenerator::new();

    // CTE query: SELECT * FROM users WHERE status = 'active'
    let cte_query = QueryExpression::Select(Box::new(
        SelectStatement::builder()
            .select(SelectItem::star())
            .from_table("users")
            .where_clause(DomainExpression::Binary {
                left: Box::new(DomainExpression::column("status")),
                op: BinaryOperator::Equal,
                right: Box::new(DomainExpression::literal(LiteralValue::String(
                    "active".to_string(),
                ))),
            })
            .build()
            .unwrap(),
    ));

    // Main query: SELECT * FROM active_users
    let main_query = QueryExpression::Select(Box::new(
        SelectStatement::builder()
            .select(SelectItem::star())
            .from_table("active_users")
            .build()
            .unwrap(),
    ));

    let stmt = SqlStatement::with_ctes(
        Some(vec![Cte::new("active_users", cte_query)]),
        main_query,
    );

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.starts_with("WITH active_users AS"));
    assert!(sql.contains("SELECT * FROM users"));
    assert!(sql.contains("WHERE status = 'active'"));
    assert!(sql.contains("SELECT * FROM active_users"));
}

#[test]
fn test_distinct() {
    let generator = SqlGenerator::new();

    // Build: SELECT DISTINCT country FROM users
    let select = SelectStatement::builder()
        .distinct()
        .select(SelectItem::expression(DomainExpression::column("country")))
        .from_table("users")
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert_eq!(sql.trim(), "SELECT DISTINCT country FROM users");
}

#[test]
fn test_order_by_with_limit() {
    let generator = SqlGenerator::new();

    // Build: SELECT * FROM products ORDER BY price DESC, name ASC LIMIT 10
    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_table("products")
        .order_by(OrderTerm::desc(DomainExpression::column("price")))
        .order_by(OrderTerm::asc(DomainExpression::column("name")))
        .limit(10)
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("ORDER BY price DESC, name ASC"));
    assert!(sql.contains("LIMIT 10"));
}

#[test]
fn test_union() {
    let generator = SqlGenerator::new();

    // First query: SELECT name FROM customers
    let query1 = QueryExpression::Select(Box::new(
        SelectStatement::builder()
            .select(SelectItem::expression(DomainExpression::column("name")))
            .from_table("customers")
            .build()
            .unwrap(),
    ));

    // Second query: SELECT name FROM suppliers
    let query2 = QueryExpression::Select(Box::new(
        SelectStatement::builder()
            .select(SelectItem::expression(DomainExpression::column("name")))
            .from_table("suppliers")
            .build()
            .unwrap(),
    ));

    // UNION query
    let union_query = QueryExpression::SetOperation {
        op: SetOperator::Union,
        left: Box::new(query1),
        right: Box::new(query2),
    };

    let stmt = SqlStatement::simple(union_query);

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("SELECT name FROM customers"));
    assert!(sql.contains("UNION"));
    assert!(sql.contains("SELECT name FROM suppliers"));
}

#[test]
fn test_values() {
    let generator = SqlGenerator::new();

    // Build: VALUES (1, 'Alice'), (2, 'Bob')
    let values = QueryExpression::Values {
        rows: vec![
            vec![
                DomainExpression::literal(LiteralValue::Number("1".to_string())),
                DomainExpression::literal(LiteralValue::String("Alice".to_string())),
            ],
            vec![
                DomainExpression::literal(LiteralValue::Number("2".to_string())),
                DomainExpression::literal(LiteralValue::String("Bob".to_string())),
            ],
        ],
    };

    let stmt = SqlStatement::simple(values);

    let sql = generator.generate_statement(&stmt).unwrap();
    assert_eq!(sql.trim(), "VALUES (1, 'Alice'), (2, 'Bob')");
}

#[test]
fn test_in_list() {
    let generator = SqlGenerator::new();

    // Build: SELECT * FROM products WHERE category IN ('electronics', 'books')
    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_table("products")
        .where_clause(DomainExpression::InList {
            expr: Box::new(DomainExpression::column("category")),
            not: false,
            values: vec![
                DomainExpression::literal(LiteralValue::String("electronics".to_string())),
                DomainExpression::literal(LiteralValue::String("books".to_string())),
            ],
        })
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("category IN ('electronics', 'books')"));
}

#[test]
fn test_case_expression() {
    let generator = SqlGenerator::new();

    // Build: SELECT CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END AS age_group
    let case_expr = DomainExpression::Case {
        expr: None,
        when_clauses: vec![
            WhenClause::new(
                DomainExpression::Binary {
                    left: Box::new(DomainExpression::column("age")),
                    op: BinaryOperator::LessThan,
                    right: Box::new(DomainExpression::literal(LiteralValue::Number(
                        "18".to_string(),
                    ))),
                },
                DomainExpression::literal(LiteralValue::String("minor".to_string())),
            ),
            WhenClause::new(
                DomainExpression::Binary {
                    left: Box::new(DomainExpression::column("age")),
                    op: BinaryOperator::LessThan,
                    right: Box::new(DomainExpression::literal(LiteralValue::Number(
                        "65".to_string(),
                    ))),
                },
                DomainExpression::literal(LiteralValue::String("adult".to_string())),
            ),
        ],
        else_clause: Some(Box::new(DomainExpression::literal(LiteralValue::String(
            "senior".to_string(),
        )))),
    };

    let select = SelectStatement::builder()
        .select(SelectItem::expression_with_alias(case_expr, "age_group"))
        .from_table("users")
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("CASE"));
    assert!(sql.contains("WHEN age < 18 THEN 'minor'"));
    assert!(sql.contains("WHEN age < 65 THEN 'adult'"));
    assert!(sql.contains("ELSE 'senior'"));
    assert!(sql.contains("END AS age_group"));
}

#[test]
fn test_dialect_boolean_sqlite() {
    let generator = SqlGenerator::with_dialect(SqlDialect::SQLite);

    // Build: SELECT * FROM users WHERE active = 1 (SQLite uses 1/0 for booleans)
    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_table("users")
        .where_clause(DomainExpression::Binary {
            left: Box::new(DomainExpression::column("active")),
            op: BinaryOperator::Equal,
            right: Box::new(DomainExpression::literal(LiteralValue::Boolean(true))),
        })
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("active = 1"));
}

#[test]
fn test_dialect_boolean_postgres() {
    let generator = SqlGenerator::with_dialect(SqlDialect::PostgreSQL);

    // Build: SELECT * FROM users WHERE active = TRUE (PostgreSQL uses TRUE/FALSE)
    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_table("users")
        .where_clause(DomainExpression::Binary {
            left: Box::new(DomainExpression::column("active")),
            op: BinaryOperator::Equal,
            right: Box::new(DomainExpression::literal(LiteralValue::Boolean(true))),
        })
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(select)));

    let sql = generator.generate_statement(&stmt).unwrap();
    assert!(sql.contains("active = TRUE"));
}

#[test]
fn test_nested_subqueries() {
    let generator = SqlGenerator::new();

    // Build the deeply nested query from our examples
    // Level 1: SELECT id, total * 2 AS double_total FROM orders AS o
    let level1 = SelectStatement::builder()
        .select(SelectItem::expression(DomainExpression::column("id")))
        .select(SelectItem::expression_with_alias(
            DomainExpression::Binary {
                left: Box::new(DomainExpression::column("total")),
                op: BinaryOperator::Multiply,
                right: Box::new(DomainExpression::literal(LiteralValue::Number(
                    "2".to_string(),
                ))),
            },
            "double_total",
        ))
        .from_table_with_alias("orders", "o")
        .build()
        .unwrap();

    // Level 2: SELECT * FROM (...) AS t1 WHERE double_total > 100
    let level2 = SelectStatement::builder()
        .select(SelectItem::star())
        .from_subquery(QueryExpression::Select(Box::new(level1)), "t1")
        .where_clause(DomainExpression::Binary {
            left: Box::new(DomainExpression::column("double_total")),
            op: BinaryOperator::GreaterThan,
            right: Box::new(DomainExpression::literal(LiteralValue::Number(
                "100".to_string(),
            ))),
        })
        .build()
        .unwrap();

    // Level 3: SELECT id, SUM(double_total) AS total_sum FROM (...) AS t2 GROUP BY id
    let level3 = SelectStatement::builder()
        .select(SelectItem::expression(DomainExpression::column("id")))
        .select(SelectItem::expression_with_alias(
            DomainExpression::function("SUM", vec![DomainExpression::column("double_total")]),
            "total_sum",
        ))
        .from_subquery(QueryExpression::Select(Box::new(level2)), "t2")
        .group_by(vec![DomainExpression::column("id")])
        .build()
        .unwrap();

    let stmt = SqlStatement::simple(QueryExpression::Select(Box::new(level3)));

    let sql = generator.generate_statement(&stmt).unwrap();

    // Check for the nested structure
    assert!(sql.contains("SELECT id, SUM(double_total) AS total_sum"));
    assert!(sql.contains("GROUP BY id"));
    assert!(sql.contains("WHERE double_total > 100"));
    assert!(sql.contains("SELECT id, total * 2 AS double_total"));
    assert!(sql.contains("FROM orders AS o"));
    assert!(sql.contains(") AS t1"));
    assert!(sql.contains(") AS t2"));
}
