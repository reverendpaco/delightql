// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// bare_join.rs — Legalize condition-less joins for dialects that reject them.
//
// A DQL join with no join predicate (a cartesian product) lowers to
// `JoinCondition::Natural`, which renders as `A INNER JOIN B` with no ON
// clause. SQLite and MySQL accept that spelling (implicit cross join);
// postgres, duckdb and sqlserver reject it as a syntax error. A legal
// equivalent always exists — CROSS JOIN for inner joins, ON TRUE for outer
// joins — so this legalization is total and never diagnoses.

use crate::pipeline::ast_refined::LiteralValue;
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::{
    walk, DomainExpression, JoinCondition, JoinType, SqlStatement, TableExpression,
};

/// Does this dialect reject `A INNER JOIN B` with no ON clause?
pub fn needs_legalization(dialect: SqlDialect) -> bool {
    match dialect {
        SqlDialect::PostgreSQL | SqlDialect::DuckDB | SqlDialect::SqlServer => true,
        SqlDialect::SQLite | SqlDialect::MySQL => false,
    }
}

/// Rewrite every condition-less join into its legal equivalent.
pub fn legalize_bare_joins(mut stmt: SqlStatement) -> SqlStatement {
    walk::visit_tables_mut(&mut stmt, &mut |table| {
        if let TableExpression::Join {
            join_type,
            join_condition,
            ..
        } = table
        {
            if matches!(join_condition, JoinCondition::Natural) {
                match join_type {
                    JoinType::Inner => *join_type = JoinType::Cross,
                    // CROSS JOIN takes no condition — already legal.
                    JoinType::Cross => {}
                    // Outer joins keep their type; ON TRUE reproduces the
                    // condition-less semantics.
                    JoinType::Left | JoinType::Right | JoinType::Full => {
                        *join_condition = JoinCondition::On(DomainExpression::Literal(
                            LiteralValue::Boolean(true),
                        ));
                    }
                }
            }
        }
    });
    stmt
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::sql_ast_v3::{QueryExpression, SelectItem, SelectStatement};

    fn table(name: &str) -> TableExpression {
        TableExpression::Table {
            schema: None,
            name: name.to_string(),
            alias: None,
        }
    }

    fn bare_join(join_type: JoinType) -> TableExpression {
        TableExpression::Join {
            left: Box::new(table("a")),
            right: Box::new(table("b")),
            join_type,
            join_condition: JoinCondition::Natural,
        }
    }

    fn select_from(from: TableExpression) -> SelectStatement {
        SelectStatement::builder()
            .select(SelectItem::star())
            .from_tables(vec![from])
            .build()
            .unwrap()
    }

    fn query_stmt(select: SelectStatement) -> SqlStatement {
        SqlStatement::Query {
            with_clause: None,
            query: QueryExpression::Select(Box::new(select)),
        }
    }

    fn first_join(stmt: &SqlStatement) -> (&JoinType, &JoinCondition) {
        let SqlStatement::Query {
            query: QueryExpression::Select(select),
            ..
        } = stmt
        else {
            panic!("expected plain SELECT statement");
        };
        let TableExpression::Join {
            join_type,
            join_condition,
            ..
        } = &select.from().unwrap()[0]
        else {
            panic!("expected join in FROM");
        };
        (join_type, join_condition)
    }

    #[test]
    fn dialect_gate() {
        assert!(needs_legalization(SqlDialect::PostgreSQL));
        assert!(needs_legalization(SqlDialect::DuckDB));
        assert!(needs_legalization(SqlDialect::SqlServer));
        assert!(!needs_legalization(SqlDialect::SQLite));
        assert!(!needs_legalization(SqlDialect::MySQL));
    }

    #[test]
    fn inner_bare_join_becomes_cross() {
        let stmt = query_stmt(select_from(bare_join(JoinType::Inner)));
        let stmt = legalize_bare_joins(stmt);
        let (join_type, condition) = first_join(&stmt);
        assert_eq!(join_type, &JoinType::Cross);
        assert_eq!(condition, &JoinCondition::Natural);
    }

    #[test]
    fn conditioned_join_untouched() {
        let on = JoinCondition::On(DomainExpression::column("x"));
        let stmt = query_stmt(select_from(TableExpression::Join {
            left: Box::new(table("a")),
            right: Box::new(table("b")),
            join_type: JoinType::Inner,
            join_condition: on.clone(),
        }));
        let stmt = legalize_bare_joins(stmt);
        let (join_type, condition) = first_join(&stmt);
        assert_eq!(join_type, &JoinType::Inner);
        assert_eq!(condition, &on);
    }

    #[test]
    fn bare_left_join_gets_on_true() {
        let stmt = query_stmt(select_from(bare_join(JoinType::Left)));
        let stmt = legalize_bare_joins(stmt);
        let (join_type, condition) = first_join(&stmt);
        assert_eq!(join_type, &JoinType::Left);
        assert_eq!(
            condition,
            &JoinCondition::On(DomainExpression::Literal(LiteralValue::Boolean(true)))
        );
    }

    #[test]
    fn bare_join_inside_exists_subquery_is_reached() {
        let inner = select_from(bare_join(JoinType::Inner));
        let outer = SelectStatement::builder()
            .select(SelectItem::star())
            .from_tables(vec![table("t")])
            .where_clause(DomainExpression::Exists {
                not: false,
                query: Box::new(QueryExpression::Select(Box::new(inner))),
            })
            .build()
            .unwrap();
        let stmt = legalize_bare_joins(query_stmt(outer));

        let SqlStatement::Query {
            query: QueryExpression::Select(select),
            ..
        } = &stmt
        else {
            panic!("expected plain SELECT statement");
        };
        let Some(DomainExpression::Exists { query, .. }) = select.where_clause() else {
            panic!("expected EXISTS in WHERE");
        };
        let QueryExpression::Select(inner) = query.as_ref() else {
            panic!("expected SELECT inside EXISTS");
        };
        let TableExpression::Join { join_type, .. } = &inner.from().unwrap()[0] else {
            panic!("expected join in inner FROM");
        };
        assert_eq!(join_type, &JoinType::Cross);
    }

    #[test]
    fn bare_join_inside_statement_level_cte_is_reached() {
        use crate::pipeline::sql_ast_v3::Cte;
        let cte_query = QueryExpression::Select(Box::new(select_from(bare_join(JoinType::Inner))));
        let stmt = SqlStatement::Query {
            with_clause: Some(vec![Cte::new("c", cte_query)]),
            query: QueryExpression::Select(Box::new(select_from(table("c")))),
        };
        let stmt = legalize_bare_joins(stmt);

        let SqlStatement::Query {
            with_clause: Some(ctes),
            ..
        } = &stmt
        else {
            panic!("expected WITH clause");
        };
        let QueryExpression::Select(select) = ctes[0].query() else {
            panic!("expected SELECT in CTE");
        };
        let TableExpression::Join { join_type, .. } = &select.from().unwrap()[0] else {
            panic!("expected join in CTE FROM");
        };
        assert_eq!(join_type, &JoinType::Cross);
    }
}
