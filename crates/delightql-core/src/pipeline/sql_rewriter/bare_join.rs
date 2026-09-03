// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// bare_join.rs — Legalize condition-less joins for dialects that reject them.
//
// A DQL join with no join predicate (a cartesian product) lowers to
// `JoinCondition::Cartesian`, which renders as `A INNER JOIN B` with no ON
// clause. SQLite and MySQL accept that spelling (implicit cross join);
// postgres, duckdb and sqlserver reject it as a syntax error. A legal
// equivalent always exists — CROSS JOIN for inner joins, ON TRUE for outer
// joins — so this legalization is total and never diagnoses.

use crate::pipeline::ast_refined::LiteralValue;
use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::{
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
            if matches!(join_condition, JoinCondition::Cartesian) {
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
    use crate::names::{Addressing, Registry};
    use crate::pipeline::sql_ast::{QueryExpression, SelectItem, SelectStatement};

    struct Fixture {
        a: crate::names::ScopeId,
        b: crate::names::ScopeId,
        t: crate::names::ScopeId,
        x: crate::names::ColId,
    }

    impl Fixture {
        fn new() -> Self {
            let identities = Registry::new(&[]);
            let make_scope = |name: &str| {
                let spelling = identities.intern(name, false);
                let entity = identities.mint_entity(spelling);
                identities.resolved_access_scope(entity, spelling)
            };
            let a = make_scope("a");
            let b = make_scope("b");
            let t = make_scope("t");
            let spelling = identities.intern("x", false);
            let x = identities.sql_column(a, Some(spelling), Addressing::Published);
            Self { a, b, t, x }
        }

        fn bare_join(&self, join_type: JoinType) -> TableExpression {
            TableExpression::Join {
                left: Box::new(TableExpression::Scope(self.a)),
                right: Box::new(TableExpression::Scope(self.b)),
                join_type,
                join_condition: JoinCondition::Cartesian,
            }
        }
    }

    /// A fixture's statements go through the same door production's do. A
    /// star names nothing, so the heading it publishes is empty.
    fn select_from(from: TableExpression, at: crate::names::ScopeId) -> SelectStatement {
        (SelectStatement::builder()
            .select(SelectItem::star_over_nothing())
            .from_tables(vec![from]))
        .standing_at(at)
        .map_err(crate::error::DelightQLError::parse_error)
        .expect("a star publishes no heading of its own")
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
        let f = Fixture::new();
        let stmt = legalize_bare_joins(query_stmt(select_from(f.bare_join(JoinType::Inner), f.t)));
        let (join_type, condition) = first_join(&stmt);
        assert_eq!(join_type, &JoinType::Cross);
        assert_eq!(condition, &JoinCondition::Cartesian);
    }

    #[test]
    fn conditioned_join_untouched() {
        let f = Fixture::new();
        let on = JoinCondition::On(DomainExpression::Column(f.x));
        let stmt = legalize_bare_joins(query_stmt(select_from(
            TableExpression::Join {
                left: Box::new(TableExpression::Scope(f.a)),
                right: Box::new(TableExpression::Scope(f.b)),
                join_type: JoinType::Inner,
                join_condition: on.clone(),
            },
            f.t,
        )));
        let (join_type, condition) = first_join(&stmt);
        assert_eq!(join_type, &JoinType::Inner);
        assert_eq!(condition, &on);
    }

    #[test]
    fn bare_left_join_gets_on_true() {
        let f = Fixture::new();
        let stmt = legalize_bare_joins(query_stmt(select_from(f.bare_join(JoinType::Left), f.t)));
        let (join_type, condition) = first_join(&stmt);
        assert_eq!(join_type, &JoinType::Left);
        assert_eq!(
            condition,
            &JoinCondition::On(DomainExpression::Literal(LiteralValue::Boolean(true)))
        );
    }

    #[test]
    fn nested_join_is_reached() {
        let f = Fixture::new();
        let inner = select_from(f.bare_join(JoinType::Inner), f.t);
        let outer = (SelectStatement::builder()
            .select(SelectItem::star_over_nothing())
            .from_tables(vec![TableExpression::Scope(f.t)])
            .where_clause(DomainExpression::Exists {
                not: false,
                query: Box::new(QueryExpression::Select(Box::new(inner))),
            }))
        .standing_at(f.t)
        .map_err(crate::error::DelightQLError::parse_error)
        .expect("a star publishes no heading of its own");
        let stmt = legalize_bare_joins(query_stmt(outer));
        let SqlStatement::Query {
            query: QueryExpression::Select(select),
            ..
        } = &stmt
        else {
            panic!("expected query");
        };
        let Some(DomainExpression::Exists { query, .. }) = select.where_clause() else {
            panic!("expected EXISTS");
        };
        let QueryExpression::Select(inner) = query.as_ref() else {
            panic!("expected nested SELECT");
        };
        let TableExpression::Join { join_type, .. } = &inner.from().unwrap()[0] else {
            panic!("expected nested join");
        };
        assert_eq!(join_type, &JoinType::Cross);
    }
}
