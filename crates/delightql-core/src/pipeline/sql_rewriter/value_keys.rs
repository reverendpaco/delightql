// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// value_keys.rs — An ordering or grouping key is a value, never a position.
//
// Every key the SQL AST holds is a value: `OrderTerm` and the grouping list
// have no positional reading, and nothing in the language lowers to one (an
// authored position such as `|2|` resolves to the column it names). SQL
// reads an integer literal key by position instead: `ORDER BY 2` sorts by
// the second output column and `GROUP BY 7` names the seventh, and neither
// parentheses nor a sign make it a value. Some targets refuse every other
// literal key.
//
// A literal distinguishes no rows, and that is the whole of what it means as
// a key:
// - an ordering key that is a literal orders nothing and is dropped;
// - a grouping key that is a literal partitions nothing and is dropped. A
//   grouping whose every key is a literal still groups: it has one group
//   when its input has a row and none otherwise, whatever its result
//   columns. The statement keeps exactly that in the form the target reads
//   so (`SqlDialect::whole_input_grouping`); on SQLite that form is a
//   `GROUP BY NULL`, the one literal key this pass writes.
//
// A constant that computes (`0 + 2`) is read as its value and is left as
// written. A window's PARTITION BY and ORDER BY are not select-list clauses:
// the standard reads their keys as values, and so does SQLite.

use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::{walk, QueryExpression, SqlStatement, WholeInputGrouping};

/// Drop every literal ordering and grouping key in the statement.
pub fn legalize_value_keys(stmt: &mut SqlStatement, dialect: SqlDialect) {
    struct Keys(WholeInputGrouping);
    impl walk::SqlVisitorMut for Keys {
        fn query(&mut self, q: &mut QueryExpression) {
            if let QueryExpression::Select(select) = q {
                select.retain_order_terms(|term| !term.expr().is_literal());
                select.retain_group_keys(|key| !key.is_literal(), self.0);
            }
        }
    }
    walk::visit_mut(stmt, &mut Keys(dialect.whole_input_grouping()));
}

#[cfg(test)]
mod tests {
    use super::legalize_value_keys;
    use crate::names::{Addressing, Registry};
    use crate::pipeline::asts::core::LiteralValue;
    use crate::pipeline::asts::core::NumericLiteral;
    use crate::pipeline::generator::SqlDialect;
    use crate::pipeline::sql_ast::{
        DomainExpression as E, OrderDirection, OrderTerm, QueryExpression, SelectItem,
        SelectStatement, SqlStatement, TableExpression, UnaryOperator,
    };

    struct Fixture {
        registry: Registry,
        source: crate::names::ScopeId,
        a: crate::names::ColId,
        b: crate::names::ColId,
    }

    fn fixture() -> Fixture {
        let registry = Registry::new(&[]);
        let source = registry.anonymous_scope(None);
        let a = registry.sql_column(source, None, Addressing::Published);
        let b = registry.sql_column(source, None, Addressing::Published);
        Fixture {
            registry,
            source,
            a,
            b,
        }
    }

    fn int(value: i64) -> E {
        E::literal(LiteralValue::integer(value))
    }

    fn statement(
        fixture: &Fixture,
        build: impl FnOnce(
            crate::pipeline::sql_ast::SelectBuilder,
        ) -> crate::pipeline::sql_ast::SelectBuilder,
    ) -> SqlStatement {
        let at = fixture.registry.anonymous_scope(None);
        let slot = fixture.registry.sql_column(at, None, Addressing::Published);
        let select = build(
            SelectStatement::builder()
                .select(SelectItem::Publishing {
                    expr: E::Column(fixture.a),
                    slot,
                    printed: true,
                })
                .from_tables(vec![TableExpression::Scope(fixture.source)]),
        )
        .standing_at(at)
        .expect("the test statement builds");
        SqlStatement::Query {
            with_clause: None,
            query: QueryExpression::Select(Box::new(select)),
        }
    }

    fn select(statement: &SqlStatement) -> &SelectStatement {
        let SqlStatement::Query {
            query: QueryExpression::Select(select),
            ..
        } = statement
        else {
            panic!("a select statement");
        };
        select
    }

    #[test]
    fn every_literal_spelling_is_a_literal_and_only_integers_and_booleans_read_as_positions() {
        let f = fixture();
        let positional = [
            int(2),
            int(0),
            int(-1),
            E::literal(LiteralValue::Boolean(true)),
            E::Parens(Box::new(int(2))),
            E::Unary {
                op: UnaryOperator::Plus,
                expr: Box::new(int(2)),
            },
            E::Unary {
                op: UnaryOperator::Minus,
                expr: Box::new(E::Parens(Box::new(int(2)))),
            },
        ];
        let valued = [
            E::literal(LiteralValue::Null),
            E::literal(LiteralValue::String("2".into())),
            E::literal(LiteralValue::Number(NumericLiteral::from_decimal_spelling(
                "2.0".into(),
            ))),
        ];
        for literal in positional.iter() {
            assert!(literal.is_literal(), "{literal:?}");
            assert!(literal.reads_as_position(), "{literal:?}");
        }
        for literal in valued.iter() {
            assert!(literal.is_literal(), "{literal:?}");
            assert!(!literal.reads_as_position(), "{literal:?}");
        }
        for value in [
            E::add(int(0), int(2)),
            E::cast(int(2), "integer"),
            E::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(int(1)),
            },
            E::Column(f.a),
            E::Parens(Box::new(E::Column(f.b))),
        ] {
            assert!(!value.is_literal(), "{value:?}");
            assert!(!value.reads_as_position(), "{value:?}");
        }
    }

    #[test]
    fn a_literal_ordering_key_orders_nothing_and_leaves_the_others() {
        let f = fixture();
        let mut stmt = statement(&f, |s| {
            s.order_by(OrderTerm::new(int(2), None))
                .order_by(OrderTerm::new(E::Column(f.a), Some(OrderDirection::Desc)))
                .order_by(OrderTerm::new(E::add(int(0), int(2)), None))
        });
        legalize_value_keys(&mut stmt, SqlDialect::SQLite);
        assert_eq!(
            select(&stmt).order_by(),
            Some(
                &[
                    OrderTerm::new(E::Column(f.a), Some(OrderDirection::Desc)),
                    OrderTerm::new(E::add(int(0), int(2)), None),
                ][..]
            )
        );
    }

    #[test]
    fn an_ordering_of_literals_alone_is_no_ordering() {
        let f = fixture();
        let mut stmt = statement(&f, |s| {
            s.order_by(OrderTerm::new(int(2), Some(OrderDirection::Desc)))
                .order_by(OrderTerm::new(
                    E::literal(LiteralValue::String("x".into())),
                    None,
                ))
        });
        legalize_value_keys(&mut stmt, SqlDialect::SQLite);
        assert_eq!(select(&stmt).order_by(), None);
    }

    #[test]
    fn a_literal_grouping_key_beside_a_column_is_dropped_and_the_grouping_stays() {
        let f = fixture();
        let mut stmt = statement(&f, |s| s.group_by(vec![int(2), E::Column(f.a)]));
        legalize_value_keys(&mut stmt, SqlDialect::SQLite);
        let select = select(&stmt);
        assert_eq!(select.group_by(), Some(&[E::Column(f.a)][..]));
        assert_eq!(select.having(), None);
    }

    #[test]
    fn a_grouping_of_literals_alone_keeps_a_shared_key_on_sqlite() {
        let f = fixture();
        let having = E::Column(f.b).gt(int(1));
        let mut stmt = statement(&f, |s| {
            s.group_by(vec![int(7), E::literal(LiteralValue::String("k".into()))])
                .having(having.clone())
        });
        legalize_value_keys(&mut stmt, SqlDialect::SQLite);
        let select = select(&stmt);
        assert_eq!(
            select.group_by(),
            Some(&[E::literal(LiteralValue::Null)][..]),
            "one group, whatever the result columns: no aggregate is needed beside the key"
        );
        assert_eq!(select.having(), Some(&having));
        assert!(!select.group_by().unwrap()[0].reads_as_position());
    }

    #[test]
    fn a_grouping_of_literals_alone_keeps_one_inhabited_group_by_having_on_the_standard_targets() {
        let f = fixture();
        let count_is_positive = || E::function("count", vec![E::Star]).gt(int(0));
        for dialect in [
            SqlDialect::PostgreSQL,
            SqlDialect::DuckDB,
            SqlDialect::MySQL,
            SqlDialect::SqlServer,
        ] {
            let mut bare = statement(&f, |s| s.group_by(vec![int(7)]));
            legalize_value_keys(&mut bare, dialect);
            assert_eq!(select(&bare).group_by(), None, "{dialect:?}");
            assert_eq!(
                select(&bare).having(),
                Some(&count_is_positive()),
                "{dialect:?}"
            );

            let having = E::Column(f.b).gt(int(1));
            let mut filtered = statement(&f, |s| {
                s.group_by(vec![int(7), E::literal(LiteralValue::Null)])
                    .having(having.clone())
            });
            legalize_value_keys(&mut filtered, dialect);
            assert_eq!(select(&filtered).group_by(), None, "{dialect:?}");
            assert_eq!(
                select(&filtered).having(),
                Some(&E::and(vec![count_is_positive(), having])),
                "{dialect:?}"
            );
        }
    }

    #[test]
    fn a_statement_without_literal_keys_is_untouched() {
        let f = fixture();
        let build = |s: crate::pipeline::sql_ast::SelectBuilder| {
            s.group_by(vec![E::Column(f.a), E::add(int(7), int(0))])
                .having(E::Column(f.b).gt(int(1)))
                .order_by(OrderTerm::new(E::Column(f.b), None))
        };
        let before = statement(&f, build);
        let mut after = before.clone();
        legalize_value_keys(&mut after, SqlDialect::SQLite);
        assert_eq!(select(&after).group_by(), select(&before).group_by());
        assert_eq!(select(&after).having(), select(&before).having());
        assert_eq!(select(&after).order_by(), select(&before).order_by());
    }
}
