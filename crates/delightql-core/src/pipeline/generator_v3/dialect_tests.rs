// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Dialect-spelling tests: prove the seeded `dialect_render` rows reproduce
//! the spellings the old `match dialect` arms produced, and that a missing
//! row falls back to the canonical (SQLite) code default.

use super::{SqlDialect, SqlGenerator};
use crate::pipeline::ast_refined::LiteralValue;
use crate::pipeline::dialect_pack::DialectPack;
use crate::pipeline::sql_ast_v3::{BinaryOperator, DomainExpression};
use std::sync::Arc;

/// A generator targeting `dialect`, carrying the pack loaded from the real
/// bootstrap schema (so these tests exercise the actual seed rows).
fn seeded_generator(dialect: SqlDialect) -> SqlGenerator {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
    let pack = DialectPack::load(&conn).unwrap();
    SqlGenerator::new()
        .with_dialect(dialect)
        .with_dialect_pack(Arc::new(pack))
}

fn not_equal_expr() -> DomainExpression {
    DomainExpression::Binary {
        left: Box::new(DomainExpression::column("a")),
        op: BinaryOperator::NotEqual,
        right: Box::new(DomainExpression::column("b")),
    }
}

fn concat_expr() -> DomainExpression {
    DomainExpression::Binary {
        left: Box::new(DomainExpression::column("a")),
        op: BinaryOperator::Concatenate,
        right: Box::new(DomainExpression::column("b")),
    }
}

fn bool_expr() -> DomainExpression {
    DomainExpression::Binary {
        left: Box::new(DomainExpression::column("active")),
        op: BinaryOperator::Equal,
        right: Box::new(DomainExpression::literal(LiteralValue::Boolean(true))),
    }
}

fn quoted_ident_expr() -> DomainExpression {
    // "order" is a reserved word, so it must be quoted in every dialect.
    DomainExpression::column("order")
}

#[test]
fn not_equal_spellings() {
    let render = |d| seeded_generator(d).render_expression(&not_equal_expr()).unwrap();
    assert_eq!(render(SqlDialect::SQLite), "a != b");
    assert_eq!(render(SqlDialect::PostgreSQL), "a != b");
    assert_eq!(render(SqlDialect::MySQL), "a <> b");
    assert_eq!(render(SqlDialect::SqlServer), "a <> b");
}

#[test]
fn concatenate_spellings() {
    let render = |d| seeded_generator(d).render_expression(&concat_expr()).unwrap();
    assert_eq!(render(SqlDialect::SQLite), "a || b");
    assert_eq!(render(SqlDialect::PostgreSQL), "a || b");
    assert_eq!(render(SqlDialect::MySQL), "a CONCAT b");
    assert_eq!(render(SqlDialect::SqlServer), "a + b");
}

#[test]
fn boolean_literal_spellings() {
    let render = |d| seeded_generator(d).render_expression(&bool_expr()).unwrap();
    assert_eq!(render(SqlDialect::SQLite), "active = 1");
    assert_eq!(render(SqlDialect::MySQL), "active = 1");
    assert_eq!(render(SqlDialect::PostgreSQL), "active = TRUE");
    assert_eq!(render(SqlDialect::SqlServer), "active = TRUE");
}

#[test]
fn identifier_quoting_spellings() {
    let render = |d| {
        seeded_generator(d)
            .render_expression(&quoted_ident_expr())
            .unwrap()
    };
    assert_eq!(render(SqlDialect::SQLite), "\"order\"");
    assert_eq!(render(SqlDialect::PostgreSQL), "\"order\"");
    assert_eq!(render(SqlDialect::MySQL), "`order`");
    assert_eq!(render(SqlDialect::SqlServer), "[order]");
}

fn fn_call(name: &str, args: Vec<DomainExpression>) -> DomainExpression {
    DomainExpression::Function {
        name: name.to_string(),
        args,
        distinct: false,
    }
}

#[test]
fn function_rename_rule() {
    // duckdb: json_extract -> json_extract_string (bare-name body keeps shape)
    let generator = seeded_generator(SqlDialect::DuckDB);
    let expr = fn_call(
        "json_extract",
        vec![
            DomainExpression::column("j"),
            DomainExpression::literal(LiteralValue::String("$.a".into())),
        ],
    );
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "json_extract_string(j, '$.a')"
    );
    // sqlite: no rule, canonical name kept
    let generator = seeded_generator(SqlDialect::SQLite);
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "json_extract(j, '$.a')"
    );
}

#[test]
fn function_template_rule_variadic() {
    // postgres: JSON_OBJECT(k,v,...) -> json_build_object({*}); the lookup
    // key is lowercased, so the AST's upper-case spelling still matches.
    let generator = seeded_generator(SqlDialect::PostgreSQL);
    let expr = fn_call(
        "JSON_OBJECT",
        vec![
            DomainExpression::literal(LiteralValue::String("a".into())),
            DomainExpression::column("x"),
            DomainExpression::literal(LiteralValue::String("b".into())),
            DomainExpression::column("y"),
        ],
    );
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "json_build_object('a', x, 'b', y)"
    );
}

#[test]
fn function_template_rejects_distinct() {
    let generator = seeded_generator(SqlDialect::PostgreSQL);
    let expr = DomainExpression::Function {
        name: "JSON_OBJECT".to_string(),
        args: vec![DomainExpression::column("x")],
        distinct: true,
    };
    assert!(generator.render_expression(&expr).is_err());
}

#[test]
fn function_rename_keeps_distinct() {
    // A bare-name rule preserves the call shape, DISTINCT included.
    let generator = seeded_generator(SqlDialect::PostgreSQL);
    let expr = DomainExpression::Function {
        name: "json_group_object".to_string(),
        args: vec![DomainExpression::column("k"), DomainExpression::column("v")],
        distinct: true,
    };
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "json_object_agg(DISTINCT k, v)"
    );
}

#[test]
fn internal_raw_json_extract_is_exempt_from_scalar_respell() {
    // The provenance split: fn.json_extract rows (duckdb -> _string) must
    // NOT touch __dql_json_extract_raw, which spells canonically as
    // json_extract on every dialect (its own key, no seeded rows).
    let expr = fn_call(
        crate::pipeline::naming::INTERNAL_JSON_EXTRACT_RAW,
        vec![
            DomainExpression::column("j"),
            DomainExpression::literal(LiteralValue::String("$.scripts".into())),
        ],
    );
    // (postgres respells it too — but via its OWN row, fn.__dql_json_extract_raw,
    // to the jsonb flavor; see rust_handler_rules_fire_end_to_end.)
    for dialect in [SqlDialect::SQLite, SqlDialect::DuckDB] {
        assert_eq!(
            seeded_generator(dialect).render_expression(&expr).unwrap(),
            "json_extract(j, '$.scripts')",
            "internal name must never leak and never hit fn.json_extract rows ({dialect:?})"
        );
    }
}

#[test]
fn rust_handler_rules_fire_end_to_end() {
    let generator = seeded_generator(SqlDialect::PostgreSQL);
    // user-facing scalar read -> text flavor
    let expr = fn_call(
        "json_extract",
        vec![
            DomainExpression::column("j"),
            DomainExpression::literal(LiteralValue::String("$.a.b".into())),
        ],
    );
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "(CAST(j AS jsonb) #>> '{a,b}')"
    );
    // native-json provenance -> jsonb flavor
    let expr = fn_call(
        crate::pipeline::naming::INTERNAL_JSON_EXTRACT_RAW,
        vec![
            DomainExpression::column("j"),
            DomainExpression::literal(LiteralValue::String("$.scripts".into())),
        ],
    );
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "(CAST(j AS jsonb) #> '{scripts}')"
    );
    // group_concat: 1-arg synthesizes the default separator; DISTINCT flows
    let expr = DomainExpression::Function {
        name: "GROUP_CONCAT".to_string(),
        args: vec![DomainExpression::column("name")],
        distinct: true,
    };
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "string_agg(DISTINCT CAST(name AS text), ',')"
    );
    // sqlite untouched by all three
    let generator = seeded_generator(SqlDialect::SQLite);
    let expr = fn_call(
        "json_extract",
        vec![
            DomainExpression::column("j"),
            DomainExpression::literal(LiteralValue::String("$.a.b".into())),
        ],
    );
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "json_extract(j, '$.a.b')"
    );
}

/// A generator whose pack went through the FULL bootstrap path including
/// bin_sync (which seeds the per-functor `dialect_form_rule` rows).
fn synced_generator(dialect: SqlDialect) -> SqlGenerator {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
    // bin_sync auto-enlists universal cartridges into 'main', which system
    // init creates via the namespace bootstrap — replicate that here.
    crate::import::create_bootstrap_namespaces(&conn).unwrap();
    let mut registry = crate::bin_cartridge::registry::BinCartridgeRegistry::new();
    registry.register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());
    crate::bootstrap::sync_bin_cartridges_to_bootstrap(&conn, &registry).unwrap();
    let pack = DialectPack::load(&conn).unwrap();
    SqlGenerator::new()
        .with_dialect(dialect)
        .with_dialect_pack(Arc::new(pack))
        .with_bin_registry(Arc::new(registry))
}

fn like_predicate(negated: bool) -> DomainExpression {
    DomainExpression::PredicateRewrite {
        name: "like".to_string(),
        args: vec![
            DomainExpression::column("email"),
            DomainExpression::literal(LiteralValue::String("%@GMAIL.com".into())),
        ],
        negated,
    }
}

#[test]
fn sigma_form_rule_fires_per_dialect() {
    // canonical (sqlite): the bin entity's code lowering
    assert_eq!(
        synced_generator(SqlDialect::SQLite)
            .render_expression(&like_predicate(false))
            .unwrap(),
        "email LIKE '%@GMAIL.com'"
    );
    // postgres + duckdb: the seeded ILIKE fidelity rule (sqlite LIKE is
    // case-insensitive; theirs isn't — ILIKE restores canonical semantics)
    for dialect in [SqlDialect::PostgreSQL, SqlDialect::DuckDB] {
        assert_eq!(
            synced_generator(dialect)
                .render_expression(&like_predicate(false))
                .unwrap(),
            "email ILIKE '%@GMAIL.com'",
            "{dialect:?}"
        );
    }
    // negation wraps the template
    assert_eq!(
        synced_generator(SqlDialect::PostgreSQL)
            .render_expression(&like_predicate(true))
            .unwrap(),
        "NOT (email ILIKE '%@GMAIL.com')"
    );
    assert_eq!(
        synced_generator(SqlDialect::SQLite)
            .render_expression(&like_predicate(true))
            .unwrap(),
        "email NOT LIKE '%@GMAIL.com'"
    );
    // +between has no rule anywhere: canonical on every dialect
    let between = DomainExpression::PredicateRewrite {
        name: "between".to_string(),
        args: vec![
            DomainExpression::column("age"),
            DomainExpression::literal(LiteralValue::Number("18".into())),
            DomainExpression::literal(LiteralValue::Number("65".into())),
        ],
        negated: false,
    };
    for dialect in [SqlDialect::SQLite, SqlDialect::PostgreSQL] {
        assert_eq!(
            synced_generator(dialect).render_expression(&between).unwrap(),
            "age BETWEEN 18 AND 65",
            "{dialect:?}"
        );
    }
}

#[test]
fn cast_type_spellings() {
    let cast_expr = DomainExpression::cast(DomainExpression::column("age"), "real");
    let render = |d| seeded_generator(d).render_expression(&cast_expr).unwrap();
    // canonical = uppercased DQL type word
    assert_eq!(render(SqlDialect::SQLite), "CAST(age AS REAL)");
    // sqlite REAL is an 8-byte float → per-target type.real rows
    assert_eq!(render(SqlDialect::PostgreSQL), "CAST(age AS DOUBLE PRECISION)");
    assert_eq!(render(SqlDialect::DuckDB), "CAST(age AS DOUBLE)");
    // a type with no rows spells canonically everywhere
    let int_cast = DomainExpression::cast(DomainExpression::column("x"), "integer");
    for dialect in [SqlDialect::SQLite, SqlDialect::PostgreSQL, SqlDialect::DuckDB] {
        assert_eq!(
            seeded_generator(dialect).render_expression(&int_cast).unwrap(),
            "CAST(x AS INTEGER)",
            "{dialect:?}"
        );
    }
}

#[test]
fn empty_pack_falls_back_to_canonical() {
    // Defaults-in-code (DESIGN §7.10): with no pack rows, every dialect
    // renders canonical — proving the table is a patch layer, never the
    // sole source of a spelling.
    let generator = SqlGenerator::new().with_dialect(SqlDialect::PostgreSQL);
    assert_eq!(
        generator.render_expression(&bool_expr()).unwrap(),
        "active = 1"
    );
    let generator = SqlGenerator::new().with_dialect(SqlDialect::SqlServer);
    assert_eq!(
        generator.render_expression(&not_equal_expr()).unwrap(),
        "a != b"
    );
}

/// SELECT * FROM (SELECT 1) AS t_1 LEFT JOIN <tvf>(t_1.j) AS _narrow_2 ON 1 —
/// the shape the transformer's array-iteration sites emit.
fn tvf_join_stmt(function: &str) -> crate::pipeline::sql_ast_v3::SqlStatement {
    use crate::pipeline::sql_ast_v3::*;
    let source = SelectStatement::builder()
        .select(SelectItem::expression_with_alias(
            DomainExpression::literal(
                crate::pipeline::asts::core::literals::LiteralValue::Number("1".into()),
            ),
            "j",
        ))
        .build()
        .unwrap();
    let tvf = TableExpression::TVF {
        schema: None,
        function: function.to_string(),
        arguments: vec![TvfArgument::QualifiedRef {
            qualifier: "t_1".to_string(),
            column: "j".to_string(),
        }],
        alias: Some("_narrow_2".to_string()),
    };
    let joined = TableExpression::Join {
        left: Box::new(TableExpression::subquery(
            QueryExpression::Select(Box::new(source)),
            "t_1",
        )),
        right: Box::new(tvf),
        join_type: JoinType::Left,
        join_condition: JoinCondition::On(DomainExpression::literal(
            crate::pipeline::asts::core::literals::LiteralValue::Boolean(true),
        )),
    };
    let select = SelectStatement::builder()
        .select(SelectItem::star())
        .from_tables(vec![joined])
        .build()
        .unwrap();
    SqlStatement::with_ctes(None, QueryExpression::Select(Box::new(select)))
}

#[test]
fn internal_json_each_array_spells_canonically_off_postgres() {
    // The internal array-each name never leaks: sqlite/duckdb (no tvf rows)
    // fall back to the canonical json_each spelling.
    let stmt = tvf_join_stmt(crate::pipeline::naming::INTERNAL_JSON_EACH_ARRAY);
    for dialect in [SqlDialect::SQLite, SqlDialect::DuckDB] {
        let sql = seeded_generator(dialect).generate_statement(&stmt).unwrap();
        assert!(
            sql.contains("json_each(t_1.j) AS _narrow_2"),
            "expected canonical json_each spelling on {dialect:?}, got: {sql}"
        );
        assert!(!sql.contains("__dql"), "internal name leaked ({dialect:?}): {sql}");
    }
}

#[test]
fn internal_json_each_array_becomes_lateral_on_postgres() {
    // pg's json_each is object-only; the array-provenance TVF renders as a
    // LATERAL derived table over jsonb_array_elements WITH ORDINALITY, with
    // sqlite-compatible column names (key 0-based, value) and the alias
    // appended by code.
    let stmt = tvf_join_stmt(crate::pipeline::naming::INTERNAL_JSON_EACH_ARRAY);
    let sql = seeded_generator(SqlDialect::PostgreSQL)
        .generate_statement(&stmt)
        .unwrap();
    assert!(
        sql.contains(
            "LEFT JOIN LATERAL (SELECT e.ordinality - 1 AS key, e.value AS value \
             FROM jsonb_array_elements(CAST(t_1.j AS jsonb)) WITH ORDINALITY AS e) \
             AS _narrow_2 ON TRUE"
        ),
        "got: {sql}"
    );
    assert!(!sql.contains("__dql"), "internal name leaked: {sql}");
}

#[test]
fn user_facing_json_each_tvf_is_not_respelled() {
    // The plain (user-facing, dynamic-document) json_each TVF has no pg row
    // yet — it spells canonically everywhere. Phase 2 (polymorphic shim)
    // owns it; this pins the phase-1 boundary.
    let stmt = tvf_join_stmt("json_each");
    for dialect in [SqlDialect::SQLite, SqlDialect::PostgreSQL, SqlDialect::DuckDB] {
        let sql = seeded_generator(dialect).generate_statement(&stmt).unwrap();
        assert!(
            sql.contains("json_each(t_1.j) AS _narrow_2"),
            "expected plain json_each on {dialect:?}, got: {sql}"
        );
    }
}

#[test]
fn internal_json_each_object_spellings() {
    // Object-each (metadata tree groups): canonical json_each off postgres;
    // pg's jsonb_each is object-each exactly, natural (key, value) columns.
    let stmt = tvf_join_stmt(crate::pipeline::naming::INTERNAL_JSON_EACH_OBJECT);
    for dialect in [SqlDialect::SQLite, SqlDialect::DuckDB] {
        let sql = seeded_generator(dialect).generate_statement(&stmt).unwrap();
        assert!(
            sql.contains("json_each(t_1.j) AS _narrow_2"),
            "expected canonical json_each spelling on {dialect:?}, got: {sql}"
        );
        assert!(!sql.contains("__dql"), "internal name leaked ({dialect:?}): {sql}");
    }
    let sql = seeded_generator(SqlDialect::PostgreSQL)
        .generate_statement(&stmt)
        .unwrap();
    assert!(
        sql.contains("LEFT JOIN jsonb_each(CAST(t_1.j AS jsonb)) AS _narrow_2 ON TRUE"),
        "got: {sql}"
    );
    assert!(!sql.contains("__dql"), "internal name leaked: {sql}");
}
