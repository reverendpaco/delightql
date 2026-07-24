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
    // mysql is a SHAPE change (CONCAT is a function, not an infix token) —
    // an op.* '{'-template, not a token swap. The old `a CONCAT b` pin
    // faithfully preserved an M1-era bug (DIALECT-CONTRACT.md B3, fixed).
    let render = |d| seeded_generator(d).render_expression(&concat_expr()).unwrap();
    assert_eq!(render(SqlDialect::SQLite), "a || b");
    assert_eq!(render(SqlDialect::PostgreSQL), "a || b");
    assert_eq!(render(SqlDialect::MySQL), "CONCAT(a, b)");
    assert_eq!(render(SqlDialect::SqlServer), "a + b");
}

#[test]
fn null_safe_spellings_mysql() {
    // DelightQL = / != are null-safe: canonical IS [NOT] DISTINCT FROM.
    // mysql has neither — <=> is its null-safe equality (token swap), and
    // the negation needs a NOT wrap (op template). DIALECT-CONTRACT.md B4.
    let eq = DomainExpression::Binary {
        left: Box::new(DomainExpression::column("a")),
        op: BinaryOperator::IsNotDistinctFrom,
        right: Box::new(DomainExpression::column("b")),
    };
    let neq = DomainExpression::Binary {
        left: Box::new(DomainExpression::column("a")),
        op: BinaryOperator::IsDistinctFrom,
        right: Box::new(DomainExpression::column("b")),
    };
    let render =
        |d: SqlDialect, e: &DomainExpression| seeded_generator(d).render_expression(e).unwrap();
    assert_eq!(render(SqlDialect::MySQL, &eq), "a <=> b");
    assert_eq!(render(SqlDialect::MySQL, &neq), "NOT (a <=> b)");
    // canonical untouched
    assert_eq!(render(SqlDialect::SQLite, &eq), "a IS NOT DISTINCT FROM b");
    assert_eq!(render(SqlDialect::SQLite, &neq), "a IS DISTINCT FROM b");
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

#[test]
fn identifier_escaping_own_closing_delimiter() {
    // Identifier bytes come from DATA (a pivot key becomes a column
    // name), so each target's closing delimiter must double inside its
    // own quoting — otherwise data rewrites the SQL token stream. A
    // foreign target's delimiter is ordinary data and passes through.
    // The pack pairs ident.quoted with ident.escape; the writer refuses
    // a template without an escape row.
    let render = |d, name: &str| {
        seeded_generator(d)
            .render_expression(&DomainExpression::column(name))
            .unwrap()
    };
    assert_eq!(render(SqlDialect::SQLite, "a\"b"), "\"a\"\"b\"");
    assert_eq!(render(SqlDialect::PostgreSQL, "a\"b"), "\"a\"\"b\"");
    assert_eq!(render(SqlDialect::DuckDB, "a\"b"), "\"a\"\"b\"");
    assert_eq!(render(SqlDialect::MySQL, "a`b"), "`a``b`");
    assert_eq!(render(SqlDialect::SqlServer, "a]b"), "[a]]b]");
    assert_eq!(render(SqlDialect::MySQL, "a]\"b"), "`a]\"b`");
    assert_eq!(render(SqlDialect::SqlServer, "a`\"b"), "[a`\"b]");
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
        // The sequence guard (JSON-SUBSTRATE.md): a non-array or
        // malformed value becomes a NULL interior — zero rows.
        assert!(
            sql.contains(
                "json_each(CASE WHEN json_valid(t_1.j) AND json_type(t_1.j) = 'array' \
                 THEN t_1.j END) AS _narrow_2"
            ),
            "expected guarded canonical json_each spelling on {dialect:?}, got: {sql}"
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
             FROM jsonb_array_elements(CASE WHEN jsonb_typeof(CAST(t_1.j AS jsonb)) = 'array' \
             THEN CAST(t_1.j AS jsonb) END) WITH ORDINALITY AS e) \
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

#[test]
fn scalar_form_overloads_max_min_round() {
    use crate::pipeline::naming;
    // The SQL-AST constructor stamps arity-revealed forms: 2+-arg max/min
    // is sqlite's SCALAR overload (pg: GREATEST/LEAST), 2-arg round needs
    // pg's numeric coercion. Aggregate (1-arg) forms keep their names.
    let scalar_max = DomainExpression::function(
        "max",
        vec![
            DomainExpression::column("a"),
            DomainExpression::literal(LiteralValue::Number("18".to_string())),
        ],
    );
    assert!(
        matches!(&scalar_max, DomainExpression::Function { name, .. }
            if name == naming::INTERNAL_SCALAR_MAX),
        "constructor did not stamp the scalar-max form"
    );
    let render = |d, e: &DomainExpression| seeded_generator(d).render_expression(e).unwrap();
    // canonical spelling on a row miss — internal name never leaks
    assert_eq!(render(SqlDialect::SQLite, &scalar_max), "max(a, 18)");
    assert_eq!(render(SqlDialect::DuckDB, &scalar_max), "max(a, 18)");
    // pg: NULL-propagating GREATEST (sqlite scalar max is NULL if ANY arg
    // is NULL; bare GREATEST ignores NULLs — the measured divergence)
    assert_eq!(
        render(SqlDialect::PostgreSQL, &scalar_max),
        "CASE WHEN a IS NULL OR 18 IS NULL THEN NULL ELSE GREATEST(a, 18) END"
    );

    let scalar_min = DomainExpression::function(
        "min",
        vec![DomainExpression::column("a"), DomainExpression::column("b")],
    );
    assert_eq!(
        render(SqlDialect::PostgreSQL, &scalar_min),
        "CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE LEAST(a, b) END"
    );
    assert_eq!(render(SqlDialect::SQLite, &scalar_min), "min(a, b)");

    // aggregate max (1-arg) is untouched everywhere, including pg
    let agg_max = DomainExpression::function("max", vec![DomainExpression::column("a")]);
    assert_eq!(render(SqlDialect::PostgreSQL, &agg_max), "max(a)");
    assert_eq!(render(SqlDialect::SQLite, &agg_max), "max(a)");

    // 2-arg round: pg coerces the value to numeric; canonical elsewhere
    let round2 = DomainExpression::function(
        "round",
        vec![
            DomainExpression::column("x"),
            DomainExpression::literal(LiteralValue::Number("1".to_string())),
        ],
    );
    assert_eq!(render(SqlDialect::SQLite, &round2), "round(x, 1)");
    assert_eq!(render(SqlDialect::DuckDB, &round2), "round(x, 1)");
    assert_eq!(
        render(SqlDialect::PostgreSQL, &round2),
        "round(CAST(x AS numeric), CAST(1 AS integer))"
    );
    // 1-arg round is not the stamped form
    let round1 = DomainExpression::function("round", vec![DomainExpression::column("x")]);
    assert_eq!(render(SqlDialect::PostgreSQL, &round1), "round(x)");
}

#[test]
fn arbitrary_witness_form() {
    use crate::pipeline::naming;
    // The transformer stamps bare `<~` delegate columns with the
    // arbitrary-witness form. Canonical/sqlite spelling UNWRAPS to the bare
    // column (relaxed GROUP BY); strict targets spell it any_value().
    let arb = DomainExpression::function(
        naming::INTERNAL_ARBITRARY,
        vec![DomainExpression::column("name")],
    );
    let render = |d, e: &DomainExpression| seeded_generator(d).render_expression(e).unwrap();
    assert_eq!(render(SqlDialect::SQLite, &arb), "name");
    assert_eq!(render(SqlDialect::PostgreSQL, &arb), "any_value(name)");
    assert_eq!(render(SqlDialect::DuckDB, &arb), "any_value(name)");

    // wrong arity is an error, not a silent leak of the internal name
    let bad = DomainExpression::function(
        naming::INTERNAL_ARBITRARY,
        vec![DomainExpression::column("a"), DomainExpression::column("b")],
    );
    assert!(seeded_generator(SqlDialect::SQLite)
        .render_expression(&bad)
        .is_err());
}

// ---------------------------------------------------------------------------
// DIALECT-CONTRACT.md probes — mechanism claims proven against SYNTHETIC
// packs (custom rows in an in-memory bootstrap, so the probes are
// independent of what happens to be seeded). P-numbers refer to the
// contract's catalog.
// ---------------------------------------------------------------------------

/// A generator whose pack contains the given extra `dialect_render` rows on
/// top of the real bootstrap seeds.
fn generator_with_rows(dialect: SqlDialect, rows: &[(&str, &str, &str, &str)]) -> SqlGenerator {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
    for (d, key, kind, body) in rows {
        conn.execute(
            "INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![d, key, kind, body],
        )
        .unwrap();
    }
    let pack = DialectPack::load(&conn).unwrap();
    SqlGenerator::new()
        .with_dialect(dialect)
        .with_dialect_pack(std::sync::Arc::new(pack))
}

#[test]
fn contract_p3_template_arg_reorder_and_duplication() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[("postgres", "fn.probe_dup", "template", "({1} + {0} - {0})")],
    );
    let expr = fn_call(
        "probe_dup",
        vec![DomainExpression::column("a"), DomainExpression::column("b")],
    );
    assert_eq!(g.render_expression(&expr).unwrap(), "(b + a - a)");
}

#[test]
fn contract_p4_custom_infix_via_template() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[("postgres", "fn.probe_bitor", "template", "({0} | {1})")],
    );
    let expr = fn_call(
        "probe_bitor",
        vec![DomainExpression::column("a"), DomainExpression::column("b")],
    );
    assert_eq!(g.render_expression(&expr).unwrap(), "(a | b)");
}

#[test]
fn contract_p5_keyword_interleaved_args() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[
            ("postgres", "fn.probe_extract_day", "template", "EXTRACT(DAY FROM {0})"),
            ("postgres", "fn.probe_attz", "template", "{0} AT TIME ZONE {1}"),
        ],
    );
    let extract = fn_call("probe_extract_day", vec![DomainExpression::column("ts")]);
    assert_eq!(
        g.render_expression(&extract).unwrap(),
        "EXTRACT(DAY FROM ts)"
    );
    let attz = fn_call(
        "probe_attz",
        vec![
            DomainExpression::column("ts"),
            DomainExpression::literal(LiteralValue::String("UTC".into())),
        ],
    );
    assert_eq!(g.render_expression(&attz).unwrap(), "ts AT TIME ZONE 'UTC'");
}

#[test]
fn contract_p6_clause_carrying_template() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[(
            "postgres",
            "fn.probe_pctl",
            "template",
            "percentile_cont({0}) WITHIN GROUP (ORDER BY {1})",
        )],
    );
    let expr = fn_call(
        "probe_pctl",
        vec![
            DomainExpression::literal(LiteralValue::Number("0.5".into())),
            DomainExpression::column("salary"),
        ],
    );
    assert_eq!(
        g.render_expression(&expr).unwrap(),
        "percentile_cont(0.5) WITHIN GROUP (ORDER BY salary)"
    );
}

#[test]
fn contract_n1_reserved_rule_kind_is_loud() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[("postgres", "fn.probe_lua", "lua", "return 'nope'")],
    );
    let expr = fn_call("probe_lua", vec![DomainExpression::column("a")]);
    let err = g.render_expression(&expr).unwrap_err();
    assert!(
        format!("{err:?}").contains("rule_kind"),
        "reserved rule_kind must fail loudly, got: {err:?}"
    );
}

#[test]
fn contract_n2_unconsumed_template_arg_is_loud() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[("postgres", "fn.probe_uncons", "template", "f({0})")],
    );
    let expr = fn_call(
        "probe_uncons",
        vec![DomainExpression::column("a"), DomainExpression::column("b")],
    );
    assert!(
        g.render_expression(&expr).is_err(),
        "a template that ignores an argument must refuse, not swallow"
    );
}

#[test]
fn contract_n3_full_template_refuses_distinct() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[("postgres", "fn.probe_distinct", "template", "g({0})")],
    );
    let expr = DomainExpression::Function {
        name: "probe_distinct".to_string(),
        args: vec![DomainExpression::column("a")],
        distinct: true,
    };
    let err = g.render_expression(&expr).unwrap_err();
    assert!(
        format!("{err:?}").contains("DISTINCT"),
        "full template + DISTINCT must refuse loudly, got: {err:?}"
    );
}

#[test]
fn contract_n5_unknown_rust_handler_is_loud() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[("postgres", "fn.probe_handler", "rust_handler", "no_such_handler")],
    );
    let expr = fn_call("probe_handler", vec![DomainExpression::column("a")]);
    let err = g.render_expression(&expr).unwrap_err();
    assert!(
        format!("{err:?}").contains("no_such_handler"),
        "unknown rust_handler must name the missing handler, got: {err:?}"
    );
}
