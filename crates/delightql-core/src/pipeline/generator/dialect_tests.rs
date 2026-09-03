// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Dialect-spelling tests: prove the seeded `dialect_render` rows reproduce
//! the spellings the old `match dialect` arms produced, and that a missing
//! row falls back to the canonical (SQLite) code default.

use super::{SqlDialect, SqlGenerator};
use crate::names::{baptise, Addressing, Baptised, Bundle, ColId, Registry, ScopeId, Statement};
use crate::pipeline::ast_refined::LiteralValue;
use crate::pipeline::dialect_pack::DialectPack;
use crate::pipeline::sql_ast::{BinaryOperator, DomainExpression};
use std::collections::HashMap;
use std::sync::Arc;

const TEST_COLUMNS: &[&str] = &[
    "a", "active", "age", "b", "email", "j", "k", "name", "order", "salary", "ts", "v", "x", "y",
    "a\"b", "a`b", "a]b", "a]\"b", "a`\"b",
];

struct TestGenerator {
    _registry: &'static Registry,
    names: &'static Baptised<'static>,
    at: ScopeId,
    columns: HashMap<&'static str, ColId>,
    dialect: SqlDialect,
    pack: Arc<DialectPack>,
    bin_registry: Option<Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>>,
}

impl TestGenerator {
    fn with_pack(
        dialect: SqlDialect,
        pack: Arc<DialectPack>,
        bin_registry: Option<Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>>,
    ) -> Self {
        let registry = Box::leak(Box::new(Registry::new(&[])));
        let at = registry.carrier_scope("expr");
        let mut columns = HashMap::new();
        let mut heading = Vec::new();
        for (_position, name) in TEST_COLUMNS.iter().copied().enumerate() {
            let spelling = registry.intern(name, false);
            let column = registry.sql_column(at, Some(spelling), Addressing::Published);
            columns.insert(name, column);
            heading.push(column);
        }
        let names = Box::leak(Box::new(
            baptise(
                registry,
                &Bundle::gather(vec![Statement {
                    scopes: vec![at],
                    headings: vec![heading.clone()],
                    refs: heading,
                }])
                .reserve_authored(registry),
            )
            .unwrap(),
        ));
        Self {
            _registry: registry,
            names,
            at,
            columns,
            dialect,
            pack,
            bin_registry,
        }
    }

    fn generator(&self) -> SqlGenerator<'static, 'static> {
        let generator = SqlGenerator::new(self.names)
            .with_dialect(self.dialect)
            .with_dialect_pack(Arc::clone(&self.pack));
        match &self.bin_registry {
            Some(registry) => generator.with_bin_registry(Arc::clone(registry)),
            None => generator,
        }
    }

    fn column(&self, name: &str) -> DomainExpression {
        DomainExpression::Column(
            *self
                .columns
                .get(name)
                .unwrap_or_else(|| panic!("test fixture has no column {name:?}")),
        )
    }

    fn render_expression(
        &self,
        expression: &DomainExpression,
    ) -> Result<String, super::GeneratorError> {
        self.generator().render_expression(expression, self.at)
    }

    fn generate_statement(
        &self,
        statement: &crate::pipeline::sql_ast::SqlStatement,
    ) -> Result<String, super::GeneratorError> {
        self.generator().generate_statement(statement)
    }
}

fn seeded_pack() -> Arc<DialectPack> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
    Arc::new(DialectPack::load(&conn).unwrap())
}

/// A generator targeting `dialect`, carrying the pack loaded from the real
/// bootstrap schema (so these tests exercise the actual seed rows).
fn seeded_generator(dialect: SqlDialect) -> TestGenerator {
    TestGenerator::with_pack(dialect, seeded_pack(), None)
}

fn not_equal_expr(generator: &TestGenerator) -> DomainExpression {
    DomainExpression::Binary {
        left: Box::new(generator.column("a")),
        op: BinaryOperator::NotEqual,
        right: Box::new(generator.column("b")),
    }
}

fn concat_expr(generator: &TestGenerator) -> DomainExpression {
    DomainExpression::Binary {
        left: Box::new(generator.column("a")),
        op: BinaryOperator::Concatenate,
        right: Box::new(generator.column("b")),
    }
}

fn bool_expr(generator: &TestGenerator) -> DomainExpression {
    DomainExpression::Binary {
        left: Box::new(generator.column("active")),
        op: BinaryOperator::Equal,
        right: Box::new(DomainExpression::literal(LiteralValue::Boolean(true))),
    }
}

fn quoted_ident_expr(generator: &TestGenerator) -> DomainExpression {
    // "order" is a reserved word, so it must be quoted in every dialect.
    generator.column("order")
}

#[test]
fn not_equal_spellings() {
    let render = |d| {
        let generator = seeded_generator(d);
        generator
            .render_expression(&not_equal_expr(&generator))
            .unwrap()
    };
    assert_eq!(render(SqlDialect::SQLite), "a != b");
    assert_eq!(render(SqlDialect::PostgreSQL), "a != b");
    assert_eq!(render(SqlDialect::MySQL), "a <> b");
    assert_eq!(render(SqlDialect::SqlServer), "a <> b");
}

#[test]
fn concatenate_spellings() {
    // mysql is a SHAPE change (CONCAT is a function, not an infix token) —
    // an op.* '{'-template, not a token swap.
    let render = |d| {
        let generator = seeded_generator(d);
        generator
            .render_expression(&concat_expr(&generator))
            .unwrap()
    };
    assert_eq!(render(SqlDialect::SQLite), "a || b");
    assert_eq!(render(SqlDialect::PostgreSQL), "a || b");
    assert_eq!(render(SqlDialect::MySQL), "CONCAT(a, b)");
    assert_eq!(render(SqlDialect::SqlServer), "a + b");
}

#[test]
fn null_safe_spellings_mysql() {
    // DelightQL = / != are null-safe: canonical IS [NOT] DISTINCT FROM.
    // mysql has neither — <=> is its null-safe equality (token swap), and
    // the negation needs a NOT wrap (op template).
    let render = |dialect: SqlDialect, op| {
        let generator = seeded_generator(dialect);
        let expression = DomainExpression::Binary {
            left: Box::new(generator.column("a")),
            op,
            right: Box::new(generator.column("b")),
        };
        generator.render_expression(&expression).unwrap()
    };
    assert_eq!(
        render(SqlDialect::MySQL, BinaryOperator::IsNotDistinctFrom),
        "a <=> b"
    );
    assert_eq!(
        render(SqlDialect::MySQL, BinaryOperator::IsDistinctFrom),
        "NOT (a <=> b)"
    );
    // canonical untouched
    assert_eq!(
        render(SqlDialect::SQLite, BinaryOperator::IsNotDistinctFrom),
        "a IS NOT DISTINCT FROM b"
    );
    assert_eq!(
        render(SqlDialect::SQLite, BinaryOperator::IsDistinctFrom),
        "a IS DISTINCT FROM b"
    );
}

#[test]
fn boolean_literal_spellings() {
    let render = |d| {
        let generator = seeded_generator(d);
        generator.render_expression(&bool_expr(&generator)).unwrap()
    };
    assert_eq!(render(SqlDialect::SQLite), "active = 1");
    assert_eq!(render(SqlDialect::MySQL), "active = 1");
    assert_eq!(render(SqlDialect::PostgreSQL), "active = TRUE");
    assert_eq!(render(SqlDialect::SqlServer), "active = TRUE");
}

#[test]
fn identifier_quoting_spellings() {
    let render = |d| {
        let generator = seeded_generator(d);
        generator
            .render_expression(&quoted_ident_expr(&generator))
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
        let generator = seeded_generator(d);
        generator
            .render_expression(&generator.column(name))
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
        name: name.into(),
        args,
        distinct: false,
    }
}

fn intrinsic_call(
    intrinsic: crate::names::Intrinsic,
    args: Vec<DomainExpression>,
) -> DomainExpression {
    DomainExpression::intrinsic(intrinsic, args)
}

#[test]
fn function_rename_rule() {
    // duckdb: json_extract -> json_extract_string (bare-name body keeps shape)
    let generator = seeded_generator(SqlDialect::DuckDB);
    let expr = fn_call(
        "json_extract",
        vec![
            generator.column("j"),
            DomainExpression::literal(LiteralValue::String("$.a".into())),
        ],
    );
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "json_extract_string(j, '$.a')"
    );
    // sqlite: no rule, canonical name kept
    let generator = seeded_generator(SqlDialect::SQLite);
    let expr = fn_call(
        "json_extract",
        vec![
            generator.column("j"),
            DomainExpression::literal(LiteralValue::String("$.a".into())),
        ],
    );
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
            generator.column("x"),
            DomainExpression::literal(LiteralValue::String("b".into())),
            generator.column("y"),
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
        name: "JSON_OBJECT".into(),
        args: vec![generator.column("x")],
        distinct: true,
    };
    assert!(generator.render_expression(&expr).is_err());
}

#[test]
fn function_rename_keeps_distinct() {
    // A bare-name rule preserves the call shape, DISTINCT included.
    let generator = seeded_generator(SqlDialect::PostgreSQL);
    let expr = DomainExpression::Function {
        name: "json_group_object".into(),
        args: vec![generator.column("k"), generator.column("v")],
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
    // (postgres respells it too — but via its OWN row, fn.__dql_json_extract_raw,
    // to the jsonb flavor; see rust_handler_rules_fire_end_to_end.)
    for dialect in [SqlDialect::SQLite, SqlDialect::DuckDB] {
        let generator = seeded_generator(dialect);
        let expr = intrinsic_call(
            crate::names::Intrinsic::JsonExtractRaw,
            vec![
                generator.column("j"),
                DomainExpression::literal(LiteralValue::String("$.scripts".into())),
            ],
        );
        assert_eq!(
            generator.render_expression(&expr).unwrap(),
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
            generator.column("j"),
            DomainExpression::literal(LiteralValue::String("$.a.b".into())),
        ],
    );
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "(CAST(j AS jsonb) #>> '{a,b}')"
    );
    // native-json provenance -> jsonb flavor
    let expr = intrinsic_call(
        crate::names::Intrinsic::JsonExtractRaw,
        vec![
            generator.column("j"),
            DomainExpression::literal(LiteralValue::String("$.scripts".into())),
        ],
    );
    assert_eq!(
        generator.render_expression(&expr).unwrap(),
        "(CAST(j AS jsonb) #> '{scripts}')"
    );
    // group_concat: 1-arg synthesizes the default separator; DISTINCT flows
    let expr = DomainExpression::Function {
        name: "GROUP_CONCAT".into(),
        args: vec![generator.column("name")],
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
            generator.column("j"),
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
fn synced_generator(dialect: SqlDialect) -> TestGenerator {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
    // bin_sync auto-enlists universal cartridges into 'main', which system
    // init creates via the namespace bootstrap — replicate that here.
    crate::import::create_bootstrap_namespaces(&conn).unwrap();
    let mut registry = crate::bin_cartridge::registry::BinCartridgeRegistry::new();
    registry.register_cartridge(crate::bin_cartridge::prelude::create_prelude_cartridge());
    registry.register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());
    crate::bootstrap::sync_bin_cartridges_to_bootstrap(&conn, &registry).unwrap();
    let pack = DialectPack::load(&conn).unwrap();
    TestGenerator::with_pack(dialect, Arc::new(pack), Some(Arc::new(registry)))
}

fn sql_comparison(generator: &TestGenerator, name: &str, namespace: &[&str]) -> DomainExpression {
    DomainExpression::PredicateRewrite {
        name: name.to_string(),
        namespace: namespace.iter().map(|part| part.to_string()).collect(),
        args: vec![
            generator.column("age"),
            DomainExpression::literal(LiteralValue::Number("1".into())),
        ],
        negated: false,
    }
}

/// `sql_eq` / `sql_ne` render as the target's ORDINARY comparison — never a
/// null-safe spelling — through the dialect's own operator row, and the
/// generator looks the entity up by the identity the resolver selected.
#[test]
fn sql_comparison_predicates_render_the_targets_ordinary_operator() {
    for (dialect, ne) in [
        (SqlDialect::SQLite, "!="),
        (SqlDialect::PostgreSQL, "!="),
        (SqlDialect::DuckDB, "!="),
        (SqlDialect::MySQL, "<>"),
        (SqlDialect::SqlServer, "<>"),
    ] {
        let generator = synced_generator(dialect);
        assert_eq!(
            generator
                .render_expression(&sql_comparison(&generator, "sql_eq", &[]))
                .unwrap(),
            "age = 1",
            "{dialect:?}"
        );
        assert_eq!(
            generator
                .render_expression(&sql_comparison(&generator, "sql_ne", &[]))
                .unwrap(),
            format!("age {ne} 1"),
            "{dialect:?}"
        );
    }
    // The qualified identity is looked up EXACTLY: the prelude namespace
    // answers, and a namespace that never held the entity refuses rather
    // than falling back to a bare-name search.
    let sqlite = synced_generator(SqlDialect::SQLite);
    assert_eq!(
        sqlite
            .render_expression(&sql_comparison(&sqlite, "sql_eq", &["std", "prelude"]))
            .unwrap(),
        "age = 1"
    );
    let refused = sqlite
        .render_expression(&sql_comparison(&sqlite, "sql_eq", &["std", "predicates"]))
        .unwrap_err();
    assert!(format!("{refused:?}").contains("Unknown predicate rewrite: 'sql_eq'"));
}

fn like_predicate(generator: &TestGenerator, negated: bool) -> DomainExpression {
    DomainExpression::PredicateRewrite {
        name: "like".to_string(),
        namespace: Vec::new(),
        args: vec![
            generator.column("email"),
            DomainExpression::literal(LiteralValue::String("%@GMAIL.com".into())),
        ],
        negated,
    }
}

#[test]
fn sigma_form_rule_fires_per_dialect() {
    // canonical (sqlite): the bin entity's code lowering
    let sqlite = synced_generator(SqlDialect::SQLite);
    assert_eq!(
        sqlite
            .render_expression(&like_predicate(&sqlite, false))
            .unwrap(),
        "email LIKE '%@GMAIL.com'"
    );
    // postgres + duckdb: the seeded ILIKE fidelity rule (sqlite LIKE is
    // case-insensitive; theirs isn't — ILIKE restores canonical semantics)
    for dialect in [SqlDialect::PostgreSQL, SqlDialect::DuckDB] {
        let generator = synced_generator(dialect);
        assert_eq!(
            generator
                .render_expression(&like_predicate(&generator, false))
                .unwrap(),
            "email ILIKE '%@GMAIL.com'",
            "{dialect:?}"
        );
    }
    // negation wraps the template
    let postgres = synced_generator(SqlDialect::PostgreSQL);
    assert_eq!(
        postgres
            .render_expression(&like_predicate(&postgres, true))
            .unwrap(),
        "NOT (email ILIKE '%@GMAIL.com')"
    );
    let sqlite = synced_generator(SqlDialect::SQLite);
    assert_eq!(
        sqlite
            .render_expression(&like_predicate(&sqlite, true))
            .unwrap(),
        "email NOT LIKE '%@GMAIL.com'"
    );
    // +between has no rule anywhere: canonical on every dialect
    for dialect in [SqlDialect::SQLite, SqlDialect::PostgreSQL] {
        let generator = synced_generator(dialect);
        let between = DomainExpression::PredicateRewrite {
            name: "between".to_string(),
            namespace: Vec::new(),
            args: vec![
                generator.column("age"),
                DomainExpression::literal(LiteralValue::Number("18".into())),
                DomainExpression::literal(LiteralValue::Number("65".into())),
            ],
            negated: false,
        };
        assert_eq!(
            generator.render_expression(&between).unwrap(),
            "age BETWEEN 18 AND 65",
            "{dialect:?}"
        );
    }
}

#[test]
fn canonical_sigma_propagates_a_nested_rendering_error() {
    let generator = synced_generator(SqlDialect::SQLite);
    let nested = DomainExpression::PredicateRewrite {
        name: "missing_nested_predicate".to_string(),
        namespace: Vec::new(),
        args: Vec::new(),
        negated: false,
    };
    let outer = DomainExpression::PredicateRewrite {
        name: "between".to_string(),
        namespace: Vec::new(),
        args: vec![
            nested,
            DomainExpression::literal(LiteralValue::Number("1".into())),
            DomainExpression::literal(LiteralValue::Number("2".into())),
        ],
        negated: false,
    };

    let error = generator.render_expression(&outer).unwrap_err();
    assert!(error
        .to_string()
        .contains("Unknown predicate rewrite: 'missing_nested_predicate'"));
}

#[test]
fn cast_type_spellings() {
    let render = |d| {
        let generator = seeded_generator(d);
        let cast_expr = DomainExpression::cast(generator.column("age"), "real");
        generator.render_expression(&cast_expr).unwrap()
    };
    // canonical = uppercased DQL type word
    assert_eq!(render(SqlDialect::SQLite), "CAST(age AS REAL)");
    // sqlite REAL is an 8-byte float → per-target type.real rows
    assert_eq!(
        render(SqlDialect::PostgreSQL),
        "CAST(age AS DOUBLE PRECISION)"
    );
    assert_eq!(render(SqlDialect::DuckDB), "CAST(age AS DOUBLE)");
    // a type with no rows spells canonically everywhere
    for dialect in [
        SqlDialect::SQLite,
        SqlDialect::PostgreSQL,
        SqlDialect::DuckDB,
    ] {
        let generator = seeded_generator(dialect);
        let int_cast = DomainExpression::cast(generator.column("x"), "integer");
        assert_eq!(
            generator.render_expression(&int_cast).unwrap(),
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
    let generator =
        TestGenerator::with_pack(SqlDialect::PostgreSQL, Arc::new(DialectPack::empty()), None);
    assert_eq!(
        generator.render_expression(&bool_expr(&generator)).unwrap(),
        "active = 1"
    );
    let generator =
        TestGenerator::with_pack(SqlDialect::SqlServer, Arc::new(DialectPack::empty()), None);
    assert_eq!(
        generator
            .render_expression(&not_equal_expr(&generator))
            .unwrap(),
        "a != b"
    );
}

/// SELECT * FROM (SELECT 1) AS t_1 LEFT JOIN <tvf>(t_1.j) AS _narrow_2 ON 1 —
/// the shape the transformer's array-iteration sites emit.
fn tvf_join_case(
    function_name: impl Into<crate::pipeline::sql_ast::FunctionName>,
    dialect: SqlDialect,
) -> (TestGenerator, crate::pipeline::sql_ast::SqlStatement) {
    use crate::pipeline::sql_ast::*;
    let registry = Box::leak(Box::new(Registry::new(&[])));
    let source_name = registry.intern("t_1", false);
    let source_scope = registry.anonymous_scope(Some(source_name));
    let column_name = registry.intern("j", false);
    let source_column = registry.sql_column(source_scope, Some(column_name), Addressing::Published);
    let tvf_name = registry.intern("_narrow_2", false);
    let tvf_scope = registry.anonymous_scope(Some(tvf_name));
    let function = match function_name.into() {
        FunctionName::User(name) => {
            let spelling = registry.intern(&name, false);
            registry.mint_function(spelling, Vec::new())
        }
        FunctionName::Intrinsic(intrinsic) => registry.mint_intrinsic(intrinsic),
    };
    let result_scope = registry.join_scope();

    let source = (SelectStatement::builder().select(SelectItem::expression_with_alias(
        DomainExpression::literal(crate::pipeline::asts::core::literals::LiteralValue::Number(
            "1".into(),
        )),
        source_column,
    )))
    .standing_at(source_scope)
    .map_err(crate::error::DelightQLError::parse_error)
    .unwrap();
    let tvf = TableExpression::TVF {
        function,
        arguments: vec![TvfArgument::Column(source_column)],
        alias: tvf_scope,
    };
    let joined = TableExpression::Join {
        left: Box::new(TableExpression::subquery(
            QueryExpression::Select(Box::new(source)),
            source_scope,
        )),
        right: Box::new(tvf),
        join_type: JoinType::Left,
        join_condition: JoinCondition::On(DomainExpression::literal(
            crate::pipeline::asts::core::literals::LiteralValue::Boolean(true),
        )),
    };
    let select = (SelectStatement::builder()
        .select(SelectItem::star_over_nothing())
        .from_tables(vec![joined]))
    .standing_at(result_scope)
    .map_err(crate::error::DelightQLError::parse_error)
    .unwrap();
    let statement = SqlStatement::with_ctes(None, QueryExpression::Select(Box::new(select)));
    let names = Box::leak(Box::new(
        baptise(
            registry,
            &Bundle::gather(vec![Statement {
                scopes: vec![source_scope, tvf_scope, result_scope],
                headings: vec![vec![source_column]],
                refs: vec![source_column],
            }])
            .reserve_authored(registry),
        )
        .unwrap(),
    ));
    let generator = TestGenerator {
        _registry: registry,
        names,
        at: result_scope,
        columns: HashMap::from([("j", source_column)]),
        dialect,
        pack: seeded_pack(),
        bin_registry: None,
    };
    (generator, statement)
}

#[test]
fn internal_json_each_array_spells_canonically_off_postgres() {
    // The internal array-each name never leaks: sqlite/duckdb (no tvf rows)
    // fall back to the canonical json_each spelling.
    for dialect in [SqlDialect::SQLite, SqlDialect::DuckDB] {
        let (generator, statement) = tvf_join_case(crate::names::Intrinsic::JsonEachArray, dialect);
        let sql = generator.generate_statement(&statement).unwrap();
        // The sequence guard: a non-array or malformed value becomes a
        // NULL interior — zero rows.
        assert!(
            sql.contains(
                "json_each(CASE WHEN json_valid(t_1.j) AND json_type(t_1.j) = 'array' \
                 THEN t_1.j END) AS _narrow_2"
            ),
            "expected guarded canonical json_each spelling on {dialect:?}, got: {sql}"
        );
        assert!(
            !sql.contains("__dql"),
            "internal name leaked ({dialect:?}): {sql}"
        );
    }
}

#[test]
fn internal_json_each_array_becomes_lateral_on_postgres() {
    // pg's json_each is object-only; the array-provenance TVF renders as a
    // LATERAL derived table over jsonb_array_elements WITH ORDINALITY, with
    // sqlite-compatible column names (key 0-based, value) and the alias
    // appended by code.
    let (generator, statement) = tvf_join_case(
        crate::names::Intrinsic::JsonEachArray,
        SqlDialect::PostgreSQL,
    );
    let sql = generator.generate_statement(&statement).unwrap();
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
    // yet — it spells canonically everywhere. The polymorphic shim
    // owns it; this pins the phase-1 boundary.
    for dialect in [
        SqlDialect::SQLite,
        SqlDialect::PostgreSQL,
        SqlDialect::DuckDB,
    ] {
        let (generator, statement) = tvf_join_case("json_each", dialect);
        let sql = generator.generate_statement(&statement).unwrap();
        assert!(
            sql.contains("json_each(t_1.j) AS _narrow_2"),
            "expected plain json_each on {dialect:?}, got: {sql}"
        );
    }
}

#[test]
fn reserved_looking_authored_tvf_is_not_an_intrinsic() {
    let (generator, statement) = tvf_join_case("__dql_json_each_array", SqlDialect::PostgreSQL);
    let sql = generator.generate_statement(&statement).unwrap();
    assert!(
        sql.contains("LEFT JOIN __dql_json_each_array(t_1.j) AS _narrow_2 ON TRUE"),
        "authored TVF was captured by an intrinsic render rule: {sql}"
    );
}

#[test]
fn internal_json_each_object_spellings() {
    // Object-each (metadata tree groups): canonical json_each off postgres;
    // pg's jsonb_each is object-each exactly, natural (key, value) columns.
    for dialect in [SqlDialect::SQLite, SqlDialect::DuckDB] {
        let (generator, statement) =
            tvf_join_case(crate::names::Intrinsic::JsonEachObject, dialect);
        let sql = generator.generate_statement(&statement).unwrap();
        assert!(
            sql.contains("json_each(t_1.j) AS _narrow_2"),
            "expected canonical json_each spelling on {dialect:?}, got: {sql}"
        );
        assert!(
            !sql.contains("__dql"),
            "internal name leaked ({dialect:?}): {sql}"
        );
    }
    let (generator, statement) = tvf_join_case(
        crate::names::Intrinsic::JsonEachObject,
        SqlDialect::PostgreSQL,
    );
    let sql = generator.generate_statement(&statement).unwrap();
    assert!(
        sql.contains("LEFT JOIN jsonb_each(CAST(t_1.j AS jsonb)) AS _narrow_2 ON TRUE"),
        "got: {sql}"
    );
    assert!(!sql.contains("__dql"), "internal name leaked: {sql}");
}

#[test]
fn scalar_form_overloads_max_min_round() {
    // The SQL-AST constructor stamps arity-revealed forms: 2+-arg max/min
    // is sqlite's SCALAR overload (pg: GREATEST/LEAST), 2-arg round needs
    // pg's numeric coercion. Aggregate (1-arg) forms keep their names.
    let sqlite = seeded_generator(SqlDialect::SQLite);
    let scalar_max = DomainExpression::function(
        "max",
        vec![
            sqlite.column("a"),
            DomainExpression::literal(LiteralValue::Number("18".to_string())),
        ],
    );
    assert!(
        matches!(&scalar_max, DomainExpression::Function { name, .. }
        if name == &crate::pipeline::sql_ast::FunctionName::Intrinsic(
            crate::names::Intrinsic::ScalarMax
        )),
        "constructor did not stamp the scalar-max form"
    );
    let render_scalar_max = |dialect| {
        let generator = seeded_generator(dialect);
        let expression = DomainExpression::function(
            "max",
            vec![
                generator.column("a"),
                DomainExpression::literal(LiteralValue::Number("18".to_string())),
            ],
        );
        generator.render_expression(&expression).unwrap()
    };
    // canonical spelling on a row miss — internal name never leaks
    assert_eq!(render_scalar_max(SqlDialect::SQLite), "max(a, 18)");
    assert_eq!(render_scalar_max(SqlDialect::DuckDB), "max(a, 18)");
    // pg: NULL-propagating GREATEST (sqlite scalar max is NULL if ANY arg
    // is NULL; bare GREATEST ignores NULLs — the measured divergence)
    assert_eq!(
        render_scalar_max(SqlDialect::PostgreSQL),
        "CASE WHEN a IS NULL OR 18 IS NULL THEN NULL ELSE GREATEST(a, 18) END"
    );

    let render_scalar_min = |dialect| {
        let generator = seeded_generator(dialect);
        let expression =
            DomainExpression::function("min", vec![generator.column("a"), generator.column("b")]);
        generator.render_expression(&expression).unwrap()
    };
    assert_eq!(
        render_scalar_min(SqlDialect::PostgreSQL),
        "CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE LEAST(a, b) END"
    );
    assert_eq!(render_scalar_min(SqlDialect::SQLite), "min(a, b)");

    // aggregate max (1-arg) is untouched everywhere, including pg
    let render_aggregate_max = |dialect| {
        let generator = seeded_generator(dialect);
        let expression = DomainExpression::function("max", vec![generator.column("a")]);
        generator.render_expression(&expression).unwrap()
    };
    assert_eq!(render_aggregate_max(SqlDialect::PostgreSQL), "max(a)");
    assert_eq!(render_aggregate_max(SqlDialect::SQLite), "max(a)");

    // 2-arg round: pg coerces the value to numeric; canonical elsewhere
    let render_round2 = |dialect| {
        let generator = seeded_generator(dialect);
        let expression = DomainExpression::function(
            "round",
            vec![
                generator.column("x"),
                DomainExpression::literal(LiteralValue::Number("1".to_string())),
            ],
        );
        generator.render_expression(&expression).unwrap()
    };
    assert_eq!(render_round2(SqlDialect::SQLite), "round(x, 1)");
    assert_eq!(render_round2(SqlDialect::DuckDB), "round(x, 1)");
    assert_eq!(
        render_round2(SqlDialect::PostgreSQL),
        "round(CAST(x AS numeric), CAST(1 AS integer))"
    );
    // 1-arg round is not the stamped form
    let postgres = seeded_generator(SqlDialect::PostgreSQL);
    let round1 = DomainExpression::function("round", vec![postgres.column("x")]);
    assert_eq!(postgres.render_expression(&round1).unwrap(), "round(x)");
}

#[test]
fn arbitrary_witness_form() {
    // The transformer stamps bare `<~` delegate columns with the
    // arbitrary-witness form. Canonical/sqlite spelling UNWRAPS to the bare
    // column (relaxed GROUP BY); strict targets spell it any_value().
    let render = |dialect| {
        let generator = seeded_generator(dialect);
        let expression = DomainExpression::intrinsic(
            crate::names::Intrinsic::Arbitrary,
            vec![generator.column("name")],
        );
        generator.render_expression(&expression).unwrap()
    };
    assert_eq!(render(SqlDialect::SQLite), "name");
    assert_eq!(render(SqlDialect::PostgreSQL), "any_value(name)");
    assert_eq!(render(SqlDialect::DuckDB), "any_value(name)");

    // wrong arity is an error, not a silent leak of the internal name
    let generator = seeded_generator(SqlDialect::SQLite);
    let bad = DomainExpression::intrinsic(
        crate::names::Intrinsic::Arbitrary,
        vec![generator.column("a"), generator.column("b")],
    );
    assert!(generator.render_expression(&bad).is_err());
}

#[test]
fn arbitrary_intrinsic_refuses_as_a_tvf_without_panicking() {
    let (generator, statement) =
        tvf_join_case(crate::names::Intrinsic::Arbitrary, SqlDialect::SQLite);
    let error = generator.generate_statement(&statement).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("intrinsic Arbitrary has no callable TVF spelling"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn reserved_looking_authored_function_is_not_an_intrinsic() {
    let generator = seeded_generator(SqlDialect::PostgreSQL);
    let authored = fn_call("__dql_arbitrary", vec![generator.column("name")]);
    assert_eq!(
        generator.render_expression(&authored).unwrap(),
        "__dql_arbitrary(name)"
    );
}

// ---------------------------------------------------------------------------
// DIALECT-CONTRACT probes — mechanism claims proven against SYNTHETIC
// packs (custom rows in an in-memory bootstrap, so the probes are
// independent of what happens to be seeded). P/N-numbers match the
// contract's own catalog numbering.
// ---------------------------------------------------------------------------

/// A generator whose pack contains the given extra `dialect_render` rows on
/// top of the real bootstrap seeds.
fn generator_with_rows(dialect: SqlDialect, rows: &[(&str, &str, &str, &str)]) -> TestGenerator {
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
    TestGenerator::with_pack(dialect, Arc::new(pack), None)
}

#[test]
fn contract_p3_template_arg_reorder_and_duplication() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[("postgres", "fn.probe_dup", "template", "({1} + {0} - {0})")],
    );
    let expr = fn_call("probe_dup", vec![g.column("a"), g.column("b")]);
    assert_eq!(g.render_expression(&expr).unwrap(), "(b + a - a)");
}

#[test]
fn contract_p4_custom_infix_via_template() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[("postgres", "fn.probe_bitor", "template", "({0} | {1})")],
    );
    let expr = fn_call("probe_bitor", vec![g.column("a"), g.column("b")]);
    assert_eq!(g.render_expression(&expr).unwrap(), "(a | b)");
}

#[test]
fn contract_p5_keyword_interleaved_args() {
    let g = generator_with_rows(
        SqlDialect::PostgreSQL,
        &[
            (
                "postgres",
                "fn.probe_extract_day",
                "template",
                "EXTRACT(DAY FROM {0})",
            ),
            (
                "postgres",
                "fn.probe_attz",
                "template",
                "{0} AT TIME ZONE {1}",
            ),
        ],
    );
    let extract = fn_call("probe_extract_day", vec![g.column("ts")]);
    assert_eq!(
        g.render_expression(&extract).unwrap(),
        "EXTRACT(DAY FROM ts)"
    );
    let attz = fn_call(
        "probe_attz",
        vec![
            g.column("ts"),
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
            g.column("salary"),
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
    let expr = fn_call("probe_lua", vec![g.column("a")]);
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
    let expr = fn_call("probe_uncons", vec![g.column("a"), g.column("b")]);
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
        name: "probe_distinct".into(),
        args: vec![g.column("a")],
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
        &[(
            "postgres",
            "fn.probe_handler",
            "rust_handler",
            "no_such_handler",
        )],
    );
    let expr = fn_call("probe_handler", vec![g.column("a")]);
    let err = g.render_expression(&expr).unwrap_err();
    assert!(
        format!("{err:?}").contains("no_such_handler"),
        "unknown rust_handler must name the missing handler, got: {err:?}"
    );
}

/// Build a statement whose single CTE must be evaluated once, and the same
/// statement without that requirement, for `dialect`.
///
/// The requirement is what a closed configured rule value produces: one
/// carrier holding an evaluation every spend of the value must share.
fn once_only_cte_case(
    dialect: SqlDialect,
) -> (
    TestGenerator,
    crate::pipeline::sql_ast::SqlStatement,
    crate::pipeline::sql_ast::SqlStatement,
) {
    use crate::pipeline::sql_ast::*;
    let registry = Box::leak(Box::new(Registry::new(&[])));
    let cte_name = registry.intern("capture_1", false);
    let cte_scope = registry.anonymous_scope(Some(cte_name));
    let value_name = registry.intern("v", false);
    let cte_column = registry.sql_column(cte_scope, Some(value_name), Addressing::Published);
    let result_scope = registry.join_scope();

    let body = (SelectStatement::builder().select(SelectItem::expression_with_alias(
        DomainExpression::literal(LiteralValue::Number("1".into())),
        cte_column,
    )))
    .standing_at(cte_scope)
    .map_err(crate::error::DelightQLError::parse_error)
    .unwrap();
    let outer = || {
        (SelectStatement::builder()
            .select(SelectItem::star_over_nothing())
            .from_tables(vec![TableExpression::Scope(cte_scope)]))
        .standing_at(result_scope)
        .map_err(crate::error::DelightQLError::parse_error)
        .unwrap()
    };
    let ordinary = Cte::ordinary(cte_scope, QueryExpression::Select(Box::new(body.clone())));
    let once_only = Cte::ordinary(cte_scope, QueryExpression::Select(Box::new(body)))
        .requiring_materialization();
    let names = Box::leak(Box::new(
        baptise(
            registry,
            &Bundle::gather(vec![Statement {
                scopes: vec![cte_scope, result_scope],
                headings: vec![vec![cte_column]],
                refs: vec![cte_column],
            }])
            .reserve_authored(registry),
        )
        .unwrap(),
    ));
    let generator = TestGenerator {
        _registry: registry,
        names,
        at: result_scope,
        columns: HashMap::from([("v", cte_column)]),
        dialect,
        pack: seeded_pack(),
        bin_registry: None,
    };
    (
        generator,
        SqlStatement::with_ctes(
            Some(vec![once_only]),
            QueryExpression::Select(Box::new(outer())),
        ),
        SqlStatement::with_ctes(
            Some(vec![ordinary]),
            QueryExpression::Select(Box::new(outer())),
        ),
    )
}

/// CONTRACT N6 — THREE TARGETS CAN PROMISE ONCE-ONLY EVALUATION.
///
/// SQLite, PostgreSQL and DuckDB all spell the promise in the binding
/// itself, and it is the binding — not a hint outside it — that a spend
/// reads.
#[test]
fn contract_n6_once_only_cte_is_spelled_where_the_target_can_promise_it() {
    for dialect in [
        SqlDialect::SQLite,
        SqlDialect::PostgreSQL,
        SqlDialect::DuckDB,
    ] {
        let (generator, once_only, ordinary) = once_only_cte_case(dialect);
        let sql = generator.generate_statement(&once_only).unwrap();
        assert!(
            sql.contains(" AS MATERIALIZED ("),
            "{dialect:?} must spell the once-only requirement in the binding, got: {sql}"
        );
        let plain = generator.generate_statement(&ordinary).unwrap();
        assert!(
            !plain.contains("MATERIALIZED"),
            "{dialect:?} must not materialize a binding that made no such \
             requirement, got: {plain}"
        );
    }
}

/// CONTRACT N6 — MYSQL AND SQL SERVER REFUSE, LOUDLY AND ON PURPOSE.
///
/// Neither target has a spelling that forbids re-evaluating a CTE, so a
/// plain binding there would silently re-run a volatile configuration once
/// per spend. The refusal names the target and the guarantee it cannot
/// make. An ordinary binding on the same targets is unaffected.
#[test]
fn contract_n6_once_only_cte_refuses_where_the_target_cannot_promise_it() {
    for dialect in [SqlDialect::MySQL, SqlDialect::SqlServer] {
        let (generator, once_only, ordinary) = once_only_cte_case(dialect);
        let error = generator
            .generate_statement(&once_only)
            .expect_err("a guarantee this target cannot make must refuse");
        let rendered = format!("{error:?}");
        assert!(
            rendered.contains("once-only materialization")
                && rendered.contains(&format!("{dialect:?}")),
            "{dialect:?} must refuse by naming itself and the guarantee, got: {rendered}"
        );
        let plain = generator
            .generate_statement(&ordinary)
            .expect("an ordinary binding is lawful on every target");
        assert!(
            !plain.contains("MATERIALIZED"),
            "{dialect:?} emits an ordinary binding unchanged, got: {plain}"
        );
    }
}
