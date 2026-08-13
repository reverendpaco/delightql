// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Per-compile dialect pack: the in-memory image of the `dialect_*`
//! bootstrap tables.
//!
//! Loaded once at the start of each query compile — alongside the other
//! bootstrap-metadata reads — and handed to the target-aware stages
//! (lowering, generator spelling) as a plain map, so the generator stays
//! a pure in-memory walk and never holds a DB handle. SQLite is the
//! canonical baseline and has no rows; a lookup miss means "use the
//! code-resident canonical default".

use crate::enums::EntityType;
use crate::names::Intrinsic;
use rusqlite::Connection;
use std::collections::HashMap;

/// One rule body plus its interpreter discriminator.
#[derive(Debug, Clone)]
pub struct RenderRule {
    pub rule_kind: String,
    pub body: String,
}

impl RenderRule {
    /// The body as a positional template — the only interpreter the render
    /// layer accepts today. A row whose kind has no interpreter is a loud
    /// error, never a silent fallback.
    pub fn template(&self) -> Result<&str, String> {
        if self.rule_kind == "template" {
            Ok(&self.body)
        } else {
            Err(format!(
                "unsupported rule_kind '{}' for render rule (no interpreter built for it)",
                self.rule_kind
            ))
        }
    }
}

/// Form rules for one (dialect, form_type): the per-functor overrides plus
/// the form-wide default (two grains via nullable entity_id).
#[derive(Debug, Default)]
struct FormRules {
    default: Option<RenderRule>,
    per_entity: HashMap<String, RenderRule>,
}

/// The resolved rule data for all dialects, keyed dialect-family → render_key.
#[derive(Debug, Default)]
pub struct DialectPack {
    render: HashMap<String, HashMap<String, RenderRule>>,
    intrinsic_render: HashMap<String, HashMap<(IntrinsicRenderKind, Intrinsic), RenderRule>>,
    /// dialect-family → form_type (entity_type_enum id) → rules.
    form: HashMap<String, HashMap<EntityType, FormRules>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IntrinsicRenderKind {
    Function,
    Tvf,
}

fn intrinsic_render_key(key: &str) -> Option<(IntrinsicRenderKind, Intrinsic)> {
    use crate::names::Intrinsic::{
        Arbitrary, JsonEachArray, JsonEachObject, JsonExtractRaw, Round2, ScalarMax, ScalarMin,
    };
    use IntrinsicRenderKind::{Function, Tvf};

    match key {
        "fn.__dql_json_extract_raw" => Some((Function, JsonExtractRaw)),
        "fn.__dql_scalar_max" => Some((Function, ScalarMax)),
        "fn.__dql_scalar_min" => Some((Function, ScalarMin)),
        "fn.__dql_round_2" => Some((Function, Round2)),
        "fn.__dql_arbitrary" => Some((Function, Arbitrary)),
        "tvf.__dql_json_each_array" => Some((Tvf, JsonEachArray)),
        "tvf.__dql_json_each_object" => Some((Tvf, JsonEachObject)),
        _ => None,
    }
}

impl DialectPack {
    /// A pack with no rules: every lookup misses, so every spelling is the
    /// code-resident canonical (SQLite) default. Used by standalone/utility
    /// generation paths that have no bootstrap connection.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Read `dialect_render` + `dialect_form_rule` into the maps.
    /// Form rules resolve `entity_id` to the entity NAME at load time —
    /// entity ids are session-local insertion order, never stable keys.
    /// Version-range selection is not yet implemented (no versioned rows
    /// exist); rows key on dialect family alone.
    pub fn load(conn: &Connection) -> rusqlite::Result<Self> {
        let mut render: HashMap<String, HashMap<String, RenderRule>> = HashMap::new();
        let mut intrinsic_render: HashMap<
            String,
            HashMap<(IntrinsicRenderKind, Intrinsic), RenderRule>,
        > = HashMap::new();
        let mut stmt =
            conn.prepare("SELECT dialect, render_key, rule_kind, body FROM dialect_render")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                RenderRule {
                    rule_kind: row.get(2)?,
                    body: row.get(3)?,
                },
            ))
        })?;
        for row in rows {
            let (dialect, key, rule) = row?;
            if let Some(intrinsic) = intrinsic_render_key(&key) {
                intrinsic_render
                    .entry(dialect)
                    .or_default()
                    .insert(intrinsic, rule);
            } else {
                render.entry(dialect).or_default().insert(key, rule);
            }
        }

        let mut form: HashMap<String, HashMap<EntityType, FormRules>> = HashMap::new();
        let mut stmt = conn.prepare(
            "SELECT f.dialect, f.form_type, e.name, f.rule_kind, f.body
             FROM dialect_form_rule f LEFT JOIN entity e ON e.id = f.entity_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i32>(1)?,
                row.get::<_, Option<String>>(2)?,
                RenderRule {
                    rule_kind: row.get(3)?,
                    body: row.get(4)?,
                },
            ))
        })?;
        for row in rows {
            let (dialect, form_type, entity_name, rule) = row?;
            // Unknown form_type = a row from a newer/corrupt catalog; convert
            // at the load border (STRING-FLOOR Tier 2c), refuse loudly.
            let form_type = EntityType::from_i32(form_type).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Integer,
                    format!("dialect_form_rule.form_type: {}", e).into(),
                )
            })?;
            let rules = form
                .entry(dialect)
                .or_default()
                .entry(form_type)
                .or_default();
            match entity_name {
                Some(name) => {
                    rules.per_entity.insert(name, rule);
                }
                None => rules.default = Some(rule),
            }
        }
        Ok(DialectPack {
            render,
            intrinsic_render,
            form,
        })
    }

    /// Look up the render rule for a (dialect-family, render_key) pair.
    /// `None` means canonical: the caller uses its code default.
    pub fn render(&self, dialect: &str, key: &str) -> Option<&RenderRule> {
        self.render.get(dialect)?.get(key)
    }

    pub fn render_intrinsic_function(
        &self,
        dialect: &str,
        intrinsic: Intrinsic,
    ) -> Option<&RenderRule> {
        self.intrinsic_render
            .get(dialect)?
            .get(&(IntrinsicRenderKind::Function, intrinsic))
    }

    pub fn render_intrinsic_tvf(&self, dialect: &str, intrinsic: Intrinsic) -> Option<&RenderRule> {
        self.intrinsic_render
            .get(dialect)?
            .get(&(IntrinsicRenderKind::Tvf, intrinsic))
    }

    /// Look up the form-lowering rule for a functor invocation, with the
    /// precedence: (entity + form + dialect) → (form + dialect
    /// default) → `None` = canonical code lowering.
    pub fn form_rule(
        &self,
        dialect: &str,
        form_type: EntityType,
        entity_name: &str,
    ) -> Option<&RenderRule> {
        let rules = self.form.get(dialect)?.get(&form_type)?;
        rules.per_entity.get(entity_name).or(rules.default.as_ref())
    }
}

// ---------------------------------------------------------------------------
// rust_handler interpreter: the rule body names a compiled
// lowering fn for renders a positional template cannot express — argument
// TRANSFORMATION (a '$.a.b' path literal rewritten to PG '{a,b}' spelling)
// or argument SYNTHESIS (group_concat's implicit default separator).
// ---------------------------------------------------------------------------

/// A compiled render handler: rendered argument texts + the call's DISTINCT
/// flag → the rendered call. Errors are loud (they surface as generator
/// errors naming the render key).
pub type RustRenderHandler = fn(args: &[&str], distinct: bool) -> Result<String, String>;

/// Resolve a `rust_handler` rule body to its compiled handler. A body with
/// no entry here is a loud "unknown rust_handler" error at render time.
pub fn rust_render_handler(key: &str) -> Option<RustRenderHandler> {
    match key {
        "pg_json_path_text" => Some(pg_json_path_text),
        "pg_json_path_jsonb" => Some(pg_json_path_jsonb),
        "pg_group_concat" => Some(pg_group_concat),
        "pg_scalar_max" => Some(pg_scalar_max),
        "pg_scalar_min" => Some(pg_scalar_min),
        _ => None,
    }
}

/// SQLite's scalar `max(a, b, ...)` → NULL-propagating `GREATEST`.
///
/// The bare rename is semantically wrong: sqlite's scalar max/min return
/// NULL when ANY argument is NULL, while pg's GREATEST/LEAST IGNORE
/// NULLs (measured: `max(age * 2, 18)` with a NULL age — sqlite NULL,
/// bare GREATEST 18). The fidelity rule (the +like → ILIKE lesson) says
/// preserve canonical semantics, and the NULL guard is variadic — a
/// per-argument CASE a positional template cannot express.
fn pg_scalar_max(args: &[&str], distinct: bool) -> Result<String, String> {
    pg_scalar_extreme(args, distinct, "GREATEST")
}

/// SQLite's scalar `min(a, b, ...)` → NULL-propagating `LEAST`.
fn pg_scalar_min(args: &[&str], distinct: bool) -> Result<String, String> {
    pg_scalar_extreme(args, distinct, "LEAST")
}

fn pg_scalar_extreme(args: &[&str], distinct: bool, fn_name: &str) -> Result<String, String> {
    if distinct {
        return Err("DISTINCT is not valid on a scalar max/min".into());
    }
    if args.len() < 2 {
        return Err(format!(
            "scalar {} takes 2+ args, got {} (1-arg is the aggregate form)",
            fn_name.to_lowercase(),
            args.len()
        ));
    }
    let null_guard = args
        .iter()
        .map(|a| format!("{} IS NULL", a))
        .collect::<Vec<_>>()
        .join(" OR ");
    Ok(format!(
        "CASE WHEN {} THEN NULL ELSE {}({}) END",
        null_guard,
        fn_name,
        args.join(", ")
    ))
}

/// `json_extract(x, '$.a.b')` → `(CAST(x AS jsonb) #>> '{a,b}')` — the
/// text-returning flavor for user-facing scalar reads (strings unquoted,
/// matching SQLite; numbers become text → typed compares remain the known
/// residual, same boundary as duckdb's `json_extract_string`).
fn pg_json_path_text(args: &[&str], distinct: bool) -> Result<String, String> {
    pg_json_path(args, distinct, "#>>")
}

/// `json_extract(x, '$.a.b')` → `(CAST(x AS jsonb) #> '{a,b}')` — the
/// json-returning flavor for `__dql_json_extract_raw` (subtrees stay json).
fn pg_json_path_jsonb(args: &[&str], distinct: bool) -> Result<String, String> {
    pg_json_path(args, distinct, "#>")
}

fn pg_json_path(args: &[&str], distinct: bool, op: &str) -> Result<String, String> {
    if distinct {
        return Err("DISTINCT is not valid on a json path read".into());
    }
    let [source, path] = args else {
        return Err(format!("json path read takes 2 args, got {}", args.len()));
    };
    let elems = parse_sqlite_json_path(path)?;
    Ok(format!(
        "(CAST({} AS jsonb) {} '{{{}}}')",
        source,
        op,
        elems.join(",")
    ))
}

/// Parse a RENDERED SQLite json-path literal (`'$.a.b[0]'`, single-quoted
/// SQL text) into PG text-array path elements. Only literal paths are
/// supported — a dynamic path expression is a loud error, as is any key
/// needing PG array-literal quoting (none exist in the measured corpus).
fn parse_sqlite_json_path(rendered: &str) -> Result<Vec<String>, String> {
    let inner = rendered
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .ok_or_else(|| format!("json path must be a string literal, got `{rendered}`"))?;
    let body = inner
        .strip_prefix('$')
        .ok_or_else(|| format!("json path must start with '$': `{inner}`"))?;
    let mut elems = Vec::new();
    let mut chars = body.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            // A key the renderer QUOTED is unquoted here, undoing exactly
            // the escaping it applied. The two are one law: the SQLite
            // renderer quotes every structural key so a key carrying a `.`
            // or a `"` cannot re-enter the path as syntax, and a reader
            // that cannot undo that reads its own output as unparseable.
            '.' if chars.peek() == Some(&'"') => {
                chars.next();
                let mut key = String::new();
                loop {
                    match chars.next() {
                        Some('\\') => match chars.next() {
                            Some(escaped) => key.push(escaped),
                            None => return Err(format!("json path ends in `\\`: `{inner}`")),
                        },
                        Some('"') => break,
                        Some(c) => key.push(c),
                        None => {
                            return Err(format!("unterminated quoted key in json path `{inner}`"))
                        }
                    }
                }
                if key.is_empty() {
                    return Err(format!("empty key in json path `{inner}`"));
                }
                elems.push(key);
            }
            '.' => {
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                if key.is_empty() {
                    return Err(format!("empty key in json path `{inner}`"));
                }
                if !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    return Err(format!(
                        "json path key `{key}` needs quoting rules not yet implemented"
                    ));
                }
                elems.push(key);
            }
            '[' => {
                let mut idx = String::new();
                for c in chars.by_ref() {
                    if c == ']' {
                        break;
                    }
                    idx.push(c);
                }
                if idx.is_empty() || !idx.chars().all(|c| c.is_ascii_digit()) {
                    return Err(format!("bad array index `[{idx}]` in json path `{inner}`"));
                }
                elems.push(idx);
            }
            other => {
                return Err(format!("unexpected `{other}` in json path `{inner}`"));
            }
        }
    }
    if elems.is_empty() {
        return Err(format!(
            "root-only json path `{inner}` has no PG array form"
        ));
    }
    Ok(elems)
}

/// `group_concat(x)` → `string_agg(x::text, ',')` (SQLite's implicit
/// default separator, which a name-keyed template cannot synthesize);
/// `group_concat(x, sep)` → `string_agg(x::text, sep)`. DISTINCT passes
/// through — but PG rejects DISTINCT with a separate ORDER BY... none in
/// the corpus; string_agg(DISTINCT x, sep) itself is valid.
fn pg_group_concat(args: &[&str], distinct: bool) -> Result<String, String> {
    let prefix = if distinct { "DISTINCT " } else { "" };
    match args {
        [x] => Ok(format!("string_agg({}CAST({} AS text), ',')", prefix, x)),
        [x, sep] => Ok(format!(
            "string_agg({}CAST({} AS text), {})",
            prefix, x, sep
        )),
        _ => Err(format!(
            "group_concat takes 1 or 2 args, got {}",
            args.len()
        )),
    }
}

/// Apply a positional template: `{0}`..`{9}` substitute the argument at
/// that index; `{*}` substitutes all arguments comma-joined. Anything else
/// (unbalanced brace, out-of-range index, an argument the template never
/// consumes) is an error — rule bodies are data and must fail loudly, not
/// mangle SQL. The unconsumed-arg check matters: a 1-arg template silently
/// dropping a caller's 2nd argument would emit wrong-but-valid SQL.
pub fn apply_template(template: &str, args: &[&str]) -> Result<String, String> {
    let mut out = String::with_capacity(template.len() + 16);
    let mut consumed = vec![false; args.len()];
    let mut chars = template.char_indices();
    while let Some((_, ch)) = chars.next() {
        if ch != '{' {
            out.push(ch);
            continue;
        }
        let mut placeholder = String::new();
        let mut closed = false;
        for (_, inner) in chars.by_ref() {
            if inner == '}' {
                closed = true;
                break;
            }
            placeholder.push(inner);
        }
        if !closed {
            return Err(format!("unclosed '{{' in template '{template}'"));
        }
        if placeholder == "*" {
            out.push_str(&args.join(", "));
            consumed.iter_mut().for_each(|c| *c = true);
        } else {
            let idx: usize = placeholder.parse().map_err(|_| {
                format!("bad placeholder '{{{placeholder}}}' in template '{template}'")
            })?;
            let arg = args.get(idx).ok_or_else(|| {
                format!(
                    "template '{template}' wants arg {idx} but only {} given",
                    args.len()
                )
            })?;
            consumed[idx] = true;
            out.push_str(arg);
        }
    }
    if let Some(idx) = consumed.iter().position(|c| !c) {
        return Err(format!(
            "template '{template}' never consumes arg {idx} ({} args given) — refusing to drop it",
            args.len()
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The SQLite renderer quotes every structural json-path key; this
    /// reader must undo exactly that, or a path the compiler itself wrote
    /// is unreadable on the dialect that needs it decomposed. No query
    /// shows the difference — both spellings mean the same key on SQLite,
    /// and the pg road is the only one that must take it apart.
    #[test]
    fn quoted_json_path_keys_round_trip_to_pg_elements() {
        assert_eq!(
            parse_sqlite_json_path("'$.\"rows\"'").unwrap(),
            vec!["rows".to_string()]
        );
        assert_eq!(
            parse_sqlite_json_path("'$.\"a.b\".c[2]'").unwrap(),
            vec!["a.b".to_string(), "c".to_string(), "2".to_string()]
        );
        assert_eq!(
            parse_sqlite_json_path("'$.\"say \\\"hi\\\"\"'").unwrap(),
            vec!["say \"hi\"".to_string()]
        );
        assert_eq!(
            parse_sqlite_json_path("'$.plain'").unwrap(),
            vec!["plain".to_string()]
        );
        assert!(parse_sqlite_json_path("'$.\"unterminated'").is_err());
    }

    fn seeded_pack() -> DialectPack {
        let conn = Connection::open_in_memory().unwrap();
        crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
        DialectPack::load(&conn).unwrap()
    }

    #[test]
    fn sqlite_is_canonical_no_rows() {
        let pack = seeded_pack();
        assert!(pack.render("sqlite", "op.not_equal").is_none());
        assert!(pack.render("sqlite", "lit.bool_true").is_none());
        assert!(pack.render("sqlite", "ident.quoted").is_none());
    }

    #[test]
    fn seed_rows_reproduce_the_old_match_arms() {
        let pack = seeded_pack();
        let body = |d: &str, k: &str| pack.render(d, k).unwrap().template().unwrap().to_string();
        // postgres deltas
        assert_eq!(body("postgres", "lit.bool_true"), "TRUE");
        assert_eq!(body("postgres", "lit.bool_false"), "FALSE");
        assert!(pack.render("postgres", "op.not_equal").is_none()); // != like sqlite
                                                                    // mysql deltas — concatenate renders as the CONCAT(...) function
                                                                    // call, never an infix operator token (mysql's CONCAT is a
                                                                    // function, not an operator), so the row is an op template over
                                                                    // both operands.
        assert_eq!(body("mysql", "op.not_equal"), "<>");
        assert_eq!(body("mysql", "op.concatenate"), "CONCAT({0}, {1})");
        assert_eq!(body("mysql", "op.is_not_distinct_from"), "<=>");
        assert_eq!(body("mysql", "op.is_distinct_from"), "NOT ({0} <=> {1})");
        assert_eq!(body("mysql", "ident.quoted"), "`{0}`");
        assert!(pack.render("mysql", "lit.bool_true").is_none()); // 1/0 like sqlite
                                                                  // sqlserver deltas
        assert_eq!(body("sqlserver", "op.not_equal"), "<>");
        assert_eq!(body("sqlserver", "op.concatenate"), "+");
        assert_eq!(body("sqlserver", "lit.bool_true"), "TRUE");
        assert_eq!(body("sqlserver", "ident.quoted"), "[{0}]");
    }

    #[test]
    fn unknown_rule_kind_is_a_loud_error() {
        let rule = RenderRule {
            rule_kind: "lua".to_string(),
            body: "whatever".to_string(),
        };
        assert!(rule.template().is_err());
    }

    #[test]
    fn form_rule_precedence() {
        // entity+form+dialect beats form+dialect default beats None.
        let conn = Connection::open_in_memory().unwrap();
        crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
        conn.execute_batch(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, connected, is_universal)
                 VALUES (1, 4, 'test://pack', 1, 1);
             INSERT INTO entity (name, type, cartridge_id) VALUES ('like', 15, 1);
             INSERT INTO dialect_form_rule (form_type, dialect, entity_id, rule_kind, body)
                 VALUES (15, 'postgres', NULL, 'template', 'FORM-DEFAULT');
             INSERT INTO dialect_form_rule (form_type, dialect, entity_id, rule_kind, body)
                 VALUES (15, 'postgres', 1, 'template', 'ENTITY-RULE');",
        )
        .unwrap();
        let pack = DialectPack::load(&conn).unwrap();
        // entity-specific wins for 'like'
        assert_eq!(
            pack.form_rule("postgres", EntityType::BinSigmaPredicate, "like")
                .unwrap()
                .body,
            "ENTITY-RULE"
        );
        // any other entity in the form falls to the form default
        assert_eq!(
            pack.form_rule("postgres", EntityType::BinSigmaPredicate, "between")
                .unwrap()
                .body,
            "FORM-DEFAULT"
        );
        // other dialect / other form: canonical
        assert!(pack
            .form_rule("sqlite", EntityType::BinSigmaPredicate, "like")
            .is_none());
        assert!(pack
            .form_rule("postgres", EntityType::BinPseudoPredicate, "like")
            .is_none());
    }

    #[test]
    fn pg_json_path_handlers() {
        let text = rust_render_handler("pg_json_path_text").unwrap();
        assert_eq!(
            text(&["j", "'$.a.b'"], false).unwrap(),
            "(CAST(j AS jsonb) #>> '{a,b}')"
        );
        assert_eq!(
            text(&["t.col", "'$[0].x'"], false).unwrap(),
            "(CAST(t.col AS jsonb) #>> '{0,x}')"
        );
        let jsonb = rust_render_handler("pg_json_path_jsonb").unwrap();
        assert_eq!(
            jsonb(&["j", "'$.scripts'"], false).unwrap(),
            "(CAST(j AS jsonb) #> '{scripts}')"
        );
        // loud errors: dynamic path, root-only path, DISTINCT, bad index
        assert!(text(&["j", "some_expr"], false).is_err());
        assert!(text(&["j", "'$'"], false).is_err());
        assert!(text(&["j", "'$.a'"], true).is_err());
        assert!(text(&["j", "'$[x]'"], false).is_err());
    }

    #[test]
    fn pg_group_concat_handler() {
        let h = rust_render_handler("pg_group_concat").unwrap();
        assert_eq!(
            h(&["name"], false).unwrap(),
            "string_agg(CAST(name AS text), ',')"
        );
        assert_eq!(
            h(&["name", "'; '"], false).unwrap(),
            "string_agg(CAST(name AS text), '; ')"
        );
        assert_eq!(
            h(&["name"], true).unwrap(),
            "string_agg(DISTINCT CAST(name AS text), ',')"
        );
        assert!(h(&[], false).is_err());
    }

    #[test]
    fn unknown_rust_handler_is_none() {
        assert!(rust_render_handler("no_such_handler").is_none());
    }

    #[test]
    fn template_substitution() {
        assert_eq!(apply_template("[{0}]", &["order"]).unwrap(), "[order]");
        assert_eq!(apply_template("`{0}`", &["a b"]).unwrap(), "`a b`");
        assert_eq!(
            apply_template("json_object({*})", &["'a'", "1", "'b'", "2"]).unwrap(),
            "json_object('a', 1, 'b', 2)"
        );
        assert_eq!(
            apply_template("{0} ->> {1}", &["col", "'$.x'"]).unwrap(),
            "col ->> '$.x'"
        );
        assert!(apply_template("{5}", &["only"]).is_err());
        assert!(apply_template("{oops", &[]).is_err());
        assert!(apply_template("{x}", &["a"]).is_err());
        // a template must consume every argument — dropping one silently
        // would emit wrong-but-valid SQL
        assert!(apply_template("string_agg({0}, ',')", &["x", "'; '"]).is_err());
        assert_eq!(
            apply_template("string_agg({0}, ',')", &["x"]).unwrap(),
            "string_agg(x, ',')"
        );
    }
}
