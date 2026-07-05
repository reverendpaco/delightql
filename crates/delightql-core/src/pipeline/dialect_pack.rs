// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Per-compile dialect pack: the in-memory image of the `dialect_*`
//! bootstrap tables (ALL-SQL-TARGETING-DESIGN.md §4, §7.11).
//!
//! Loaded once at the start of each query compile — alongside the other
//! bootstrap-metadata reads — and handed to the target-aware stages
//! (lowering, generator spelling) as a plain map, so the generator stays
//! a pure in-memory walk and never holds a DB handle. SQLite is the
//! canonical baseline and has no rows; a lookup miss means "use the
//! code-resident canonical default" (DESIGN §7.10).

use rusqlite::Connection;
use std::collections::HashMap;

/// One rule body plus its interpreter discriminator (DESIGN §4.4).
#[derive(Debug, Clone)]
pub struct RenderRule {
    pub rule_kind: String,
    pub body: String,
}

impl RenderRule {
    /// The body as a positional template — the only interpreter the render
    /// layer accepts today. A row whose kind has no interpreter is a loud
    /// error, never a silent fallback (DESIGN §4.4).
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
/// the form-wide default (DESIGN §4.1's two grains via nullable entity_id).
#[derive(Debug, Default)]
struct FormRules {
    default: Option<RenderRule>,
    per_entity: HashMap<String, RenderRule>,
}

/// The resolved rule data for all dialects, keyed dialect-family → render_key.
#[derive(Debug, Default)]
pub struct DialectPack {
    render: HashMap<String, HashMap<String, RenderRule>>,
    /// dialect-family → form_type (entity_type_enum id) → rules.
    form: HashMap<String, HashMap<i32, FormRules>>,
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
            render.entry(dialect).or_default().insert(key, rule);
        }

        let mut form: HashMap<String, HashMap<i32, FormRules>> = HashMap::new();
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
        Ok(DialectPack { render, form })
    }

    /// Look up the render rule for a (dialect-family, render_key) pair.
    /// `None` means canonical: the caller uses its code default.
    pub fn render(&self, dialect: &str, key: &str) -> Option<&RenderRule> {
        self.render.get(dialect)?.get(key)
    }

    /// Look up the form-lowering rule for a functor invocation, with the
    /// DESIGN §4.1 precedence: (entity + form + dialect) → (form + dialect
    /// default) → `None` = canonical code lowering.
    pub fn form_rule(
        &self,
        dialect: &str,
        form_type: i32,
        entity_name: &str,
    ) -> Option<&RenderRule> {
        let rules = self.form.get(dialect)?.get(&form_type)?;
        rules.per_entity.get(entity_name).or(rules.default.as_ref())
    }
}

// ---------------------------------------------------------------------------
// rust_handler interpreter (DESIGN §4.4): the rule body names a compiled
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
        _ => None,
    }
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
    Ok(format!("(CAST({} AS jsonb) {} '{{{}}}')", source, op, elems.join(",")))
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
        return Err(format!("root-only json path `{inner}` has no PG array form"));
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
        [x, sep] => Ok(format!("string_agg({}CAST({} AS text), {})", prefix, x, sep)),
        _ => Err(format!("group_concat takes 1 or 2 args, got {}", args.len())),
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
            let idx: usize = placeholder
                .parse()
                .map_err(|_| format!("bad placeholder '{{{placeholder}}}' in template '{template}'"))?;
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
        // mysql deltas
        assert_eq!(body("mysql", "op.not_equal"), "<>");
        assert_eq!(body("mysql", "op.concatenate"), "CONCAT");
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
        assert_eq!(pack.form_rule("postgres", 15, "like").unwrap().body, "ENTITY-RULE");
        // any other entity in the form falls to the form default
        assert_eq!(
            pack.form_rule("postgres", 15, "between").unwrap().body,
            "FORM-DEFAULT"
        );
        // other dialect / other form: canonical
        assert!(pack.form_rule("sqlite", 15, "like").is_none());
        assert!(pack.form_rule("postgres", 14, "like").is_none());
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
