// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::dialect::SqlDialect;
use super::errors::GeneratorError;
use crate::pipeline::asts::core::LiteralValue;
use crate::pipeline::asts::core::NumericCategory;
use crate::pipeline::dialect_pack::DialectPack;

pub fn generate_literal(
    sql: &mut String,
    value: &LiteralValue,
    dialect: SqlDialect,
    pack: &DialectPack,
) -> Result<(), GeneratorError> {
    // Canonical (SQLite) spelling unless the pack carries a render override.
    let spell = |sql: &mut String, key: &str, canonical: &str| -> Result<(), GeneratorError> {
        match pack.render(dialect.family_name(), key) {
            Some(rule) => sql.push_str(rule.template().map_err(GeneratorError::Error)?),
            None => sql.push_str(canonical),
        }
        Ok(())
    };
    match value {
        LiteralValue::String(s) => {
            if s.contains('\0') {
                if dialect == SqlDialect::PostgreSQL {
                    return Err(GeneratorError::Typed(
                        crate::diagnostic::Constraint::Unsupported {
                            message: "NUL-bearing text is not representable by PostgreSQL; use a target-supported text value without a zero byte"
                                .to_string(),
                        }
                        .into(),
                    ));
                }
                // A NUL is valid text data but cannot occur inside SQL source.
                // Reconstruct the authored string from target expressions so
                // the value reaches the engine without embedding a terminator
                // in the generated statement.
                let (concat, codepoint) = match dialect {
                    SqlDialect::SQLite => (" || ", "char"),
                    SqlDialect::PostgreSQL | SqlDialect::DuckDB => (" || ", "chr"),
                    SqlDialect::MySQL => (", ", "CHAR"),
                    SqlDialect::SqlServer => (" + ", "CHAR"),
                };
                let mut terms = Vec::new();
                let mut text = String::new();
                let flush = |terms: &mut Vec<String>, text: &mut String| {
                    let mut term = String::from("'");
                    for ch in text.drain(..) {
                        if ch == '\'' {
                            term.push_str("''");
                        } else {
                            term.push(ch);
                        }
                    }
                    term.push('\'');
                    terms.push(term);
                };
                for ch in s.chars() {
                    if ch == '\0' {
                        flush(&mut terms, &mut text);
                        terms.push(format!("{codepoint}(0)"));
                    } else {
                        text.push(ch);
                    }
                }
                flush(&mut terms, &mut text);
                if dialect == SqlDialect::MySQL {
                    sql.push_str("CONCAT(");
                    sql.push_str(&terms.join(concat));
                    sql.push(')');
                } else {
                    sql.push('(');
                    sql.push_str(&terms.join(concat));
                    sql.push(')');
                }
            } else {
                sql.push('\'');
                // Escape single quotes by doubling them
                for ch in s.chars() {
                    if ch == '\'' {
                        sql.push_str("''");
                    } else {
                        sql.push(ch);
                    }
                }
                sql.push('\'');
            }
        }
        LiteralValue::Number(n) => match n.category() {
            // An integer or decimal spelling is target syntax as written.
            NumericCategory::Integer | NumericCategory::Decimal => sql.push_str(n.spelling()),
            // The approximate category is stated to the target explicitly
            // where its own reading of the exponent spelling would be
            // exact (PostgreSQL's `numeric`); every other target reads
            // the spelling as its binary floating-point type, which is
            // the canonical rendering.
            NumericCategory::Approximate => {
                match pack.render(dialect.family_name(), "lit.approximate") {
                    Some(rule) => {
                        let template = rule.template().map_err(GeneratorError::Error)?;
                        let rendered = crate::pipeline::dialect_pack::apply_template(
                            template,
                            &[n.spelling()],
                        )
                        .map_err(GeneratorError::Error)?;
                        sql.push_str(&rendered);
                    }
                    None => sql.push_str(n.spelling()),
                }
            }
        },
        LiteralValue::Boolean(b) => {
            if *b {
                spell(sql, "lit.bool_true", "1")?;
            } else {
                spell(sql, "lit.bool_false", "0")?;
            }
        }
        LiteralValue::Null => {
            spell(sql, "lit.null", "NULL")?;
        }
        // A symbol is byte-identical to its spelling at execution:
        // ::active emits the string literal '::active'.
        LiteralValue::Symbol(name) => {
            sql.push_str("'::");
            sql.push_str(name);
            sql.push('\'');
        }
        // A mention is byte-identical to its encoding at execution,
        // marker included: :`people(*)` emits the string literal
        // ':`people(*)`'. The canonical interior may itself contain
        // quotes (a term can hold string literals), so it escapes
        // like any string.
        LiteralValue::Mention(canonical) => {
            sql.push_str("':`");
            for ch in canonical.chars() {
                if ch == '\'' {
                    sql.push_str("''");
                } else {
                    sql.push(ch);
                }
            }
            sql.push_str("`'");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(value: &str, dialect: SqlDialect) -> String {
        let mut sql = String::new();
        generate_literal(
            &mut sql,
            &LiteralValue::String(value.to_string()),
            dialect,
            &DialectPack::default(),
        )
        .unwrap();
        sql
    }

    #[test]
    fn nul_is_reconstructed_without_nul_in_sql_source() {
        let sql = render("a\0b\0", SqlDialect::SQLite);
        assert_eq!(sql, "('a' || char(0) || 'b' || char(0) || '')");
        assert!(!sql.contains('\0'));
    }

    #[test]
    fn postgres_refuses_nul_text_before_sql_generation() {
        let mut sql = String::new();
        let error = generate_literal(
            &mut sql,
            &LiteralValue::String("x\0y".to_string()),
            SqlDialect::PostgreSQL,
            &DialectPack::default(),
        )
        .expect_err("PostgreSQL text cannot contain a zero byte");
        let GeneratorError::Typed(error) = error else {
            panic!("NUL refusal must retain its semantic identity");
        };
        assert_eq!(
            error.error_uri(),
            "delightql-error://semantic/constraint/unsupported"
        );
        assert!(sql.is_empty());
    }

    #[test]
    fn nul_uses_each_supported_targets_string_construction() {
        assert_eq!(render("x\0y", SqlDialect::DuckDB), "('x' || chr(0) || 'y')");
        assert_eq!(
            render("x\0y", SqlDialect::MySQL),
            "CONCAT('x', CHAR(0), 'y')"
        );
        assert_eq!(
            render("x\0y", SqlDialect::SqlServer),
            "('x' + CHAR(0) + 'y')"
        );
    }
}
