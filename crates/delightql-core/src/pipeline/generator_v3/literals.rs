// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::dialect::SqlDialect;
use super::errors::GeneratorError;
use crate::pipeline::ast_refined::LiteralValue;
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
        LiteralValue::Number(n) => {
            // Numbers are stored as strings, output them directly
            sql.push_str(n);
        }
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
    }
    Ok(())
}
