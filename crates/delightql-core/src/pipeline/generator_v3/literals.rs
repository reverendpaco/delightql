use super::dialect::SqlDialect;
use super::errors::GeneratorError;
use crate::pipeline::ast_refined::LiteralValue;

pub fn generate_literal(
    sql: &mut String,
    value: &LiteralValue,
    dialect: SqlDialect,
) -> Result<(), GeneratorError> {
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
            // Handle dialect differences
            match dialect {
                SqlDialect::SQLite | SqlDialect::MySQL => {
                    sql.push_str(if *b { "1" } else { "0" });
                }
                SqlDialect::PostgreSQL | SqlDialect::SqlServer => {
                    sql.push_str(if *b { "TRUE" } else { "FALSE" });
                }
            }
        }
        LiteralValue::Null => {
            sql.push_str("NULL");
        }
    }
    Ok(())
}
