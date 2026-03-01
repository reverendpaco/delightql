use super::dialect::SqlDialect;
use super::errors::GeneratorError;

pub fn write_identifier(
    sql: &mut String,
    ident: &str,
    dialect: SqlDialect,
) -> Result<(), GeneratorError> {
    if needs_quoting(ident) {
        match dialect {
            SqlDialect::SQLite | SqlDialect::PostgreSQL => {
                sql.push('"');
                sql.push_str(ident);
                sql.push('"');
            }
            SqlDialect::MySQL => {
                sql.push('`');
                sql.push_str(ident);
                sql.push('`');
            }
            SqlDialect::SqlServer => {
                sql.push('[');
                sql.push_str(ident);
                sql.push(']');
            }
        }
    } else {
        sql.push_str(ident);
    }

    Ok(())
}

/// Returns true if an identifier needs quoting in SQL output.
///
/// Plain identifiers matching [a-zA-Z_][a-zA-Z0-9_]* that are not
/// SQL reserved words are emitted unquoted. This lets each backend
/// apply its native case-folding (uppercase on Snowflake, lowercase
/// on PostgreSQL, case-insensitive on SQLite).
pub fn needs_quoting(ident: &str) -> bool {
    if ident.is_empty() {
        return true;
    }

    // Must match [a-zA-Z_][a-zA-Z0-9_]*
    let mut chars = ident.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return true,
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return true;
        }
    }

    // Check against reserved words (case-insensitive)
    is_reserved_word(ident)
}

/// Common SQL reserved words across major dialects.
///
/// This covers SQL:2016 reserved words plus dialect-specific additions
/// for SQLite, PostgreSQL, MySQL, SQL Server, DuckDB, and Snowflake.
/// Not exhaustive, but covers the words most likely to appear as
/// column or table names.
fn is_reserved_word(word: &str) -> bool {
    // Binary search on a sorted array for O(log n) lookup.
    RESERVED_WORDS.binary_search_by(|probe| {
        probe.as_bytes().iter()
            .zip(word.as_bytes().iter())
            .map(|(&a, &b)| a.cmp(&b.to_ascii_uppercase()))
            .find(|&ord| ord != std::cmp::Ordering::Equal)
            .unwrap_or_else(|| probe.len().cmp(&word.len()))
    }).is_ok()
}

/// Sorted uppercase. Covers SQL:2016 core + common dialect extensions.
static RESERVED_WORDS: &[&str] = &[
    "ABORT",
    "ABS",
    "ACTION",
    "ADD",
    "AFTER",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "ATTACH",
    "AUTOINCREMENT",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BIT",
    "BLOB",
    "BOOLEAN",
    "BOTH",
    "BY",
    "CASCADE",
    "CASE",
    "CAST",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "CLOB",
    "CLOSE",
    "COLLATE",
    "COLUMN",
    "COMMIT",
    "CONFLICT",
    "CONNECT",
    "CONSTRAINT",
    "COPY",
    "CREATE",
    "CROSS",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURSOR",
    "DATABASE",
    "DATE",
    "DATETIME",
    "DAY",
    "DEALLOCATE",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DEFERRABLE",
    "DEFERRED",
    "DELETE",
    "DESC",
    "DESCRIBE",
    "DETACH",
    "DISTINCT",
    "DO",
    "DOUBLE",
    "DROP",
    "EACH",
    "ELSE",
    "ELSEIF",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXCLUDE",
    "EXCLUSIVE",
    "EXEC",
    "EXECUTE",
    "EXISTS",
    "EXPLAIN",
    "EXPORT",
    "EXTERNAL",
    "EXTRACT",
    "FAIL",
    "FALSE",
    "FETCH",
    "FILTER",
    "FIRST",
    "FLOAT",
    "FOLLOWING",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "FUNCTION",
    "GLOB",
    "GRANT",
    "GROUP",
    "GROUPS",
    "HAVING",
    "HOUR",
    "IDENTITY",
    "IF",
    "IGNORE",
    "ILIKE",
    "IMMEDIATE",
    "IMPORT",
    "IN",
    "INCREMENT",
    "INDEX",
    "INDEXED",
    "INITIALLY",
    "INNER",
    "INSERT",
    "INSTEAD",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "JSON",
    "KEY",
    "LAST",
    "LATERAL",
    "LEADING",
    "LEFT",
    "LEVEL",
    "LIKE",
    "LIMIT",
    "LOCAL",
    "LOCK",
    "MATCH",
    "MATERIALIZED",
    "MERGE",
    "MINUTE",
    "MONTH",
    "NATURAL",
    "NCHAR",
    "NO",
    "NOT",
    "NOTHING",
    "NOTNULL",
    "NULL",
    "NULLIF",
    "NULLS",
    "NUMERIC",
    "OF",
    "OFFSET",
    "ON",
    "ONLY",
    "OPEN",
    "OR",
    "ORDER",
    "OTHERS",
    "OUTER",
    "OVER",
    "OVERLAPS",
    "PARTITION",
    "PLAN",
    "POSITION",
    "PRAGMA",
    "PRECEDING",
    "PRECISION",
    "PRIMARY",
    "PROCEDURE",
    "PUBLIC",
    "QUALIFY",
    "QUERY",
    "RAISE",
    "RANGE",
    "REAL",
    "RECURSIVE",
    "REFERENCES",
    "REGEXP",
    "REINDEX",
    "RELEASE",
    "RENAME",
    "REPLACE",
    "RESTRICT",
    "RETURN",
    "RETURNING",
    "REVOKE",
    "RIGHT",
    "ROLLBACK",
    "ROW",
    "ROWS",
    "SAVEPOINT",
    "SCHEMA",
    "SECOND",
    "SELECT",
    "SEQUENCE",
    "SESSION",
    "SESSION_USER",
    "SET",
    "SHOW",
    "SIMILAR",
    "SMALLINT",
    "SOME",
    "START",
    "STRUCT",
    "TABLE",
    "TEMP",
    "TEMPORARY",
    "TEXT",
    "THEN",
    "TIES",
    "TIME",
    "TIMESTAMP",
    "TINYINT",
    "TO",
    "TOP",
    "TRAILING",
    "TRANSACTION",
    "TRIGGER",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "TYPE",
    "UNBOUNDED",
    "UNION",
    "UNIQUE",
    "UNNEST",
    "UPDATE",
    "UPPER",
    "USE",
    "USER",
    "USING",
    "VACUUM",
    "VALUES",
    "VARCHAR",
    "VARYING",
    "VIEW",
    "VIRTUAL",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "WITHOUT",
    "WORK",
    "YEAR",
    "ZONE",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reserved_words_sorted() {
        for pair in RESERVED_WORDS.windows(2) {
            assert!(
                pair[0] < pair[1],
                "RESERVED_WORDS not sorted: {:?} >= {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn test_plain_identifiers_not_quoted() {
        assert!(!needs_quoting("country"));
        assert!(!needs_quoting("COUNTRY"));
        assert!(!needs_quoting("first_name"));
        assert!(!needs_quoting("col1"));
        assert!(!needs_quoting("_private"));
    }

    #[test]
    fn test_reserved_words_quoted() {
        assert!(needs_quoting("select"));
        assert!(needs_quoting("SELECT"));
        assert!(needs_quoting("from"));
        assert!(needs_quoting("table"));
        assert!(needs_quoting("user"));
        assert!(needs_quoting("column"));
        assert!(needs_quoting("index"));
        assert!(needs_quoting("json"));
        assert!(needs_quoting("commit"));
    }

    #[test]
    fn test_special_chars_quoted() {
        assert!(needs_quoting("has space"));
        assert!(needs_quoting("with-dash"));
        assert!(needs_quoting("123start"));
        assert!(needs_quoting(""));
    }
}
