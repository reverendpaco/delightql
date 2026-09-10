// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::dialect::SqlDialect;
use super::errors::GeneratorError;
use crate::pipeline::dialect_pack::{apply_template, DialectPack};

pub fn write_identifier_with_stropping(
    sql: &mut String,
    ident: &str,
    stropped: bool,
    dialect: SqlDialect,
    pack: &DialectPack,
) -> Result<(), GeneratorError> {
    if stropped || needs_quoting(ident) {
        write_delimited(sql, ident, dialect, pack)
    } else {
        sql.push_str(ident);
        Ok(())
    }
}

/// Write `ident` delimited for `dialect`, whatever the word. The callers
/// decide WHEN a delimiter is owed (an identifier position by
/// [`needs_quoting`] or a strop; a callee position by
/// [`callee_needs_quoting`]); this writes it lawfully.
pub fn write_delimited(
    sql: &mut String,
    ident: &str,
    dialect: SqlDialect,
    pack: &DialectPack,
) -> Result<(), GeneratorError> {
    {
        // Canonical double-quote unless the pack carries an `ident.quoted`
        // template ('`{0}`' for mysql, '[{0}]' for sqlserver).
        match pack.render(dialect.family_name(), "ident.quoted") {
            Some(rule) => {
                // A positional template cannot be responsible for escaping:
                // identifier bytes come from DATA (pivot keys become column
                // names), so the delimiter that closes the quoted token must
                // be doubled BEFORE interpolation. The pack pairs every
                // `ident.quoted` with `ident.escape` (the character to
                // double); a pack that omits it refuses loudly here rather
                // than let data rewrite the SQL token stream.
                let escape = pack
                    .render(dialect.family_name(), "ident.escape")
                    .ok_or_else(|| {
                        GeneratorError::Error(format!(
                            "dialect pack for '{}' carries ident.quoted without \
                             ident.escape — identifier escaping would be skipped",
                            dialect.family_name()
                        ))
                    })?
                    .template()
                    .map_err(GeneratorError::Error)?;
                let doubled = format!("{escape}{escape}");
                let escaped = ident.replace(escape, &doubled);
                let template = rule.template().map_err(GeneratorError::Error)?;
                let quoted =
                    apply_template(template, &[&escaped]).map_err(GeneratorError::Error)?;
                sql.push_str(&quoted);
            }
            None => {
                // Embedded quotes DOUBLE inside a quoted identifier — a
                // data-derived name (a pivot key becoming a column) must
                // never break out of its delimiters.
                sql.push('"');
                sql.push_str(&ident.replace('"', "\"\""));
                sql.push('"');
            }
        }
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

/// Whether a target would misread the bare spelling as a keyword.
///
/// THIS IS OUTPUT KNOWLEDGE, NOT SOURCE LAW: no word is refused as a
/// DelightQL name, so every word any supported target reserves must be
/// here, or the emission of an admitted name is unlawful SQL on that
/// target. A word appears when SQLite, PostgreSQL, DuckDB, MySQL or SQL
/// Server refuses it unquoted in relation or column position; a word
/// only one of them reserves is quoted for all, since delimiting a lawful
/// name is always lawful and the spelling stays target-independent. A
/// missing word is an emission defect to fix here, never a reason to
/// restore a source ban.
fn is_reserved_word(word: &str) -> bool {
    contains_case_insensitive(RESERVED_WORDS, word)
}

/// Whether the bare spelling of `name` fails to reach a callable of that
/// name on `dialect`.
///
/// A CALLEE POSITION HAS ITS OWN LAW, and it is about INVOCATION, not about
/// whether the text parses: `not(1)` parses on every target and is Boolean
/// negation, never a call of a function named `not`; `coalesce(1)` reaches
/// the target's own coalesce, which IS the callable that name denotes. So a
/// word is delimited exactly when the bare spelling can reach no callable
/// of that name — an operator, a clause word, a type name — and left bare
/// when it reaches one, whether a built-in construct or a user function.
/// Delimiting by the identifier inventory instead would turn working
/// built-in calls into user-function lookups on the targets that treat a
/// delimited callee that way.
///
/// The lists: for SQLite, every word for which a function registered under
/// that name is NOT reached by `SELECT w(1)` (and `SELECT w(1) OVER ()`) —
/// an application-defined function shadows every built-in there, so a miss
/// means the bare text is not a call of any callable named `w`. For
/// PostgreSQL and DuckDB a word is delimited when BOTH hold: a registered
/// function of that name is not reached bare, AND the bare spelling is not
/// a NATIVE CALL FORM of the engine. A native call form is measured, not
/// read off a keyword category — the engine's categories say "reserved"
/// about `current_timestamp(2)` and `coalesce(a, b)` alike — as: some
/// parenthesized argument list succeeds while the same text without the
/// parentheses is a syntax error (so the parentheses ARE a call:
/// `current_timestamp(1)` yes, `not(1)` no — `not 1` parses the same way),
/// or, for a word the engine classes "cannot be a function name", some
/// argument list is at least not syntax (`grouping(x)`, `trim(x)`).
/// A built-in the engine will not let a user shadow under either spelling
/// (DuckDB's `unnest`, `date`) may sit in a list inertly: its delimited
/// call reaches the engine's own function exactly as the bare one does.
/// MySQL and SQL Server, which have no execution lane, take the words all
/// three measured engines delimit — a floor every SQL grammar shares, not an
/// inventory of theirs. `new_test_suite/callee_invocation.py` re-measures
/// the installed engines with registered functions AND with the native
/// forms unregistered, since a registered function proves only half of the
/// obligation.
pub fn callee_needs_quoting(name: &str, dialect: SqlDialect) -> bool {
    if name.is_empty() || !is_identifier_shaped(name) {
        return true;
    }
    let reserved = match dialect {
        SqlDialect::SQLite => SQLITE_RESERVED_CALLEES,
        SqlDialect::PostgreSQL => POSTGRES_RESERVED_CALLEES,
        SqlDialect::DuckDB => DUCKDB_RESERVED_CALLEES,
        SqlDialect::MySQL | SqlDialect::SqlServer => COMMON_RESERVED_CALLEES,
    };
    contains_case_insensitive(reserved, name)
}

fn is_identifier_shaped(ident: &str) -> bool {
    let mut chars = ident.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn contains_case_insensitive(sorted_upper: &[&str], word: &str) -> bool {
    sorted_upper
        .binary_search_by(|probe| {
            probe
                .as_bytes()
                .iter()
                .zip(word.as_bytes().iter())
                .map(|(&a, &b)| a.cmp(&b.to_ascii_uppercase()))
                .find(|&ord| ord != std::cmp::Ordering::Equal)
                .unwrap_or_else(|| probe.len().cmp(&word.len()))
        })
        .is_ok()
}

/// SQLite: a registered function under the name is not reached bare; sorted uppercase.
static SQLITE_RESERVED_CALLEES: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "AUTOINCREMENT",
    "BETWEEN",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COMMIT",
    "CONSTRAINT",
    "CREATE",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "DEFAULT",
    "DEFERRABLE",
    "DELETE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "ESCAPE",
    "EXCEPT",
    "EXISTS",
    "FOREIGN",
    "FROM",
    "GROUP",
    "HAVING",
    "IN",
    "INDEX",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "LIMIT",
    "NOT",
    "NOTHING",
    "NOTNULL",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "PRIMARY",
    "RAISE",
    "REFERENCES",
    "RETURNING",
    "SELECT",
    "SET",
    "TABLE",
    "THEN",
    "TO",
    "TRANSACTION",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
];

/// PostgreSQL 18: neither reached by a registered function nor a native call
/// form; sorted uppercase.
static POSTGRES_RESERVED_CALLEES: &[&str] = &[
    "ALL",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASYMMETRIC",
    "BETWEEN",
    "BIGINT",
    "BIT",
    "BOOLEAN",
    "BOTH",
    "CASE",
    "CAST",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONSTRAINT",
    "CREATE",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_USER",
    "DEC",
    "DECIMAL",
    "DEFAULT",
    "DEFERRABLE",
    "DESC",
    "DISTINCT",
    "DO",
    "ELSE",
    "END",
    "EXCEPT",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FLOAT",
    "FOR",
    "FOREIGN",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "IN",
    "INITIALLY",
    "INOUT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "JSON_OBJECTAGG",
    "JSON_TABLE",
    "LATERAL",
    "LEADING",
    "LIMIT",
    "MERGE_ACTION",
    "NATIONAL",
    "NCHAR",
    "NONE",
    "NOT",
    "NULL",
    "NUMERIC",
    "OFFSET",
    "ON",
    "ONLY",
    "OPERATOR",
    "OR",
    "ORDER",
    "OUT",
    "PLACING",
    "POSITION",
    "PRECISION",
    "PRIMARY",
    "REAL",
    "REFERENCES",
    "RETURNING",
    "SELECT",
    "SESSION_USER",
    "SETOF",
    "SMALLINT",
    "SOME",
    "SYMMETRIC",
    "SYSTEM_USER",
    "TABLE",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TO",
    "TRAILING",
    "TREAT",
    "TRUE",
    "UNION",
    "UNIQUE",
    "USER",
    "USING",
    "VALUES",
    "VARCHAR",
    "VARIADIC",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "XMLATTRIBUTES",
    "XMLELEMENT",
    "XMLEXISTS",
    "XMLNAMESPACES",
    "XMLPARSE",
    "XMLPI",
    "XMLROOT",
    "XMLSERIALIZE",
    "XMLTABLE",
];

/// DuckDB: neither reached by a registered macro nor a native call form;
/// sorted uppercase.
static DUCKDB_RESERVED_CALLEES: &[&str] = &[
    "ALL",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANTI",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASYMMETRIC",
    "BETWEEN",
    "BIGINT",
    "BIT",
    "BOOLEAN",
    "BOTH",
    "BY",
    "CASE",
    "CAST",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "COLUMNS",
    "CONSTRAINT",
    "CREATE",
    "DATE",
    "DEC",
    "DECIMAL",
    "DEFAULT",
    "DEFERRABLE",
    "DESC",
    "DESCRIBE",
    "DISTINCT",
    "DO",
    "ELSE",
    "END",
    "EXCEPT",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FLOAT",
    "FOR",
    "FOREIGN",
    "FROM",
    "GROUP",
    "HAVING",
    "IN",
    "INITIALLY",
    "INOUT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTO",
    "LAMBDA",
    "LATERAL",
    "LEADING",
    "LIMIT",
    "NATIONAL",
    "NCHAR",
    "NONE",
    "NOT",
    "NULL",
    "NUMERIC",
    "OFFSET",
    "ON",
    "ONLY",
    "OPERATOR",
    "OR",
    "ORDER",
    "OUT",
    "OVERLAY",
    "PIVOT",
    "PIVOT_LONGER",
    "PIVOT_WIDER",
    "PLACING",
    "POSITION",
    "PRECISION",
    "PRIMARY",
    "QUALIFY",
    "REAL",
    "REFERENCES",
    "RETURNING",
    "SELECT",
    "SEMI",
    "SETOF",
    "SHOW",
    "SMALLINT",
    "SOME",
    "SUMMARIZE",
    "SYMMETRIC",
    "TABLE",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TO",
    "TRAILING",
    "TREAT",
    "TRUE",
    "TRY_CAST",
    "UNION",
    "UNIQUE",
    "UNNEST",
    "UNPACK",
    "UNPIVOT",
    "USING",
    "VALUES",
    "VARCHAR",
    "VARIADIC",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "XMLATTRIBUTES",
    "XMLCONCAT",
    "XMLELEMENT",
    "XMLEXISTS",
    "XMLFOREST",
    "XMLNAMESPACES",
    "XMLPARSE",
    "XMLPI",
    "XMLROOT",
    "XMLSERIALIZE",
    "XMLTABLE",
];

/// The words every measured engine reserves as a callee — the floor the
/// text-only dialects take; sorted uppercase.
static COMMON_RESERVED_CALLEES: &[&str] = &[
    "ALL",
    "AND",
    "AS",
    "BETWEEN",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "CONSTRAINT",
    "CREATE",
    "DEFAULT",
    "DEFERRABLE",
    "DISTINCT",
    "ELSE",
    "EXCEPT",
    "EXISTS",
    "FOREIGN",
    "FROM",
    "GROUP",
    "HAVING",
    "IN",
    "INTERSECT",
    "INTO",
    "LIMIT",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "PRIMARY",
    "REFERENCES",
    "RETURNING",
    "SELECT",
    "TABLE",
    "THEN",
    "TO",
    "UNION",
    "UNIQUE",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
];

/// Sorted uppercase; the union over the five supported targets.
static RESERVED_WORDS: &[&str] = &[
    "ABORT",
    "ABS",
    "ACTION",
    "ADD",
    "AFTER",
    "ALL",
    "ALTER",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASCENDING",
    "ASYMMETRIC",
    "ATTACH",
    "AUTHORIZATION",
    "AUTOINCREMENT",
    "BACKUP",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BIT",
    "BLOB",
    "BOOLEAN",
    "BOTH",
    "BREAK",
    "BROWSE",
    "BULK",
    "BY",
    "CALL",
    "CASCADE",
    "CASE",
    "CAST",
    "CHANGE",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "CHECKPOINT",
    "CLOB",
    "CLOSE",
    "CLUSTERED",
    "COALESCE",
    "COLLATE",
    "COLLATION",
    "COLUMN",
    "COMMIT",
    "COMPUTE",
    "CONCURRENTLY",
    "CONDITION",
    "CONFLICT",
    "CONNECT",
    "CONSTRAINT",
    "CONTAINS",
    "CONTINUE",
    "CONVERT",
    "COPY",
    "CREATE",
    "CROSS",
    "CUBE",
    "CUME_DIST",
    "CURRENT",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURSOR",
    "DATABASE",
    "DATE",
    "DATETIME",
    "DAY",
    "DBCC",
    "DEALLOCATE",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DEFERRABLE",
    "DEFERRED",
    "DELETE",
    "DENSE_RANK",
    "DENY",
    "DESC",
    "DESCENDING",
    "DESCRIBE",
    "DETACH",
    "DISK",
    "DISTINCT",
    "DISTRIBUTED",
    "DIV",
    "DO",
    "DOUBLE",
    "DROP",
    "DUMP",
    "EACH",
    "ELSE",
    "ELSEIF",
    "END",
    "ERRLVL",
    "ESCAPE",
    "EXCEPT",
    "EXCLUDE",
    "EXCLUSIVE",
    "EXEC",
    "EXECUTE",
    "EXISTS",
    "EXIT",
    "EXPLAIN",
    "EXPORT",
    "EXTERNAL",
    "EXTRACT",
    "FAIL",
    "FALSE",
    "FETCH",
    "FILE",
    "FILLFACTOR",
    "FILTER",
    "FIRST",
    "FIRST_VALUE",
    "FLOAT",
    "FOLLOWING",
    "FOR",
    "FOREIGN",
    "FREETEXT",
    "FREEZE",
    "FROM",
    "FULL",
    "FUNCTION",
    "GENERATED",
    "GLOB",
    "GRANT",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HAVING",
    "HIGH_PRIORITY",
    "HOLDLOCK",
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
    "KILL",
    "LAG",
    "LAMBDA",
    "LAST",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "LEADING",
    "LEFT",
    "LEVEL",
    "LIKE",
    "LIMIT",
    "LINENO",
    "LOAD",
    "LOCAL",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCK",
    "LONG",
    "LOOP",
    "LOW_PRIORITY",
    "MATCH",
    "MATERIALIZED",
    "MERGE",
    "MINUTE",
    "MOD",
    "MONTH",
    "NATURAL",
    "NCHAR",
    "NO",
    "NOCHECK",
    "NONCLUSTERED",
    "NOT",
    "NOTHING",
    "NOTNULL",
    "NTH_VALUE",
    "NTILE",
    "NULL",
    "NULLIF",
    "NULLS",
    "NUMERIC",
    "OF",
    "OFF",
    "OFFSET",
    "OFFSETS",
    "ON",
    "ONLY",
    "OPEN",
    "OPTIMIZE",
    "OPTION",
    "OR",
    "ORDER",
    "OTHERS",
    "OUT",
    "OUTER",
    "OVER",
    "OVERLAPS",
    "PARTITION",
    "PERCENT",
    "PERCENT_RANK",
    "PIVOT",
    "PIVOT_LONGER",
    "PIVOT_WIDER",
    "PLACING",
    "PLAN",
    "POSITION",
    "PRAGMA",
    "PRECEDING",
    "PRECISION",
    "PREPARE",
    "PRIMARY",
    "PRINT",
    "PROC",
    "PROCEDURE",
    "PUBLIC",
    "PURGE",
    "QUALIFY",
    "QUERY",
    "RAISE",
    "RAISERROR",
    "RANGE",
    "RANK",
    "READ",
    "REAL",
    "RECURSIVE",
    "REFERENCES",
    "REGEXP",
    "REINDEX",
    "RELEASE",
    "RENAME",
    "REPEAT",
    "REPLACE",
    "REQUIRE",
    "RESTORE",
    "RESTRICT",
    "RETURN",
    "RETURNING",
    "REVERT",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWCOUNT",
    "ROWGUIDCOL",
    "ROWS",
    "ROW_NUMBER",
    "RULE",
    "SAVE",
    "SAVEPOINT",
    "SCHEMA",
    "SECOND",
    "SECURITYAUDIT",
    "SELECT",
    "SEMANTICKEYPHRASETABLE",
    "SEQUENCE",
    "SESSION",
    "SESSION_USER",
    "SET",
    "SETUSER",
    "SHOW",
    "SHUTDOWN",
    "SIMILAR",
    "SMALLINT",
    "SOME",
    "SPATIAL",
    "SQL",
    "START",
    "STRUCT",
    "SUMMARIZE",
    "SYMMETRIC",
    "SYSTEM_USER",
    "TABLE",
    "TABLESAMPLE",
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
    "TRAN",
    "TRANSACTION",
    "TRIGGER",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "TYPE",
    "UNBOUNDED",
    "UNION",
    "UNIQUE",
    "UNLOCK",
    "UNNEST",
    "UNPIVOT",
    "UNSIGNED",
    "UPDATE",
    "UPDATETEXT",
    "UPPER",
    "USE",
    "USER",
    "USING",
    "VACUUM",
    "VALUES",
    "VARCHAR",
    "VARIADIC",
    "VARYING",
    "VERBOSE",
    "VIEW",
    "VIRTUAL",
    "WAITFOR",
    "WHEN",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "WITHOUT",
    "WORK",
    "WRITETEXT",
    "XOR",
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

    // Words some single target reserves — PostgreSQL's ANALYSE and
    // VARIADIC, MySQL's RANK and DIV, SQL Server's PERCENT and PIVOT,
    // DuckDB's SUMMARIZE — are quoted for every target, as are DelightQL's
    // own keywords, which no target treats specially.
    #[test]
    fn every_target_reserved_and_language_keyword_is_quoted() {
        for word in [
            "analyse",
            "variadic",
            "rank",
            "div",
            "percent",
            "pivot",
            "summarize",
            "as",
            "in",
            "and",
            "or",
            "not",
            "of",
            "asc",
            "desc",
            "null",
            "true",
            "false",
        ] {
            assert!(needs_quoting(word), "{word}");
            assert!(needs_quoting(&word.to_ascii_uppercase()), "{word}");
        }
    }

    // The callee law is per dialect and narrower than the identifier law:
    // a built-in a target accepts bare stays bare, a word its parser refuses
    // as a call is delimited, and every list is sorted for the search.
    #[test]
    fn callee_quoting_follows_the_measured_target_not_the_identifier_inventory() {
        for list in [
            SQLITE_RESERVED_CALLEES,
            POSTGRES_RESERVED_CALLEES,
            DUCKDB_RESERVED_CALLEES,
            COMMON_RESERVED_CALLEES,
        ] {
            for pair in list.windows(2) {
                assert!(pair[0] < pair[1], "{} !< {}", pair[0], pair[1]);
            }
        }
        for d in [
            SqlDialect::SQLite,
            SqlDialect::PostgreSQL,
            SqlDialect::DuckDB,
            SqlDialect::MySQL,
            SqlDialect::SqlServer,
        ] {
            assert!(callee_needs_quoting("from", d), "{d:?}");
            assert!(callee_needs_quoting("SELECT", d), "{d:?}");
            // Parses everywhere, invokes nothing named `not` anywhere.
            assert!(callee_needs_quoting("not", d), "{d:?}");
            assert!(callee_needs_quoting("case", d), "{d:?}");
            assert!(!callee_needs_quoting("abs", d), "{d:?}");
            assert!(!callee_needs_quoting("replace", d), "{d:?}");
            // A construct-call keyword reaches the target's own callable bare.
            assert!(!callee_needs_quoting("coalesce", d), "{d:?}");
            assert!(!callee_needs_quoting("nullif", d), "{d:?}");

            // A word whose bare call is syntax everywhere reaches nothing bare.
            assert!(callee_needs_quoting("exists", d), "{d:?}");
            assert!(
                needs_quoting("abs"),
                "abs is still delimited as an identifier"
            );
        }
        assert!(callee_needs_quoting("lambda", SqlDialect::DuckDB));
        // A reserved word can still be a native call form: PostgreSQL answers
        // `current_timestamp(2)`, so it stays bare there; SQLite's bare
        // spelling is syntax, so a user function of that name is delimited.
        assert!(!callee_needs_quoting(
            "current_timestamp",
            SqlDialect::PostgreSQL
        ));
        assert!(callee_needs_quoting(
            "current_timestamp",
            SqlDialect::SQLite
        ));
        assert!(!callee_needs_quoting("lambda", SqlDialect::SQLite));
        assert!(callee_needs_quoting("has space", SqlDialect::SQLite));
    }

    #[test]
    fn test_special_chars_quoted() {
        assert!(needs_quoting("has space"));
        assert!(needs_quoting("with-dash"));
        assert!(needs_quoting("123start"));
        assert!(needs_quoting(""));
    }

    fn seeded_pack() -> DialectPack {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
        DialectPack::load(&conn).unwrap()
    }

    fn quoted(ident: &str, dialect: SqlDialect, pack: &DialectPack) -> String {
        let mut sql = String::new();
        write_identifier_with_stropping(&mut sql, ident, false, dialect, pack).unwrap();
        sql
    }

    // An identifier containing its target's own closing delimiter must
    // never terminate the quoted token: pivot column names come from
    // DATA VALUES, so an unescaped delimiter lets data rewrite the SQL
    // token stream (injection, not just breakage).
    #[test]
    fn each_dialect_escapes_its_own_closing_delimiter() {
        let pack = seeded_pack();
        // Standard family: embedded double quotes double.
        for d in [
            SqlDialect::SQLite,
            SqlDialect::PostgreSQL,
            SqlDialect::DuckDB,
        ] {
            assert_eq!(quoted("a\"b", d, &pack), "\"a\"\"b\"");
        }
        // MySQL backticks: embedded backtick doubles inside `...`.
        assert_eq!(quoted("a`b", SqlDialect::MySQL, &pack), "`a``b`");
        // SQL Server brackets: embedded closing bracket doubles; the
        // opening bracket needs no escape (QUOTENAME semantics).
        assert_eq!(quoted("a]b", SqlDialect::SqlServer, &pack), "[a]]b]");
        assert_eq!(quoted("a[b", SqlDialect::SqlServer, &pack), "[a[b]");
    }

    // Cross-delimiter characters are data, not structure, on dialects
    // where they are not the delimiter.
    #[test]
    fn foreign_delimiters_pass_through_unescaped() {
        let pack = seeded_pack();
        assert_eq!(quoted("a]b", SqlDialect::MySQL, &pack), "`a]b`");
        assert_eq!(quoted("a`b", SqlDialect::SqlServer, &pack), "[a`b]");
        assert_eq!(quoted("a`]b", SqlDialect::SQLite, &pack), "\"a`]b\"");
        // The standard family's quote is data inside brackets/backticks.
        assert_eq!(quoted("a\"b", SqlDialect::MySQL, &pack), "`a\"b`");
        assert_eq!(quoted("a\"b", SqlDialect::SqlServer, &pack), "[a\"b]");
    }
}
