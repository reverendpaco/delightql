// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Position-valid authored names.
//!
//! A parsed spelling is a CANDIDATE, not yet an alias, a stage name, a
//! definition name, or a published column name. Each naming position has a
//! fallible constructor here, and the position types have private fields,
//! so a spelling reaches a semantic naming carrier only through the one
//! admission law:
//!
//! - exact `_` is reserved deixis, bare or stropped — stropping is
//!   spelling and does not release the reservation (strops-law);
//! - a reserved word — DelightQL's keyword vocabulary or a reserved word
//!   of a supported SQL target — is an identifier only when stropped; the
//!   judgment is case-insensitive and target-independent (top-grammar);
//! - `schema` is the ordinary companion relation at definition/reference
//!   positions; its accepted language spelling is bare even though SQL
//!   targets reserve the word, and generation quotes it as needed;
//! - a lawful strop is an ordinary exact name; its payload domain
//!   (emptiness, control characters, a backtick escape) is DOCKETED and
//!   deliberately not judged here — [`strop_payload`] is the one seam the
//!   future ruling lands in.
//!
//! Admission also RESERVES the authored spelling with the compilation
//! registry, which is what lets baptism refuse to draw an invented name
//! any authored name already owns (ALIAS ALWAYS PRE-EMPTS A MINT).
//!
//! The compiler's own exact spellings are a different source with a
//! different policy and never pass through this authority: a receipt
//! column the compiler spells is not an authored candidate. The two
//! non-authoring roads that re-read admitted text — stored definition
//! source and system-owned `_`-child blocks — skip the law at the
//! normalizer, which owns that classification.

use delightql_types::SqlIdentifier;

use crate::error::{DelightQLError, Result};

/// The declared reserved-word inventory (top-grammar: "The implementation
/// owes one declared inventory").
///
/// A word appears here when it is DelightQL keyword vocabulary, or when at
/// least one supported SQL target (SQLite, PostgreSQL, DuckDB, MySQL,
/// SQL Server) refuses it as an unquoted identifier in relation or column
/// position. Uppercase, sorted, deduplicated — the membership test is
/// case-insensitive. This is admission law, not the generator's quoting
/// heuristic: the generator quotes many words that are lawful bare names,
/// and that caution must not leak into what an author may write.
const RESERVED_INVENTORY: &[&str] = &[
    "ADD",
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
    "AUTHORIZATION",
    "AUTOINCREMENT",
    "BACKUP",
    "BEGIN",
    "BETWEEN",
    "BINARY",
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
    "CHECK",
    "CHECKPOINT",
    "CLOSE",
    "CLUSTERED",
    "COALESCE",
    "COLLATE",
    "COLUMN",
    "COMMIT",
    "COMPUTE",
    "CONDITION",
    "CONSTRAINT",
    "CONTAINS",
    "CONTINUE",
    "CONVERT",
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
    "DBCC",
    "DEALLOCATE",
    "DECLARE",
    "DEFAULT",
    "DEFERRABLE",
    "DELETE",
    "DENSE_RANK",
    "DENY",
    "DESC",
    "DESCENDING",
    "DESCRIBE",
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
    "END",
    "ERRLVL",
    "ESCAPE",
    "EXCEPT",
    "EXEC",
    "EXECUTE",
    "EXISTS",
    "EXIT",
    "EXPLAIN",
    "FALSE",
    "FETCH",
    "FILE",
    "FILLFACTOR",
    "FIRST_VALUE",
    "FOR",
    "FOREIGN",
    "FREETEXT",
    "FREEZE",
    "FROM",
    "FULL",
    "FUNCTION",
    "GENERATED",
    "GRANT",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HAVING",
    "HIGH_PRIORITY",
    "HOLDLOCK",
    "IDENTITY",
    "IF",
    "IGNORE",
    "ILIKE",
    "IN",
    "INDEX",
    "INITIALLY",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "KEY",
    "KILL",
    "LAG",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LINENO",
    "LOAD",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCK",
    "LONG",
    "LOOP",
    "LOW_PRIORITY",
    "MATCH",
    "MERGE",
    "MOD",
    "NATURAL",
    "NOCHECK",
    "NONCLUSTERED",
    "NOT",
    "NOTNULL",
    "NTH_VALUE",
    "NTILE",
    "NULL",
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
    "OUT",
    "OUTER",
    "OVER",
    "OVERLAPS",
    "PARTITION",
    "PERCENT",
    "PERCENT_RANK",
    "PIVOT",
    "PLACING",
    "PLAN",
    "PRECISION",
    "PREPARE",
    "PRIMARY",
    "PRINT",
    "PROC",
    "PROCEDURE",
    "PURGE",
    "RAISERROR",
    "RANGE",
    "RANK",
    "READ",
    "RECURSIVE",
    "REFERENCES",
    "REGEXP",
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
    "SECURITYAUDIT",
    "SELECT",
    "SEMANTICKEYPHRASETABLE",
    "SESSION_USER",
    "SET",
    "SETUSER",
    "SHOW",
    "SHUTDOWN",
    "SIMILAR",
    "SOME",
    "SPATIAL",
    "SQL",
    "SYMMETRIC",
    "SYSTEM_USER",
    "TABLE",
    "TABLESAMPLE",
    "THEN",
    "TO",
    "TOP",
    "TRAILING",
    "TRAN",
    "TRANSACTION",
    "TRIGGER",
    "TRUE",
    "TRUNCATE",
    "UNION",
    "UNIQUE",
    "UNLOCK",
    "UNPIVOT",
    "UNSIGNED",
    "UPDATE",
    "UPDATETEXT",
    "USE",
    "USER",
    "USING",
    "VALUES",
    "VARIADIC",
    "VARYING",
    "VERBOSE",
    "VIEW",
    "WAITFOR",
    "WHEN",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "WRITETEXT",
    "XOR",
];

/// Whether a spelling is in the declared reserved inventory,
/// case-insensitively.
fn is_reserved(text: &str) -> bool {
    RESERVED_INVENTORY
        .iter()
        .any(|word| word.eq_ignore_ascii_case(text))
}

/// The naming position a candidate was written in — what the refusal
/// teaches with. Positions refuse identically today; the position is
/// carried so the teaching can say WHERE the unlawful name stood.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamingPosition {
    /// A relation being referenced in query position.
    Reference,
    /// `as q` on a stage, member, head, or relation.
    StageOrAlias,
    /// `expr as name` in a publication or transform item.
    Published,
    /// A rename target (`*[old as new]`) or template survivor.
    RenameTarget,
    /// A definition or fact head name.
    Definition,
    /// A `let`-bound (CTE) name.
    Cte,
}

impl NamingPosition {
    fn role(self) -> &'static str {
        match self {
            NamingPosition::Reference => "a relation reference",
            NamingPosition::StageOrAlias => "an alias",
            NamingPosition::Published => "a published column name",
            NamingPosition::RenameTarget => "a rename target",
            NamingPosition::Definition => "a definition name",
            NamingPosition::Cte => "a binding name",
        }
    }
}

/// One admitted authored name. Private field: the only constructors are
/// the position admissions below, so holding one IS the proof that the
/// admission law ran.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthoredName {
    spelling: SqlIdentifier,
}

/// The one admission law, shared by every naming position.
fn admit(
    spelling: SqlIdentifier,
    position: NamingPosition,
    registry: &crate::names::Registry,
) -> Result<AuthoredName> {
    if spelling.as_str() == "_" {
        return Err(DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::IDENTIFIER_DEIXIS,
            format!(
                "exact '_' is reserved for deixis; it cannot become {}",
                position.role()
            ),
            "'_' points at the one unnamed pipe stage and disregards slots; \
             longer spellings such as '__' are ordinary names",
        ));
    }
    if spelling.is_stropped() {
        strop_payload(spelling.as_str())?;
    } else if is_reserved(spelling.as_str())
        && !(spelling.as_str().eq_ignore_ascii_case("schema")
            && matches!(
                position,
                NamingPosition::Reference | NamingPosition::Definition
            ))
    {
        return Err(DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::IDENTIFIER_KEYWORD,
            format!(
                "keyword '{}' is an identifier only when stropped",
                spelling.as_str()
            ),
            format!(
                "write `{}` (stropped) to use the word as {}",
                spelling.as_str(),
                position.role()
            ),
        ));
    }
    registry.reserve_authored(spelling.as_str(), spelling.is_stropped());
    Ok(AuthoredName { spelling })
}

/// The strop payload seam. WHAT TEXT A STROP MAY CONTAIN is docketed
/// (`DOCKET.md`): emptiness, control characters, and a backtick escape are
/// deliberately unruled, so this admits the grammar's current domain —
/// one or more non-backtick characters — and refuses nothing of its own.
/// The future ruling changes THIS function and nothing else.
fn strop_payload(_text: &str) -> Result<()> {
    Ok(())
}

macro_rules! position_name {
    ($(#[$doc:meta])* $name:ident, $position:expr) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq)]
        pub struct $name(AuthoredName);

        impl $name {
            /// Admit an authored candidate into this position, reserving
            /// its spelling with the compilation registry.
            pub fn admit(
                spelling: SqlIdentifier,
                registry: &crate::names::Registry,
            ) -> Result<Self> {
                admit(spelling, $position, registry).map($name)
            }

            pub fn into_spelling(self) -> SqlIdentifier {
                self.0.spelling
            }
        }

        impl crate::lispy::ToLispy for $name {
            fn to_lispy(&self) -> String {
                self.0.spelling.to_lispy()
            }
        }
    };
}

position_name!(
    /// A relation name written in query position.
    ReferenceName,
    NamingPosition::Reference
);
position_name!(
    /// An `as` name on a stage, member, head, or relation — the answering
    /// name a scope will carry.
    StageName,
    NamingPosition::StageOrAlias
);
position_name!(
    /// A published column name (`expr as name`, a transform's target).
    PublishedName,
    NamingPosition::Published
);
position_name!(
    /// A rename target's literal new name.
    RenameName,
    NamingPosition::RenameTarget
);
position_name!(
    /// A definition or fact head's name.
    DefinitionName,
    NamingPosition::Definition
);
position_name!(
    /// A `let`-bound (CTE) name.
    CteName,
    NamingPosition::Cte
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::Registry;

    fn registry() -> Registry {
        Registry::new(&[])
    }

    #[test]
    fn exact_deixis_refuses_bare_and_stropped_in_every_position() {
        let reg = registry();
        for candidate in [SqlIdentifier::new("_"), SqlIdentifier::stropped("_")] {
            assert!(StageName::admit(candidate.clone(), &reg).is_err());
            assert!(PublishedName::admit(candidate.clone(), &reg).is_err());
            assert!(RenameName::admit(candidate.clone(), &reg).is_err());
            assert!(DefinitionName::admit(candidate.clone(), &reg).is_err());
            assert!(CteName::admit(candidate, &reg).is_err());
        }
    }

    #[test]
    fn reserved_words_refuse_bare_case_insensitively() {
        let reg = registry();
        for word in ["as", "SELECT", "Where", "double", "then"] {
            let refusal = StageName::admit(SqlIdentifier::new(word), &reg)
                .expect_err("a bare reserved word is not an identifier");
            assert!(
                refusal.to_string().contains("only when stropped"),
                "{refusal}"
            );
        }
    }

    #[test]
    fn a_stropped_reserved_word_is_an_ordinary_exact_name() {
        let reg = registry();
        for word in ["select", "as", "then"] {
            let admitted = StageName::admit(SqlIdentifier::stropped(word), &reg)
                .expect("stropping is the spelling that makes a keyword a name");
            assert_eq!(admitted.into_spelling().as_str(), word);
        }
    }

    #[test]
    fn longer_underscore_spellings_are_ordinary_names() {
        let reg = registry();
        for word in ["__", "_____", "_fn"] {
            assert!(StageName::admit(SqlIdentifier::new(word), &reg).is_ok());
        }
    }

    #[test]
    fn admission_reserves_the_canonical_spelling() {
        let reg = registry();
        StageName::admit(SqlIdentifier::new("MyAlias"), &reg).unwrap();
        let reserved = reg.authored_reserved();
        assert_eq!(reserved.len(), 1);
        // The reservation is the CANONICAL identity: a later folded use of
        // the same name reserves nothing new.
        StageName::admit(SqlIdentifier::new("myalias"), &reg).unwrap();
        assert_eq!(reg.authored_reserved().len(), 1);
    }

    #[test]
    fn the_inventory_is_sorted_unique_uppercase() {
        for pair in RESERVED_INVENTORY.windows(2) {
            assert!(pair[0] < pair[1], "{} !< {}", pair[0], pair[1]);
        }
        for word in RESERVED_INVENTORY {
            assert_eq!(*word, word.to_ascii_uppercase());
        }
    }
}
