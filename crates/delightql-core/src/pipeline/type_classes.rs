// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The per-compile image of the `type_classes` targeting table
//! (`bootstrap/type_classes.sql`): which declared column types each target
//! family classes numeric, date or boolean (THE DOCUMENT DENYLIST).
//!
//! A host reads the table by direct SQL once per compilation
//! ([`crate::host::CompilerHost::type_classes`]); the statement target's
//! vocabulary is kept from that image, and a declaration's class is a lookup
//! in it, never a test of spellings in code. How a row matches a
//! declaration is the table's own column: by the affinity SQLite's rule
//! gives it (the product's one statement of the rule,
//! `crate::pipeline::sqlite_affinity`), or by its leading type-name words.

use crate::diagnostic::Runtime;
use crate::error::Result;
use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sqlite_affinity::Affinity;
use std::sync::Arc;

/// The table and its seeded rows, `{table}` standing for the name a host's
/// SQL substrate holds the table under.
const SCRIPT: &str = include_str!("../../bootstrap/type_classes.sql");

/// The DDL and seed rows for a table named `table`.
pub(crate) fn seed_script(table: &str) -> String {
    SCRIPT.replace("{table}", table)
}

/// The one read of the table named `table`, columns in [`TypeClassRow`]
/// order.
pub(crate) fn select_rows(table: &str) -> String {
    format!("SELECT dialect, reads, type_name, class FROM {table}")
}

/// One row as a host's substrate returned it.
pub(crate) struct TypeClassRow {
    pub(crate) dialect: String,
    pub(crate) reads: String,
    pub(crate) type_name: String,
    pub(crate) class: String,
}

/// The class a target gives a declared type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TypeClass {
    Numeric,
    Date,
    Boolean,
}

/// How a row matches a column declaration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reads {
    /// The affinity SQLite's rule gives the declaration is the row's name.
    Affinity,
    /// The declaration's leading words, its parameter list dropped, are the
    /// row's name: the longest run of them a row names.
    TypeName,
}

/// One family's rows: its affinity rows by affinity, its type-name rows by
/// their words.
#[derive(Debug)]
struct Family {
    dialect: SqlDialect,
    affinities: Vec<(Affinity, TypeClass)>,
    names: Vec<(Vec<String>, TypeClass)>,
}

/// One compilation's immutable image of the table, every family's rows.
#[derive(Debug, Default)]
pub(crate) struct TypeClasses {
    families: Vec<Family>,
}

fn malformed(row: &TypeClassRow, what: &str) -> crate::error::DelightQLError {
    Runtime::catalog(
        format!(
            "type_classes row ({}, {}, {}, {}) is malformed: {what}",
            row.dialect, row.reads, row.type_name, row.class
        ),
        "the type_classes targeting table holds a row its read cannot admit",
    )
}

impl TypeClasses {
    /// Admit every row, refusing the whole read on the first row the table's
    /// own constraints should have kept out.
    pub(crate) fn from_rows(rows: impl IntoIterator<Item = TypeClassRow>) -> Result<Self> {
        let mut families: Vec<Family> = Vec::new();
        for row in rows {
            let dialect = match SqlDialect::from_family_name(&row.dialect) {
                Some(d) if d.family_name() == row.dialect => d,
                _ => return Err(malformed(&row, "unknown dialect family")),
            };
            let reads = match row.reads.as_str() {
                "affinity" => Reads::Affinity,
                "type_name" => Reads::TypeName,
                _ => return Err(malformed(&row, "reads is affinity or type_name")),
            };
            let class = match row.class.as_str() {
                "numeric" => TypeClass::Numeric,
                "date" => TypeClass::Date,
                "boolean" => TypeClass::Boolean,
                _ => return Err(malformed(&row, "the class is numeric, date or boolean")),
            };
            let name = &row.type_name;
            if name.is_empty() || *name != name.to_ascii_lowercase() || name.split(' ').any(str::is_empty) {
                return Err(malformed(&row, "a type name is lower-case words separated by one space"));
            }
            let at = match families.iter().position(|f| f.dialect == dialect) {
                Some(at) => at,
                None => {
                    families.push(Family {
                        dialect,
                        affinities: Vec::new(),
                        names: Vec::new(),
                    });
                    families.len() - 1
                }
            };
            let family = &mut families[at];
            match reads {
                Reads::Affinity => {
                    let affinity = Affinity::ALL
                        .into_iter()
                        .find(|a| a.name() == name)
                        .ok_or_else(|| malformed(&row, "an affinity row names one of SQLite's five affinities"))?;
                    if family.affinities.iter().any(|(a, _)| *a == affinity) {
                        return Err(malformed(&row, "duplicate (dialect, reads, type_name)"));
                    }
                    family.affinities.push((affinity, class));
                }
                Reads::TypeName => {
                    let words: Vec<String> = name.split(' ').map(str::to_string).collect();
                    if family.names.iter().any(|(n, _)| *n == words) {
                        return Err(malformed(&row, "duplicate (dialect, reads, type_name)"));
                    }
                    family.names.push((words, class));
                }
            }
        }
        Ok(TypeClasses { families })
    }
}

/// The statement target's vocabulary: its family's rows, read its way.
#[derive(Debug)]
pub(crate) struct TargetTypes {
    image: Arc<TypeClasses>,
    family: usize,
}

impl TargetTypes {
    /// `dialect`'s family's vocabulary. A family the table holds no row for
    /// refuses: its declarations would all lower, and that is no reading of
    /// the target.
    pub(crate) fn of(image: Arc<TypeClasses>, dialect: SqlDialect) -> Result<Self> {
        let family = image.families.iter().position(|f| f.dialect == dialect).ok_or_else(|| {
            Runtime::catalog(
                format!("the type_classes targeting table holds no row for {}", dialect.family_name()),
                "the target's type vocabulary is missing from its host's targeting tables",
            )
        })?;
        Ok(TargetTypes { image, family })
    }

    /// The class of a declaration: the class of the type-name row its
    /// leading words match, else of the affinity row its affinity matches;
    /// `None` for one no row matches.
    pub(crate) fn class(&self, declared: &str) -> Option<TypeClass> {
        let family = &self.image.families[self.family];
        let head = declared.split('(').next().unwrap_or_default().to_ascii_lowercase();
        let words: Vec<&str> = head.split_whitespace().collect();
        let named = family
            .names
            .iter()
            .filter(|(name, _)| name.len() <= words.len() && name.iter().zip(&words).all(|(a, b)| a == b))
            .max_by_key(|(name, _)| name.len())
            .map(|(_, class)| *class);
        named.or_else(|| {
            if family.affinities.is_empty() {
                return None;
            }
            let affinity = crate::pipeline::sqlite_affinity::declared_affinity(Some(declared))?;
            family.affinities.iter().find(|(a, _)| *a == affinity).map(|(_, class)| *class)
        })
    }
}

/// Seed the table into a fresh in-memory database and read it back, as a
/// host does.
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn seeded() -> Arc<TypeClasses> {
    let conn = rusqlite::Connection::open_in_memory().expect("an in-memory database opens");
    conn.execute_batch(&seed_script("type_classes")).expect("the seed script runs");
    let mut statement = conn.prepare(&select_rows("type_classes")).expect("the read prepares");
    let rows = statement
        .query_map([], |row| {
            Ok(TypeClassRow {
                dialect: row.get(0)?,
                reads: row.get(1)?,
                type_name: row.get(2)?,
                class: row.get(3)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
        .expect("the rows read");
    Arc::new(TypeClasses::from_rows(rows).expect("the seeded rows admit"))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    fn class_of(dialect: SqlDialect, declared: &str) -> Option<TypeClass> {
        TargetTypes::of(seeded(), dialect).expect("the family is seeded").class(declared)
    }

    #[test]
    fn every_family_states_its_own_vocabulary() {
        for dialect in [
            SqlDialect::SQLite,
            SqlDialect::PostgreSQL,
            SqlDialect::DuckDB,
            SqlDialect::MySQL,
            SqlDialect::SqlServer,
        ] {
            TargetTypes::of(seeded(), dialect).expect("the family is seeded");
        }
    }

    #[test]
    fn sqlite_classes_a_declaration_by_positive_affinity_and_by_name() {
        use TypeClass::{Boolean, Date, Numeric};
        for (declared, class) in [
            // INTEGER or REAL affinity, positively derived.
            ("INTEGER", Some(Numeric)),
            ("INT64", Some(Numeric)),
            ("BIGINTEGER", Some(Numeric)),
            ("MEDIUMINT", Some(Numeric)),
            ("HUGEINT", Some(Numeric)),
            ("UBIGINT", Some(Numeric)),
            ("INTERNAL", Some(Numeric)),
            ("REAL", Some(Numeric)),
            ("DOUBLE PRECISION", Some(Numeric)),
            // Named rows.
            ("SERIAL", Some(Numeric)),
            ("MONEY", Some(Numeric)),
            ("DEC(5,2)", Some(Numeric)),
            ("NUMERIC(10,2)", Some(Numeric)),
            ("SMALLMONEY", Some(Numeric)),
            ("LOGICAL", Some(Boolean)),
            ("BOOLEAN", Some(Boolean)),
            ("YEAR", Some(Date)),
            ("DATE", Some(Date)),
            ("TIMESTAMP WITH TIME ZONE", Some(Date)),
            // SQLite's fallback NUMERIC affinity: unknown, lowers.
            ("JSON", None),
            ("TIMELINE", None),
            ("TIMESTAMP_NS", None),
            ("DECIMALS", None),
            ("NUMBER", None),
            // Textual, BLOB, undeclared, and an affinity not established.
            ("VARCHAR(100)", None),
            ("TEXT", None),
            ("DATETIME_TEXT", None),
            ("CLOB", None),
            ("BLOB", None),
            ("", None),
            ("ANY", None),
        ] {
            assert_eq!(class_of(SqlDialect::SQLite, declared), class, "{declared}");
        }
    }

    #[test]
    fn a_closed_vocabulary_reads_the_leading_type_name_words() {
        use TypeClass::{Boolean, Date, Numeric};
        for (dialect, declared, class) in [
            (SqlDialect::DuckDB, "INT64", Some(Numeric)),
            (SqlDialect::DuckDB, "TIMESTAMP_NS", Some(Date)),
            (SqlDialect::DuckDB, "TIMESTAMP WITH TIME ZONE", Some(Date)),
            (SqlDialect::DuckDB, "DECIMAL(18,3)", Some(Numeric)),
            (SqlDialect::DuckDB, "BOOLEAN", Some(Boolean)),
            (SqlDialect::DuckDB, "BIGINTEGER", None),
            (SqlDialect::DuckDB, "INTERNAL", None),
            (SqlDialect::DuckDB, "TIMELINE", None),
            (SqlDialect::DuckDB, "VARCHAR", None),
            (SqlDialect::DuckDB, "JSON", None),
            (SqlDialect::PostgreSQL, "double precision", Some(Numeric)),
            (SqlDialect::PostgreSQL, "timestamp without time zone", Some(Date)),
            (SqlDialect::PostgreSQL, "numeric(10,2)", Some(Numeric)),
            (SqlDialect::PostgreSQL, "character varying(100)", None),
            (SqlDialect::PostgreSQL, "jsonb", None),
            (SqlDialect::PostgreSQL, "INTERNAL", None),
            (SqlDialect::MySQL, "MEDIUMINT", Some(Numeric)),
            (SqlDialect::MySQL, "BIGINT UNSIGNED", Some(Numeric)),
            (SqlDialect::MySQL, "YEAR", Some(Date)),
            (SqlDialect::MySQL, "TIMELINE", None),
            (SqlDialect::SqlServer, "DATETIME2", Some(Date)),
            (SqlDialect::SqlServer, "BIT", Some(Numeric)),
            (SqlDialect::SqlServer, "TIMESTAMP", None),
            (SqlDialect::SqlServer, "NVARCHAR(50)", None),
        ] {
            assert_eq!(class_of(dialect, declared), class, "{dialect:?} {declared}");
        }
    }

    #[test]
    fn rows_are_admitted_by_how_they_match() {
        let row = |reads: &str, name: &str| TypeClassRow {
            dialect: "sqlite".to_string(),
            reads: reads.to_string(),
            type_name: name.to_string(),
            class: "numeric".to_string(),
        };
        assert!(TypeClasses::from_rows([row("type_name", "integer"), row("affinity", "integer")]).is_ok());
        assert!(TypeClasses::from_rows([row("affinity", "int")]).is_err());
        assert!(TypeClasses::from_rows([row("type_name", "int"), row("type_name", "int")]).is_err());
        assert!(TypeClasses::from_rows([row("affinity", "real"), row("affinity", "real")]).is_err());
    }
}
