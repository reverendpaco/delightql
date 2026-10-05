// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The per-compile image of the `aggregates` targeting table: which call of
//! a target callable — functor and resolved arity, within one dialect —
//! reduces the rows it stands over, and whether its whole-operand star form
//! and its windowed form are admitted.
//!
//! A host reads the table by direct SQL once at the start of a compilation
//! unit ([`crate::host::CompilerHost::aggregate_catalog`]); the unit's arena
//! keeps that one immutable image for every judgment its nested work makes,
//! and the next compilation reads again. The image is positive knowledge
//! only: a miss is an unknown callable, never a scalar one.

use crate::diagnostic::Runtime;
use crate::error::Result;
use crate::pipeline::generator::SqlDialect;
use std::sync::Arc;

/// The table and its seeded rows, `{table}` standing for the name a host's
/// SQL substrate holds the table under.
const SCRIPT: &str = include_str!("../../bootstrap/aggregates.sql");

/// The DDL and seed rows for a table named `table`.
pub(crate) fn seed_script(table: &str) -> String {
    SCRIPT.replace("{table}", table)
}

/// The one read of the table named `table`, columns in [`AggregateRow`]
/// order.
pub(crate) fn select_rows(table: &str) -> String {
    format!("SELECT dialect, functor_name, arity, can_be_globbed, can_be_windowed FROM {table}")
}

/// One row as a host's substrate returned it.
pub(crate) struct AggregateRow {
    pub(crate) dialect: String,
    pub(crate) functor_name: String,
    pub(crate) arity: i64,
    pub(crate) can_be_globbed: i64,
    pub(crate) can_be_windowed: i64,
}

/// What the catalog knows of one call: it reduces, whether the
/// whole-operand star may stand as its argument, and whether the target
/// evaluates it over a window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AggregateFact {
    can_be_globbed: bool,
    can_be_windowed: bool,
}

impl AggregateFact {
    pub(crate) fn admits_glob(self) -> bool {
        self.can_be_globbed
    }
}

/// One row, admitted. `None` is the all-dialect key `'*'`.
#[derive(Debug)]
struct Fact {
    functor_name: String,
    arity: usize,
    dialect: Option<SqlDialect>,
    fact: AggregateFact,
}

/// One compilation's immutable image of the table, ordered by functor and
/// arity so a lookup is a search, never a scan.
#[derive(Debug, Default)]
pub(crate) struct AggregateCatalog {
    facts: Vec<Fact>,
}

fn malformed(row: &AggregateRow, what: &str) -> crate::error::DelightQLError {
    Runtime::catalog(
        format!(
            "aggregates row ({}, {}, {}, {}, {}) is malformed: {what}",
            row.dialect, row.functor_name, row.arity, row.can_be_globbed, row.can_be_windowed
        ),
        "the aggregates targeting table holds a row its read cannot admit",
    )
}

impl AggregateCatalog {
    /// Admit every row, refusing the whole read on the first row the table's
    /// own constraints should have kept out.
    pub(crate) fn from_rows(rows: impl IntoIterator<Item = AggregateRow>) -> Result<Self> {
        let rows = rows.into_iter();
        let mut facts = Vec::with_capacity(rows.size_hint().0);
        for row in rows {
            let dialect = match row.dialect.as_str() {
                "*" => None,
                family => match SqlDialect::from_family_name(family) {
                    Some(dialect) if dialect.family_name() == family => Some(dialect),
                    _ => return Err(malformed(&row, "unknown dialect family")),
                },
            };
            // The table's CHECK folds with SQLite's `lower()`, which folds
            // ASCII alone.
            if row.functor_name.is_empty()
                || row.functor_name.bytes().any(|b| b.is_ascii_uppercase())
            {
                return Err(malformed(&row, "functor_name must be non-empty lower case"));
            }
            let arity =
                usize::try_from(row.arity).map_err(|_| malformed(&row, "negative arity"))?;
            let can_be_globbed = match row.can_be_globbed {
                0 => false,
                1 => true,
                _ => return Err(malformed(&row, "can_be_globbed must be 0 or 1")),
            };
            let can_be_windowed = match row.can_be_windowed {
                0 => false,
                1 => true,
                _ => return Err(malformed(&row, "can_be_windowed must be 0 or 1")),
            };
            if facts.iter().any(|fact: &Fact| {
                fact.dialect == dialect
                    && fact.arity == arity
                    && fact.functor_name == row.functor_name
            }) {
                return Err(malformed(&row, "duplicate (dialect, functor_name, arity)"));
            }
            facts.push(Fact {
                functor_name: row.functor_name,
                arity,
                dialect,
                fact: AggregateFact {
                    can_be_globbed,
                    can_be_windowed,
                },
            });
        }
        facts.sort_by(|a, b| {
            (a.functor_name.as_str(), a.arity).cmp(&(b.functor_name.as_str(), b.arity))
        });
        Ok(AggregateCatalog { facts })
    }

    /// The exact dialect's row, else the all-dialect row.
    fn fact(&self, dialect: SqlDialect, name: &str, arity: usize) -> Option<AggregateFact> {
        let name = name.to_ascii_lowercase();
        let start = self.facts.partition_point(|fact| {
            (fact.functor_name.as_str(), fact.arity) < (name.as_str(), arity)
        });
        let rows = self.facts[start..]
            .iter()
            .take_while(|fact| fact.functor_name == name && fact.arity == arity);
        let mut all_dialects = None;
        for row in rows {
            match row.dialect {
                Some(exact) if exact == dialect => return Some(row.fact),
                Some(_) => {}
                None => all_dialects = Some(row.fact),
            }
        }
        all_dialects
    }
}

/// What ONE target is known to reduce: a catalog read against the dialect a
/// statement is emitted for — or nothing, for work no host supplied a
/// catalog to.
#[derive(Clone, Debug)]
pub(crate) struct TargetAggregates {
    known: Option<(Arc<AggregateCatalog>, SqlDialect)>,
}

impl TargetAggregates {
    /// No catalog: every target callable is unknown.
    pub(crate) fn unknown() -> Self {
        TargetAggregates { known: None }
    }

    pub(crate) fn of(catalog: Arc<AggregateCatalog>, dialect: SqlDialect) -> Self {
        TargetAggregates {
            known: Some((catalog, dialect)),
        }
    }

    /// The arena's catalog read against `dialect`; an arena no host armed
    /// knows no target aggregate.
    pub(crate) fn of_arena(names: &crate::names::Registry, dialect: SqlDialect) -> Self {
        match names.aggregate_catalog() {
            Some(catalog) => TargetAggregates::of(catalog, dialect),
            None => TargetAggregates::unknown(),
        }
    }

    /// The row for a call of `name` over `arity` resolved members.
    pub(crate) fn fact(&self, name: &str, arity: usize) -> Option<AggregateFact> {
        let (catalog, dialect) = self.known.as_ref()?;
        catalog.fact(*dialect, name, arity)
    }

    /// Whether a call of `name` over `arity` members is known to reduce.
    pub(crate) fn reduces(&self, name: &str, arity: usize) -> bool {
        self.fact(name, arity).is_some()
    }
}

/// The seeded table, read the way a host reads it: for tests that judge
/// against the catalog a fresh session holds.
#[cfg(test)]
pub(crate) fn seeded_catalog() -> Arc<AggregateCatalog> {
    let conn = rusqlite::Connection::open_in_memory().expect("in-memory catalog");
    conn.execute_batch(&seed_script("aggregates"))
        .expect("the seed script runs");
    let mut statement = conn
        .prepare(&select_rows("aggregates"))
        .expect("the read prepares");
    let rows = statement
        .query_map([], |row| {
            Ok(AggregateRow {
                dialect: row.get(0)?,
                functor_name: row.get(1)?,
                arity: row.get(2)?,
                can_be_globbed: row.get(3)?,
                can_be_windowed: row.get(4)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
        .expect("the seeded rows read");
    Arc::new(AggregateCatalog::from_rows(rows).expect("the seeded rows are admitted"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(dialect: &str, name: &str, arity: i64, glob: i64) -> AggregateRow {
        AggregateRow {
            dialect: dialect.to_string(),
            functor_name: name.to_string(),
            arity,
            can_be_globbed: glob,
            can_be_windowed: 1,
        }
    }

    #[test]
    fn an_exact_dialect_row_is_read_in_place_of_the_all_dialect_row() {
        let catalog = Arc::new(
            AggregateCatalog::from_rows([
                row("*", "tally", 1, 0),
                row("duckdb", "tally", 1, 1),
                row("*", "count", 1, 1),
            ])
            .unwrap(),
        );
        let sqlite = TargetAggregates::of(Arc::clone(&catalog), SqlDialect::SQLite);
        let duckdb = TargetAggregates::of(Arc::clone(&catalog), SqlDialect::DuckDB);
        assert_eq!(
            sqlite.fact("tally", 1).map(AggregateFact::admits_glob),
            Some(false)
        );
        assert_eq!(
            duckdb.fact("tally", 1).map(AggregateFact::admits_glob),
            Some(true)
        );
        assert!(duckdb.fact("count", 1).unwrap().admits_glob());
        assert_eq!(
            sqlite.fact("tally", 2),
            None,
            "the arity is part of the key"
        );
        assert_eq!(sqlite.fact("TALLY", 1), sqlite.fact("tally", 1));
    }

    #[test]
    fn a_target_only_row_answers_for_its_family_alone() {
        let catalog =
            Arc::new(AggregateCatalog::from_rows([row("sqlite", "total", 1, 0)]).unwrap());
        assert!(
            TargetAggregates::of(Arc::clone(&catalog), SqlDialect::SQLite)
                .fact("total", 1)
                .is_some()
        );
        assert_eq!(
            TargetAggregates::of(catalog, SqlDialect::PostgreSQL).fact("total", 1),
            None
        );
        assert_eq!(TargetAggregates::unknown().fact("count", 1), None);
    }

    #[test]
    fn a_row_the_table_constraints_exclude_refuses_the_read() {
        for bad in [
            row("oracle", "sum", 1, 0),
            row("postgresql", "sum", 1, 0),
            row("*", "Sum", 1, 0),
            row("*", "", 1, 0),
            row("*", "sum", -1, 0),
            row("*", "sum", 1, 2),
            AggregateRow {
                can_be_windowed: 2,
                ..row("*", "sum", 1, 0)
            },
        ] {
            assert!(AggregateCatalog::from_rows([bad]).is_err());
        }
        assert!(
            AggregateCatalog::from_rows([row("*", "sum", 1, 0), row("*", "sum", 1, 1)]).is_err()
        );
    }

    #[test]
    fn the_seeded_rows_give_count_alone_the_glob_among_the_universal_aggregates() {
        let sqlite = TargetAggregates::of(seeded_catalog(), SqlDialect::SQLite);
        assert!(sqlite.fact("count", 1).unwrap().admits_glob());
        for name in ["sum", "avg", "min", "max"] {
            assert!(!sqlite.fact(name, 1).unwrap().admits_glob(), "{name}");
        }
        assert_eq!(
            sqlite.fact("max", 2),
            None,
            "two-argument max is no aggregate"
        );
        let postgres = TargetAggregates::of(seeded_catalog(), SqlDialect::PostgreSQL);
        assert!(sqlite.reduces("total", 1));
        assert!(!postgres.reduces("total", 1));
        assert!(postgres.reduces("string_agg", 2));
        assert!(!sqlite.reduces("median", 1));
    }

    #[test]
    fn the_seed_script_names_the_table_it_is_given() {
        let script = seed_script("catalog.aggregates");
        assert!(!script.contains("{table}"));
        assert!(script.contains("CREATE TABLE catalog.aggregates"));
        assert!(script.contains("INSERT INTO catalog.aggregates"));
    }
}
