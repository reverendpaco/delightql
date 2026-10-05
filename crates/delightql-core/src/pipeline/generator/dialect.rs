// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SqlDialect {
    #[default]
    SQLite,
    PostgreSQL,
    MySQL,
    SqlServer,
    DuckDB,
}

/// WHERE A TARGET PUTS ITS ROW BOUND.
///
/// Four of the five write a trailing `LIMIT`/`OFFSET` clause that stands on
/// its own. Transact-SQL has no such clause: a cap is a `TOP` between
/// `SELECT` and the list, and a skip belongs to `ORDER BY`, which must
/// therefore be present in the same query block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowClauseStyle {
    /// `LIMIT <count|uncapped> [OFFSET n]`, where `uncapped` is how this
    /// target spells "no maximum".
    Trailing { uncapped: &'static str },
    /// `SELECT TOP n` for a bare cap, `ORDER BY … OFFSET n ROWS [FETCH NEXT
    /// m ROWS ONLY]` once a skip is involved.
    TopAndFetch,
}

/// WHAT A NUMBER READ OUT OF A DOCUMENT CARRIES on a target, as this
/// target's document scalar read publishes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentNumberCategory {
    /// The read hands back a typed value: a JSON integer is an integer and
    /// a JSON real a real (SQLite's `json_extract`). A numeric consumer may
    /// take it as it is.
    Stated,
    /// The read publishes TEXT (PostgreSQL's `#>>`, DuckDB's
    /// `json_extract_string`) or a document value (`#>` jsonb): the lexical
    /// integer/decimal/approximate category is gone and no numeric consumer
    /// may take the value without an explicit cast — `sum(text)` is refused
    /// by the target, and a compiler that cast every document number to one
    /// type would promote the rest. Measured on stock PostgreSQL 18 and
    /// DuckDB.
    Erased,
    /// No live lane has measured this target's read. That is not proof:
    /// the ruled alpha contract requires a category to be established
    /// before a consumer that needs one, so an unmeasured target is judged
    /// exactly as an erased one until its read is measured.
    Unmeasured,
}

impl SqlDialect {
    /// WHAT THIS TARGET'S DOCUMENT SCALAR READ PUBLISHES for a number.
    pub fn document_number_category(self) -> DocumentNumberCategory {
        match self {
            SqlDialect::SQLite => DocumentNumberCategory::Stated,
            SqlDialect::PostgreSQL | SqlDialect::DuckDB => DocumentNumberCategory::Erased,
            SqlDialect::MySQL | SqlDialect::SqlServer => DocumentNumberCategory::Unmeasured,
        }
    }

    /// HOW THE GENERIC JSON-EACH ITERATOR REPORTS AN ARRAY. This judgment is
    /// used only when the selected intrinsic has no target template. A
    /// target template owns its complete guard (PostgreSQL uses
    /// `jsonb_typeof`), so its spelling cannot be accidentally compared with
    /// this generic operation's result.
    pub fn json_each_array_guard_type(self) -> &'static str {
        match self {
            SqlDialect::SQLite | SqlDialect::SqlServer => "array",
            SqlDialect::MySQL | SqlDialect::DuckDB => "ARRAY",
            SqlDialect::PostgreSQL => "array",
        }
    }

    /// HOW AN EFFECT PLAN'S BRACKET OPENS ON THIS TARGET. The bracket is the
    /// extent a row locator is valid for: under PostgreSQL's default Read
    /// Committed a later statement sees a later snapshot, so a `ctid`
    /// captured while staging may name a version a concurrent writer has
    /// since replaced, and the mutation would silently narrow. REPEATABLE
    /// READ holds one snapshot for the bracket, and a selected row a
    /// concurrent transaction changed fails the mutation with a
    /// serialization error rather than missing it. SQLite serializes
    /// writers within its transaction and DuckDB refuses a conflicting
    /// write, so their plain `BEGIN` already gives the bracket that extent.
    pub fn transaction_begin(self) -> &'static str {
        match self {
            SqlDialect::PostgreSQL => "BEGIN ISOLATION LEVEL REPEATABLE READ",
            SqlDialect::SQLite | SqlDialect::DuckDB | SqlDialect::MySQL | SqlDialect::SqlServer => {
                "BEGIN"
            }
        }
    }

    /// HOW THIS TARGET LOCATES A STORED ROW: the pseudo-columns a base
    /// table row carries, read beside its columns and compared part by
    /// part — ALL of them, because one alone may not be exact: PostgreSQL's
    /// `ctid` is a location within one table, and a partitioned or
    /// inherited parent scans several, so `tableoid` names which. Each part
    /// lists the names a statement reads it by, preferred first: SQLite
    /// answers its rowid to `rowid`, `oid` and `_rowid_` alike, each until a
    /// stored column takes the name. `None` where the target exposes no
    /// locator to a statement — a mutation that must reach the exact
    /// occurrence it selected refuses there rather than matching rows by
    /// value.
    pub fn row_locator_spellings(self) -> Option<&'static [&'static [&'static str]]> {
        match self {
            SqlDialect::SQLite => Some(&[&["rowid", "oid", "_rowid_"]]),
            SqlDialect::DuckDB => Some(&[&["rowid"]]),
            SqlDialect::PostgreSQL => Some(&[&["tableoid"], &["ctid"]]),
            SqlDialect::MySQL | SqlDialect::SqlServer => None,
        }
    }

    /// Whether this target spells a total-row cap on a recursive binding
    /// natively: a LIMIT on the recursive member, which SQLite and MySQL
    /// read as a cap on the whole compound. Elsewhere the only spelling
    /// limits each iteration, a different, non-terminating meaning.
    pub fn spells_recursive_total_cap(self) -> bool {
        match self {
            SqlDialect::SQLite | SqlDialect::MySQL => true,
            SqlDialect::PostgreSQL | SqlDialect::DuckDB | SqlDialect::SqlServer => false,
        }
    }

    /// Whether this target's SQL can return a table of zero columns as a
    /// statement's result. PostgreSQL admits an empty select list; SQLite
    /// and DuckDB require a column, and the column a statement writes for
    /// them would be a value the user receives. MySQL and Transact-SQL have
    /// no empty select list either.
    pub fn returns_zero_column_table(self) -> bool {
        matches!(self, SqlDialect::PostgreSQL)
    }

    /// The most terms one compound SELECT may hold on this target, where a
    /// limit exists: SQLite refuses a compound of more than 500 terms (its
    /// default `SQLITE_MAX_COMPOUND_SELECT`). `None` where no limit binds.
    pub fn compound_term_limit(self) -> Option<usize> {
        match self {
            SqlDialect::SQLite => Some(500),
            SqlDialect::PostgreSQL | SqlDialect::DuckDB | SqlDialect::MySQL | SqlDialect::SqlServer => None,
        }
    }

    /// Whether a recursive compound term must be isolated before its row
    /// bound. SQLite parses a bare LIMIT as belonging to the whole compound;
    /// a derived SELECT keeps the anchor's bound local to that term.
    pub fn compound_term_requires_derived_limit(self) -> bool {
        matches!(self, SqlDialect::SQLite)
    }

    /// HOW THIS TARGET GROUPS ITS WHOLE INPUT AS ONE GROUP, a group that
    /// exists only when the input has a row: what a grouping whose every key
    /// was a literal means once the literals are gone. SQLite admits a
    /// HAVING without GROUP BY only in a query whose result columns
    /// aggregate, so it keeps a GROUP BY on a key every row shares (measured
    /// on 3.45 and 3.50). The others read a HAVING without GROUP BY as the
    /// standard does, one group of the whole input; PostgreSQL and T-SQL
    /// document refusing a constant GROUP BY key (documented, unmeasured).
    pub fn whole_input_grouping(self) -> crate::pipeline::sql_ast::WholeInputGrouping {
        use crate::pipeline::sql_ast::WholeInputGrouping;
        match self {
            SqlDialect::SQLite => WholeInputGrouping::SharedKey,
            SqlDialect::PostgreSQL
            | SqlDialect::DuckDB
            | SqlDialect::MySQL
            | SqlDialect::SqlServer => WholeInputGrouping::InhabitedHaving,
        }
    }

    /// HOW THIS TARGET SPELLS A ROW BOUND.
    ///
    /// `#>n` skips rows and names no cap, and no two targets agree on how to
    /// say so: two write a sentinel count beside `OFFSET`, two write the
    /// keyword the standard gives them, and one has no `LIMIT` at all.
    pub fn row_clause_style(self) -> RowClauseStyle {
        match self {
            // SQLite documents a negative limit as "no upper bound".
            SqlDialect::SQLite => RowClauseStyle::Trailing { uncapped: "-1" },
            // MySQL has no keyword; its manual prescribes the largest
            // unsigned value, which does not fit the AST's signed count and
            // so is written here rather than carried as one.
            SqlDialect::MySQL => RowClauseStyle::Trailing {
                uncapped: "18446744073709551615",
            },
            SqlDialect::PostgreSQL | SqlDialect::DuckDB => {
                RowClauseStyle::Trailing { uncapped: "ALL" }
            }
            SqlDialect::SqlServer => RowClauseStyle::TopAndFetch,
        }
    }

    /// HOW THIS TARGET SPELLS A TYPED REACH, as the SQL literal its JSON
    /// operations take. THE ONE RENDERER OF A PATH: the steps are written
    /// in the target's own path representation and escaped by the target's
    /// own rules, and nothing between here and the target parses them
    /// again. Four families read a `$`-rooted path whose keys are written
    /// quoted, always — a key is data, so no character in it decides its
    /// quoting — and whose from-the-end index is spelled as each family
    /// requires. PostgreSQL's operators take a text array instead, so the
    /// same steps become its array literal, quoted and escaped by ITS
    /// rules. A family with no spelling for a step refuses, naming itself.
    pub fn json_path_literal(
        self,
        path: &crate::pipeline::asts::core::Path,
    ) -> Result<String, String> {
        use crate::pipeline::asts::core::PathStep;
        let mut inner = String::new();
        match self {
            SqlDialect::SQLite | SqlDialect::DuckDB | SqlDialect::MySQL | SqlDialect::SqlServer => {
                inner.push('$');
                for step in path.steps() {
                    match step {
                        PathStep::Key(key) => {
                            inner.push_str(".\"");
                            inner.push_str(&key.replace('\\', "\\\\").replace('"', "\\\""));
                            inner.push('"');
                        }
                        PathStep::Index(index) if *index >= 0 => {
                            inner.push_str(&format!("[{index}]"));
                        }
                        PathStep::Index(index) => {
                            inner.push_str(&self.index_from_end(index.unsigned_abs())?);
                        }
                    }
                }
            }
            SqlDialect::PostgreSQL => {
                inner.push('{');
                for (position, step) in path.steps().enumerate() {
                    if position > 0 {
                        inner.push(',');
                    }
                    match step {
                        PathStep::Key(key) => {
                            inner.push('"');
                            inner.push_str(&key.replace('\\', "\\\\").replace('"', "\\\""));
                            inner.push('"');
                        }
                        // A negative index counts from the end here too.
                        PathStep::Index(index) => inner.push_str(&index.to_string()),
                    }
                }
                inner.push('}');
            }
        }
        Ok(format!("'{}'", inner.replace('\'', "''")))
    }

    /// The `k`-th element from the end (`k >= 1`) in this family's `$`-path
    /// grammar.
    fn index_from_end(self, k: u64) -> Result<String, String> {
        match self {
            SqlDialect::SQLite | SqlDialect::DuckDB => Ok(format!("[#-{k}]")),
            SqlDialect::MySQL => Ok(if k == 1 {
                "[last]".to_string()
            } else {
                format!("[last-{}]", k - 1)
            }),
            SqlDialect::SqlServer => Err(
                "SQL Server's JSON path has no from-the-end index, so a negative path index cannot be rendered for it"
                    .to_string(),
            ),
            SqlDialect::PostgreSQL => Ok(format!("-{k}")),
        }
    }

    /// Dialect family key as spelled in the targeting tables and
    /// `language.dialect` (`dialect_render.dialect` etc.).
    pub fn family_name(self) -> &'static str {
        match self {
            SqlDialect::SQLite => "sqlite",
            SqlDialect::PostgreSQL => "postgres",
            SqlDialect::MySQL => "mysql",
            SqlDialect::SqlServer => "sqlserver",
            SqlDialect::DuckDB => "duckdb",
        }
    }

    /// Parse a dialect family key (the `family_name` spelling).
    pub fn from_family_name(name: &str) -> Option<Self> {
        match name {
            "sqlite" => Some(SqlDialect::SQLite),
            "postgres" | "postgresql" => Some(SqlDialect::PostgreSQL),
            "mysql" => Some(SqlDialect::MySQL),
            "sqlserver" => Some(SqlDialect::SqlServer),
            "duckdb" => Some(SqlDialect::DuckDB),
            _ => None,
        }
    }
}
