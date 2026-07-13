// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Compiled query output types.
//!
//! A `CompiledQuery` bundles everything the core pipeline produces after
//! compilation: the primary SQL, assertion SQL, and emit streams. The host
//! (CLI, TUI, library) receives this and decides how to execute each piece.
//!
//! `CompiledPlan` is the generalization (effect algebra, plan §2.3): an
//! ORDERED list of entries the pump plays start to finish — plain
//! statements, statements whose result sets ship to the client, assertion
//! checks, emit streams, and the transaction bracket. A plain query is the
//! degenerate plan (see `From<CompiledQuery> for CompiledPlan`); the effect
//! transformer (Epic 3) is what will produce multi-entry plans.

/// A named SQL stream compiled from an `(~~emit:name ... ~~)` hook.
#[derive(Debug, Clone)]
pub struct EmitStream {
    /// Instance name from `(~~emit:name ~~)`.
    pub name: String,
    /// The filtered SQL query to execute.
    pub sql: String,
    /// Source location in the original DQL (byte start, byte end).
    pub _source_location: Option<(usize, usize)>,
}

/// Whether the compiled SQL is a query (returns rows) or a DML statement (returns affected count).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlKind {
    /// SELECT or similar — returns a result set.
    Query,
    /// DELETE, UPDATE, INSERT — mutates data, returns affected row count.
    Dml,
}

/// Everything the core produces after compilation, before execution.
///
/// The host receives this and decides how to execute each piece:
/// - Primary SQL goes to the main result display (stdout, table pane, etc.)
/// - Assertion SQL is evaluated for boolean verdicts
/// - Emit streams are routed to sinks (`--sink` flag, stderr, TUI panes, etc.)
#[derive(Debug, Clone)]
pub struct CompiledQuery {
    /// The primary SQL query.
    pub primary_sql: String,
    /// Whether this is a query or DML statement.
    pub _kind: SqlKind,
    /// Assertion SQLs (boolean queries). Each is `(sql, source_location)`.
    pub assertion_sqls: Vec<(String, Option<(usize, usize)>)>,
    /// Named emit streams (filtered SQL variants).
    pub emit_streams: Vec<EmitStream>,
    /// Connection ID for routing (which backend to execute on).
    pub connection_id: Option<i64>,
}

// ============================================================================
// CompiledPlan — the generalized output structure (effect algebra, plan §2.3)
// ============================================================================

/// One executable SQL statement inside a plan entry.
///
/// Carries exactly what the pump consumes per statement today
/// (relay `execute_sql_routed(&sql, connection_id)`), plus an optional
/// comment used only by `CompiledPlan::render_sql` — the planner (Epic 3)
/// writes the arm/step annotations there, in the TORTURE-TEST-NORMAL.sql
/// banner style.
#[derive(Debug, Clone)]
#[allow(dead_code)] // consumed by the pump/effect transformer (Epic 3); exercised by this file's tests today
pub struct PlanStatement {
    /// The SQL text, exactly as the generator spelled it.
    pub sql: String,
    /// Connection ID for routing (which backend executes this statement).
    /// `None` = the session's default connection, same semantics as
    /// `CompiledQuery::connection_id`.
    pub connection_id: Option<i64>,
    /// Optional annotation printed as a `-- ` banner above the statement
    /// by `render_sql`. Never affects execution.
    pub comment: Option<String>,
}

#[allow(dead_code)] // see dead_code note on PlanStatement
impl PlanStatement {
    /// A bare statement: SQL only, default connection, no banner.
    pub fn bare(sql: impl Into<String>) -> Self {
        PlanStatement {
            sql: sql.into(),
            connection_id: None,
            comment: None,
        }
    }
}

/// One entry in a `CompiledPlan` — the unit the pump iterates.
///
/// The variants are the pump's vocabulary (IMPLEMENTATION-ARCHITECTURE §4,
/// "relay handle_query → the pump"):
///
/// - `Statement` — execute, discard the result (DML, DDL, receipt inserts,
///   the `__exit` insert). Exit-guard conjuncts (invariant §5.9) are
///   compiled INTO the SQL text by the planner; the entry stays dumb.
/// - `ShippedStatement` — execute AND forward the result set to the client
///   (`stdout!`, the final value). The marker is what lets the pump know a
///   result must ship without inspecting SQL text.
/// - `Assertion` — execute, read the first value as a boolean verdict,
///   abort the run on failure (today's assertion behavior, made an entry).
/// - `Emit` — execute, route the result set to the named sink (today's
///   emit-stream behavior, made an entry).
/// - `BeginTransaction` / `CommitTransaction` — the bracket, as ordinary
///   list positions so the planner can EXPRESS placement invariants:
///   scratch shells go BEFORE `BeginTransaction` (invariant §5.6), and "no
///   transaction control between a DML and its receipt" (§5.2) is checkable
///   as list adjacency. Rollback-on-error is pump behavior (Epic 3).
///
/// Rendering of every variant is pinned by the `render_*` tests in this
/// file's test module.
#[derive(Debug, Clone)]
#[allow(dead_code)] // see dead_code note on PlanStatement
pub enum PlanEntry {
    /// Execute; result discarded.
    Statement(PlanStatement),
    /// Execute; result set ships to the client.
    ShippedStatement(PlanStatement),
    /// Execute; first value is a pass/fail verdict; failure aborts the run.
    Assertion {
        statement: PlanStatement,
        /// Source location in the original DQL (byte start, byte end).
        source_location: Option<(usize, usize)>,
    },
    /// Execute; result set routes to the named emit sink.
    Emit {
        name: String,
        statement: PlanStatement,
        /// Source location in the original DQL (byte start, byte end).
        source_location: Option<(usize, usize)>,
    },
    /// Open the transaction bracket on the routed connection.
    BeginTransaction {
        connection_id: Option<i64>,
        comment: Option<String>,
    },
    /// Close the transaction bracket on the routed connection.
    CommitTransaction {
        connection_id: Option<i64>,
        comment: Option<String>,
    },
}

/// The generalized compilation output: an ordered entry list the pump
/// plays start to finish. NOTHING here executes; compilation stays pure
/// string → strings.
///
/// A plain query is the degenerate plan — see `From<CompiledQuery>`
/// (order pinned by `degenerate_entry_order_mirrors_relay`).
#[derive(Debug, Clone)]
#[allow(dead_code)] // see dead_code note on PlanStatement
pub struct CompiledPlan {
    /// The ordered entries. The pump executes them first to last.
    pub entries: Vec<PlanEntry>,
    /// Name of the exit-flag table, when the plan uses the exit machinery,
    /// carried in the SETTLED connection's dialect spelling (`temp.__exit`
    /// on SQLite/DuckDB, `pg_temp.__exit` on PG — R-T2; the pump runs it
    /// VERBATIM, so the planner owns the spelling; pinned by
    /// `pg_exit_table_and_wrap_guard_spell_pg_temp` in
    /// pipeline/effect_transformer/tests.rs). This is the pump's ONLY
    /// mid-run read: it peeks this table before each entry and stops the
    /// run once a row appears (IMPLEMENTATION-ARCHITECTURE §4). `None` =
    /// no exit machinery; the pump never peeks. Populated by the effect
    /// transformer (Epic 3).
    pub exit_table: Option<String>,
    /// The user-visible objects this plan's DDL directives create
    /// (`temp_table!`/`table!`/`temp_view!` targets — NOT the `__`-scratch
    /// shells). The pump ignores these; the Epic-3.3 entry point registers
    /// them in the session catalog after a successful run so post-run
    /// statements resolve them bare (materialize-pipe.md §1
    /// "catalog-registered"; pinned by the effects ball's
    /// ddl_receipt--12/--13/--14 and util--36 post-state reads).
    pub created_objects: Vec<PlanCreatedObject>,
}

/// One object a plan creates (see `CompiledPlan::created_objects`).
#[derive(Debug, Clone)]
pub struct PlanCreatedObject {
    /// Bare object name as created (unqualified — temp objects live in the
    /// connection's temp schema, materialize-pipe.md §3).
    pub name: String,
    /// True for `temp_view!` targets; false for the table directives.
    pub is_view: bool,
    /// The connection the object was created on (`None` = session default).
    pub connection_id: Option<i64>,
}

#[allow(dead_code)] // see dead_code note on PlanStatement
impl From<CompiledQuery> for CompiledPlan {
    /// The degenerate plan of a plain query.
    ///
    /// Entry order mirrors the relay's hardcoded sequence in
    /// `handle_query` (relay/mod.rs): assertions first (abort on failure),
    /// then emit streams, then the primary statement, whose results ship.
    /// Every entry inherits the query's `connection_id` — per-statement
    /// routing generalizes what the relay already consumes. Pinned by
    /// `degenerate_entry_order_mirrors_relay` and
    /// `degenerate_plain_query_is_one_shipped_entry`.
    fn from(q: CompiledQuery) -> Self {
        let mut entries = Vec::with_capacity(q.assertion_sqls.len() + q.emit_streams.len() + 1);
        for (sql, source_location) in q.assertion_sqls {
            entries.push(PlanEntry::Assertion {
                statement: PlanStatement {
                    sql,
                    connection_id: q.connection_id,
                    comment: None,
                },
                source_location,
            });
        }
        for emit in q.emit_streams {
            entries.push(PlanEntry::Emit {
                name: emit.name,
                statement: PlanStatement {
                    sql: emit.sql,
                    connection_id: q.connection_id,
                    comment: None,
                },
                source_location: emit._source_location,
            });
        }
        entries.push(PlanEntry::ShippedStatement(PlanStatement {
            sql: q.primary_sql,
            connection_id: q.connection_id,
            comment: None,
        }));
        CompiledPlan {
            entries,
            exit_table: None,
            created_objects: Vec::new(),
        }
    }
}

#[allow(dead_code)] // see dead_code note on PlanStatement
impl CompiledPlan {
    /// Render the plan as a readable, commented, `;`-terminated statement
    /// list — the TORTURE-TEST-NORMAL.sql format (plan §2.3: that file IS
    /// the target output for how a plan prints under `--to sql`).
    ///
    /// Format, pinned by the `render_*` tests below:
    /// - entries are separated by one blank line;
    /// - an entry's banner is `-- [tags] first comment line`, with any
    ///   further comment lines continuing as `-- ` lines; a plain
    ///   `Statement` on the default connection with no comment gets no
    ///   banner at all;
    /// - tags: `[ship]`, `[assert]`, `[emit <name>]`, `[conn <n>]` (only
    ///   when a statement routes off the default connection);
    /// - every statement is `;`-terminated (one is appended when the
    ///   generator's text lacks it);
    /// - the bracket prints as bare `BEGIN;` / `COMMIT;`.
    ///
    /// NOTE: `--to sql` for plain queries does NOT route through this
    /// renderer today — its output stays byte-identical to the generator's
    /// (no `;`, no banners). This renderer takes over only when a compiler
    /// path produces multi-entry plans (Epic 3).
    pub fn render_sql(&self) -> String {
        let blocks: Vec<String> = self.entries.iter().map(render_entry).collect();
        blocks.join("\n\n")
    }
}

/// Render one entry as its banner (if any) plus its `;`-terminated SQL.
#[allow(dead_code)] // see dead_code note on PlanStatement
fn render_entry(entry: &PlanEntry) -> String {
    match entry {
        PlanEntry::Statement(st) => render_statement(&[], st),
        PlanEntry::ShippedStatement(st) => render_statement(&["[ship]".to_string()], st),
        PlanEntry::Assertion { statement, .. } => {
            render_statement(&["[assert]".to_string()], statement)
        }
        PlanEntry::Emit { name, statement, .. } => {
            render_statement(&[format!("[emit {}]", name)], statement)
        }
        PlanEntry::BeginTransaction {
            connection_id,
            comment,
        } => render_bracket("BEGIN", *connection_id, comment.as_deref()),
        PlanEntry::CommitTransaction {
            connection_id,
            comment,
        } => render_bracket("COMMIT", *connection_id, comment.as_deref()),
    }
}

#[allow(dead_code)] // see dead_code note on PlanStatement
fn render_bracket(keyword: &str, connection_id: Option<i64>, comment: Option<&str>) -> String {
    let st = PlanStatement {
        sql: keyword.to_string(),
        connection_id,
        comment: comment.map(str::to_string),
    };
    render_statement(&[], &st)
}

#[allow(dead_code)] // see dead_code note on PlanStatement
fn render_statement(tags: &[String], st: &PlanStatement) -> String {
    let mut all_tags: Vec<String> = tags.to_vec();
    if let Some(cid) = st.connection_id {
        all_tags.push(format!("[conn {}]", cid));
    }

    let mut comment_lines = st
        .comment
        .as_deref()
        .map(|c| c.lines().map(str::to_string).collect::<Vec<_>>())
        .unwrap_or_default();

    let mut out = String::new();
    if !all_tags.is_empty() {
        out.push_str("-- ");
        out.push_str(&all_tags.join(" "));
        if !comment_lines.is_empty() {
            out.push(' ');
            out.push_str(&comment_lines.remove(0));
        }
        out.push('\n');
    }
    for line in &comment_lines {
        out.push_str("-- ");
        out.push_str(line);
        out.push('\n');
    }

    let sql = st.sql.trim_end();
    out.push_str(sql);
    if !sql.ends_with(';') {
        out.push(';');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_query(primary: &str, connection_id: Option<i64>) -> CompiledQuery {
        CompiledQuery {
            primary_sql: primary.to_string(),
            _kind: SqlKind::Query,
            assertion_sqls: vec![],
            emit_streams: vec![],
            connection_id,
        }
    }

    // ------------------------------------------------------------------
    // Degenerate case: a plain query is a one-entry plan.
    // ------------------------------------------------------------------

    #[test]
    fn degenerate_plain_query_is_one_shipped_entry() {
        let plan: CompiledPlan = plain_query("SELECT 1 AS a", Some(3)).into();
        assert!(plan.exit_table.is_none());
        assert_eq!(plan.entries.len(), 1);
        match &plan.entries[0] {
            PlanEntry::ShippedStatement(st) => {
                assert_eq!(st.sql, "SELECT 1 AS a");
                assert_eq!(st.connection_id, Some(3));
                assert!(st.comment.is_none());
            }
            other => panic!("expected ShippedStatement, got {:?}", other),
        }
    }

    #[test]
    fn degenerate_entry_order_mirrors_relay() {
        // Relay handle_query order: assertions, then emits, then primary.
        let q = CompiledQuery {
            primary_sql: "SELECT * FROM t".to_string(),
            _kind: SqlKind::Query,
            assertion_sqls: vec![
                ("SELECT count(*) > 0 FROM t".to_string(), Some((5, 9))),
                ("SELECT 1".to_string(), None),
            ],
            emit_streams: vec![EmitStream {
                name: "audit".to_string(),
                sql: "SELECT * FROM t WHERE flagged".to_string(),
                _source_location: Some((10, 20)),
            }],
            connection_id: Some(7),
        };
        let plan: CompiledPlan = q.into();
        assert_eq!(plan.entries.len(), 4);
        match &plan.entries[0] {
            PlanEntry::Assertion {
                statement,
                source_location,
            } => {
                assert_eq!(statement.sql, "SELECT count(*) > 0 FROM t");
                assert_eq!(statement.connection_id, Some(7));
                assert_eq!(*source_location, Some((5, 9)));
            }
            other => panic!("entry 0: expected Assertion, got {:?}", other),
        }
        assert!(matches!(&plan.entries[1], PlanEntry::Assertion { .. }));
        match &plan.entries[2] {
            PlanEntry::Emit {
                name, statement, ..
            } => {
                assert_eq!(name, "audit");
                assert_eq!(statement.connection_id, Some(7));
            }
            other => panic!("entry 2: expected Emit, got {:?}", other),
        }
        match &plan.entries[3] {
            PlanEntry::ShippedStatement(st) => {
                assert_eq!(st.sql, "SELECT * FROM t");
                assert_eq!(st.connection_id, Some(7));
            }
            other => panic!("entry 3: expected ShippedStatement, got {:?}", other),
        }
    }

    // ------------------------------------------------------------------
    // Rendering: the statement-list format (TORTURE-TEST-NORMAL style).
    // ------------------------------------------------------------------

    #[test]
    fn render_bare_statement_terminates_with_semicolon() {
        let plan = CompiledPlan {
            entries: vec![PlanEntry::Statement(PlanStatement::bare(
                "CREATE TEMP TABLE __r_s (success INTEGER, name TEXT)",
            ))],
            exit_table: None,
            created_objects: Vec::new(),
        };
        assert_eq!(
            plan.render_sql(),
            "CREATE TEMP TABLE __r_s (success INTEGER, name TEXT);"
        );
    }

    #[test]
    fn render_does_not_double_semicolon() {
        let plan = CompiledPlan {
            entries: vec![PlanEntry::Statement(PlanStatement::bare("SELECT 1;"))],
            exit_table: None,
            created_objects: Vec::new(),
        };
        assert_eq!(plan.render_sql(), "SELECT 1;");
    }

    #[test]
    fn render_multi_entry_statement_list() {
        // A hand-constructed slice of the torture lowering: scratch shell,
        // shipped stdout! SELECT, CTAS, receipt insert. No compiler path
        // produces this yet (Epic 3); the format itself is what's pinned.
        let plan = CompiledPlan {
            entries: vec![
                PlanEntry::Statement(PlanStatement {
                    sql: "CREATE TEMP TABLE __r_s (success INTEGER, name TEXT)".to_string(),
                    connection_id: None,
                    comment: Some("[plan] scratch: receipts + exit flag".to_string()),
                }),
                PlanEntry::ShippedStatement(PlanStatement {
                    sql: "SELECT * FROM source.orders WHERE order_date >= '2026-07-01'"
                        .to_string(),
                    connection_id: None,
                    comment: Some("stdout! #1".to_string()),
                }),
                PlanEntry::Statement(PlanStatement {
                    sql: "CREATE TEMP TABLE staged AS\nSELECT * FROM source.orders WHERE order_date >= '2026-07-01'"
                        .to_string(),
                    connection_id: None,
                    comment: Some("[arm s!] recent_orders(*) |> temp_table!(staged)".to_string()),
                }),
                PlanEntry::Statement(PlanStatement {
                    sql: "INSERT INTO __r_s SELECT 1, 'staged'".to_string(),
                    connection_id: None,
                    comment: Some("echo receipt: (success, name)".to_string()),
                }),
            ],
            exit_table: Some("__exit".to_string()),
            created_objects: Vec::new(),
        };
        let expected = "\
-- [plan] scratch: receipts + exit flag
CREATE TEMP TABLE __r_s (success INTEGER, name TEXT);

-- [ship] stdout! #1
SELECT * FROM source.orders WHERE order_date >= '2026-07-01';

-- [arm s!] recent_orders(*) |> temp_table!(staged)
CREATE TEMP TABLE staged AS
SELECT * FROM source.orders WHERE order_date >= '2026-07-01';

-- echo receipt: (success, name)
INSERT INTO __r_s SELECT 1, 'staged';";
        assert_eq!(plan.render_sql(), expected);
    }

    #[test]
    fn render_transaction_bracket_after_scratch_shells() {
        // Invariant §5.6: scratch shells first, THEN the bracket. The list
        // representation expresses the placement; this pins how it prints.
        let plan = CompiledPlan {
            entries: vec![
                PlanEntry::Statement(PlanStatement::bare(
                    "CREATE TEMP TABLE __exit (hit INTEGER)",
                )),
                PlanEntry::BeginTransaction {
                    connection_id: None,
                    comment: None,
                },
                PlanEntry::Statement(PlanStatement::bare(
                    "INSERT INTO warehouse.orders_eu SELECT * FROM valid",
                )),
                PlanEntry::CommitTransaction {
                    connection_id: None,
                    comment: None,
                },
            ],
            exit_table: Some("__exit".to_string()),
            created_objects: Vec::new(),
        };
        let expected = "\
CREATE TEMP TABLE __exit (hit INTEGER);

BEGIN;

INSERT INTO warehouse.orders_eu SELECT * FROM valid;

COMMIT;";
        assert_eq!(plan.render_sql(), expected);
    }

    #[test]
    fn render_tags_assert_emit_and_connection() {
        let plan = CompiledPlan {
            entries: vec![
                PlanEntry::Assertion {
                    statement: PlanStatement::bare("SELECT count(*) = 3 FROM t"),
                    source_location: None,
                },
                PlanEntry::Emit {
                    name: "audit".to_string(),
                    statement: PlanStatement::bare("SELECT * FROM t WHERE flagged"),
                    source_location: None,
                },
                PlanEntry::ShippedStatement(PlanStatement {
                    sql: "SELECT * FROM t".to_string(),
                    connection_id: Some(4),
                    comment: None,
                }),
            ],
            exit_table: None,
            created_objects: Vec::new(),
        };
        let expected = "\
-- [assert]
SELECT count(*) = 3 FROM t;

-- [emit audit]
SELECT * FROM t WHERE flagged;

-- [ship] [conn 4]
SELECT * FROM t;";
        assert_eq!(plan.render_sql(), expected);
    }

    #[test]
    fn render_multiline_comment_banner() {
        let plan = CompiledPlan {
            entries: vec![PlanEntry::Statement(PlanStatement {
                sql: "DELETE FROM staged".to_string(),
                connection_id: None,
                comment: Some("[arm k!] cleanup respelled as delete!\nthe condition inlines".to_string()),
            })],
            exit_table: None,
            created_objects: Vec::new(),
        };
        let expected = "\
-- [arm k!] cleanup respelled as delete!
-- the condition inlines
DELETE FROM staged;";
        assert_eq!(plan.render_sql(), expected);
    }

    #[test]
    fn render_degenerate_plain_query() {
        // The degenerate plan of a plain query prints as one shipped entry.
        // (`--to sql` does NOT route through this today — see render_sql docs.)
        let plan: CompiledPlan = plain_query("SELECT 1 AS a", None).into();
        assert_eq!(plan.render_sql(), "-- [ship]\nSELECT 1 AS a;");
    }
}
