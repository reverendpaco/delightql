// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! ORDERING IS ADMITTED; PRESERVATION IS NOT IMPLIED. An ordering that the
//! statement does not present is lowered at its authored stage, and nothing
//! after it inherits its order. No answer observes this portably (the rows a
//! later step sees in order are the target's affair), so these contracts read
//! the SQL the CLI emits.

use std::path::Path;
use std::process::{Command, Stdio};

const R: &str = "r(*) : _(id, s @ 1, 20; 2, 30; 3, 10)\n";

fn sql(dir: &Path, query: &str) -> String {
    sql_for(dir, None, &format!("{R}{query}"))
}

fn sql_for(dir: &Path, dialect: Option<&str>, source: &str) -> String {
    let database = dir.join("empty.sqlite");
    let mut command = Command::new(env!("CARGO_BIN_EXE_dql"));
    command
        .args(["query", "--make-new-db-if-missing", "--db"])
        .arg(&database)
        .args(["--to", "sql", source])
        .current_dir(dir)
        .env("DQL_STATE_DIR", dir.join("state"))
        .stdin(Stdio::null());
    match dialect {
        Some(dialect) => command.env("DQL_DIALECT", dialect),
        None => command.env_remove("DQL_DIALECT"),
    };
    let output = command.output().expect("spawn dql");
    assert!(
        output.status.success(),
        "{dialect:?} {source}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("utf-8 SQL")
        .lines()
        .last()
        .unwrap_or_default()
        .replace("\\n", "\n")
}

/// The lines of `sql` from the first `ORDER BY` on.
fn from_order_by(sql: &str) -> Vec<&str> {
    let lines: Vec<&str> = sql.lines().collect();
    let at = lines
        .iter()
        .position(|l| l.trim_start().starts_with("ORDER BY"))
        .unwrap_or_else(|| panic!("no ORDER BY in:\n{sql}"));
    lines[at..].to_vec()
}

#[test]
fn an_ordering_another_step_follows_is_written_in_its_own_stage() {
    let dir = tempfile::tempdir().unwrap();
    let sql = sql(dir.path(), "r(*) |> #(s desc) |> (id)");
    let after = from_order_by(&sql);
    // The ORDER BY closes a derived table, and the statement's own select
    // orders nothing.
    assert!(after[1].trim_start().starts_with(") AS"), "{sql}");
    assert_eq!(sql.matches("ORDER BY").count(), 1, "{sql}");
}

#[test]
fn a_later_bound_stays_at_its_own_stage() {
    let dir = tempfile::tempdir().unwrap();
    let sql = sql(dir.path(), "r(*) |> #(s desc) |> (id), #<2");
    let after = from_order_by(&sql);
    assert_eq!(sql.matches("ORDER BY").count(), 1, "{sql}");
    // The bound's stage is written after the ordered stage closes, with no
    // ordering of its own: the inner ordering is never moved to it.
    let limit = after.iter().position(|l| l.trim_start().starts_with("LIMIT")).expect("a LIMIT");
    assert!(after[1..limit].iter().any(|l| l.trim_start().starts_with(") AS")), "{sql}");
}

#[test]
fn an_ordered_set_arm_is_written_inside_its_arm() {
    let dir = tempfile::tempdir().unwrap();
    let sql = sql(dir.path(), "r(*) |> #(s desc) |> (id) ; _(id @ 9)");
    let after = from_order_by(&sql);
    assert!(after[1].trim_start().starts_with(") AS"), "{sql}");
    assert!(!sql.trim_end().lines().last().unwrap_or_default().trim_start().starts_with("ORDER BY"), "{sql}");
}

#[test]
fn the_presented_ordering_is_the_statements_own() {
    let dir = tempfile::tempdir().unwrap();
    let sql = sql(dir.path(), "r(*) |> #(s desc)");
    let last = sql.trim_end().lines().last().unwrap_or_default();
    assert!(last.trim_start().starts_with("ORDER BY"), "{sql}");
    assert_eq!(sql.matches("ORDER BY").count(), 1, "{sql}");
}

/// A recursive member's ordered stage is legal on every target, and keeps no
/// ordering there: no target spells one on a recursive member. PostgreSQL is
/// absent because the CLI cannot compile in its spelling over a SQLite file.
#[test]
fn an_ordering_inside_a_recursive_member_compiles_on_every_target() {
    for stages in [
        "|> #(n desc)",
        "|> #(n desc) |> #(n)",
        "|> #(n desc), n > 0 |> #(n)",
        "|> #(n desc) |> (n + 0 as n) |> #(n)",
        "|> #(n desc) |> #(n) |> #(n desc)",
    ] {
        let source = format!("c(*) : _(n @ 1)\nc(*) : c(*) {stages} |> (n + 1 as n), n < 4\nc(*)");
        for dialect in ["sqlite", "duckdb", "mysql", "sqlserver"] {
            let dir = tempfile::tempdir().unwrap();
            let sql = sql_for(dir.path(), Some(dialect), &source);
            assert!(!sql.contains("ORDER BY"), "{dialect} {stages}:\n{sql}");
            assert!(!sql.contains("OFFSET"), "{dialect} {stages}:\n{sql}");
        }
    }
}

/// Over a carrier, an ordering a restriction follows is still written in its
/// own stage, and the later bound ranks by nothing: the inner ordering (the
/// query's only descending term) never reaches the bound's window.
#[test]
fn a_dependent_later_bound_never_ranks_by_an_inner_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let sql = sql(
        dir.path(),
        "h(x)(*) : r(*), s > $.x |> #(s desc), id > 0, #<1\n_(x @ 0; 15), h(x)(*)",
    );
    let lines: Vec<&str> = sql.lines().collect();
    assert!(!lines.iter().any(|l| l.contains("OVER (") && l.contains("DESC")), "{sql}");
    let at = lines
        .iter()
        .position(|l| l.trim_start().starts_with("ORDER BY") && l.contains("DESC"))
        .unwrap_or_else(|| panic!("the ordering is not written in a stage:\n{sql}"));
    assert!(lines[at + 1].trim_start().starts_with(") AS"), "{sql}");
}
