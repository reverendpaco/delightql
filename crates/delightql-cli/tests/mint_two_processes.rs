// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE MINT, observed from outside the process that draws it.
//!
//! An invented name is output only: a heading has to say something, but
//! nothing in the language reaches it. So the shipped policy draws that
//! spelling fresh for every compilation, and a client keying on one finds
//! out on its second run rather than after shipping.
//!
//! Why an integration test and not a unit one: "fresh per compilation" is
//! only half the claim. The other half is that the drawn value does not
//! survive the PROCESS — a unit test sharing one address space cannot tell
//! a per-compilation draw from a per-process one, and a suite that could not
//! tell them apart would pass over a mint seeded once at startup.
//!
//! The canonical policy is the other side of the same acceptance: the same
//! two processes must agree exactly, or no contract lane could pin emitted
//! SQL at all.

use std::process::Command;

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Four invented names in one heading and nothing else to depend on.
const QUERY: &str = "_(v @ 1) |> (v + 1, v + 2, v + 3, v + 4)";

fn sql_from_a_fresh_process(policy: Option<&str>) -> String {
    let mut cmd = Command::new(dql_bin());
    cmd.args(["query", "--to", "sql", QUERY]);
    match policy {
        Some(policy) => cmd.env("DQL_NAME_POLICY", policy),
        None => cmd.env_remove("DQL_NAME_POLICY"),
    };
    let out = cmd.output().expect("spawn dql");
    assert!(
        out.status.success(),
        "dql refused: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8 sql")
}

#[test]
fn two_fresh_processes_draw_different_names() {
    let first = sql_from_a_fresh_process(None);
    let second = sql_from_a_fresh_process(None);
    assert_ne!(
        first, second,
        "the shipped policy must draw invented names fresh; identical SQL from \
         two processes means something is dependable that was ruled not to be"
    );
}

#[test]
fn two_fresh_processes_agree_on_canonical_names() {
    let first = sql_from_a_fresh_process(Some("canonical"));
    let second = sql_from_a_fresh_process(Some("canonical"));
    assert_eq!(
        first, second,
        "canonical SQL is what a contract lane pins; it must not move between runs"
    );
    assert!(
        first.contains("<mint:1>") && first.contains("<mint:4>"),
        "canonical names are numbered per heading: {first}"
    );
}

#[test]
fn an_unknown_policy_refuses_rather_than_falling_back() {
    let mut cmd = Command::new(dql_bin());
    cmd.args(["query", "--to", "sql", QUERY])
        .env("DQL_NAME_POLICY", "canonicalish");
    let out = cmd.output().expect("spawn dql");
    assert!(
        !out.status.success(),
        "a misspelled policy that fell back to the default would report a \
         contract nobody asked for"
    );
}
