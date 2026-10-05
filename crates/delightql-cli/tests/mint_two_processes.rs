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
//! The other side of the same acceptance is the Header: it marks each
//! invented name minted, and a digest that includes column names reads a
//! minted column by position rather than by spelling. So two processes that
//! disagree on every invented name still agree on the digest.

use std::process::Command;

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Four invented names in one heading and nothing else to depend on.
const QUERY: &str = "_(v @ 1) |> (v + 1, v + 2, v + 3, v + 4)";

fn from_a_fresh_process(args: &[&str]) -> String {
    let out = Command::new(dql_bin())
        .args(["query"])
        .args(args)
        .arg(QUERY)
        .output()
        .expect("spawn dql");
    assert!(
        out.status.success(),
        "dql refused: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8 output")
}

#[test]
fn two_fresh_processes_draw_different_names() {
    let first = from_a_fresh_process(&["--to", "sql"]);
    let second = from_a_fresh_process(&["--to", "sql"]);
    assert_ne!(
        first, second,
        "invented names must be drawn fresh; identical SQL from two processes \
         means something is dependable that was ruled not to be"
    );
}

#[test]
fn two_fresh_processes_agree_on_a_name_bearing_digest() {
    for digest in ["totalhash", "fingerprint"] {
        let first = from_a_fresh_process(&["-f", digest]);
        let second = from_a_fresh_process(&["-f", digest]);
        assert_eq!(
            first, second,
            "{digest} reads a minted column by position, so it must not move between runs"
        );
    }
}
