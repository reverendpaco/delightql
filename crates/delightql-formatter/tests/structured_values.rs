// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The formatter carries the empty constructors and scientific notation
//! through unchanged, and a second pass changes nothing.

use delightql_formatter::{format, format_outcome, FormatConfig, FormatOutcome};

fn formatted(source: &str) -> String {
    match format_outcome(source, &FormatConfig::default()).expect("formats") {
        FormatOutcome::Formatted(text) => text,
        FormatOutcome::PassedThrough { .. } => {
            panic!("the formatter passed {source:?} through instead of formatting it")
        }
    }
}

fn round_trips(source: &str) -> String {
    let once = formatted(source);
    let twice = format(&once, &FormatConfig::default()).expect("formats again");
    assert_eq!(once, twice, "second pass moved {source:?}");
    once
}

#[test]
fn empty_constructors_survive_formatting() {
    let out = round_trips("#!dql query-sequence\n_(value @ {})\n");
    assert!(out.contains("{}"), "{out}");
    let out = round_trips("#!dql query-sequence\n_(value @ [])\n");
    assert!(out.contains("[]"), "{out}");
    let out = round_trips("#!dql query-sequence\n_(value @ {\"array\": [], \"object\": {}})\n");
    assert!(out.contains("[]") && out.contains("{}"), "{out}");
}

#[test]
fn empty_collectors_survive_formatting() {
    let out = round_trips("#!dql query-sequence\n_(x @ 1; 2) ~> {} as value\n");
    assert!(out.contains("~> {}"), "{out}");
    let out = round_trips("#!dql query-sequence\n_(x @ 1; 2) ~> [] as value\n");
    assert!(out.contains("~> []"), "{out}");
}

#[test]
fn scientific_notation_is_written_as_authored() {
    let out = round_trips(
        "#!dql query-sequence\n_(label, value @ \"a\", 1.25e-2; \"b\", 1e3; \"c\", -4.5e-2; \"d\", 2E+3)\n",
    );
    for spelling in ["1.25e-2", "1e3", "-4.5e-2", "2E+3"] {
        assert!(out.contains(spelling), "{spelling} missing from {out}");
    }
    let out = round_trips("#!dql query-sequence\n_(j @ {\"n\": 1.25e2}) |> (j:{.n} as n)\n");
    assert!(out.contains("1.25e2"), "{out}");
}
